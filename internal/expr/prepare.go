package expr

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// PreparedExpr contains an SQL expression that is ready for execution.
type PreparedExpr struct {
	outputs []outputDest
	inputs  []inputLoc
	SQL     string
}

// TODO Once these are in a final form work out a single "Location/Loc" struct
// can be used.

// outputDest records the go object and location within it to store an output.
type outputDest struct {
	structType reflect.Type
	field      field
}

// inputLoc records a go object and the location within it where an input can be
// found.
type inputLoc struct {
	inputType reflect.Type
	field     field
}

type typeNameToInfo map[string]*info

// getKeys returns the keys of a string map in a deterministic order.
func getKeys[T any](m map[string]T) []string {
	i := 0
	keys := make([]string, len(m))
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

func starCount(fns []fullName) int {
	s := 0
	for _, fn := range fns {
		if fn.name == "*" {
			s++
		}
	}
	return s
}

func printList(xs []string) string {
	var s bytes.Buffer
	s.WriteString("(")
	s.WriteString(strings.Join(xs, ", "))
	s.WriteString(")")
	return s.String()
}

// nParams returns "num" incrementing parameters with the first index being
// "start".
func nParams(start int, num int) string {
	var s bytes.Buffer
	s.WriteString("(")
	for i := start; i < start+num; i++ {
		s.WriteString("@sqlair_")
		s.WriteString(strconv.Itoa(i))
		if i < start+num-1 {
			s.WriteString(", ")
		}
	}
	s.WriteString(")")
	return s.String()
}

// TODO see how easily this can be combined with starCheckOutput.

// starCheckInput checks that the input is well formed with regard to
// asterisks and the number of sources and targets.
func starCheckInput(p *inputPart) error {
	numColumns := len(p.cols)
	numSources := len(p.source)

	sourceStars := starCount(p.source)
	columnStars := 0
	for _, col := range p.cols {
		if col == "*" {
			columnStars++
		}
	}
	starSource := sourceStars == 1
	starColumn := columnStars == 1

	if sourceStars > 1 || columnStars > 1 || (columnStars == 1 && sourceStars == 0) ||
		(starColumn && numColumns > 1) || (starSource && numSources > 1) || (starSource && numColumns == 0) {
		return fmt.Errorf("invalid asterisk in: %s", p.raw())
	}
	if !starSource && (numColumns > 0 && (numColumns != numSources)) {
		return fmt.Errorf("cannot match columns to targets in: %s", p.raw())
	}
	return nil
}

// prepareInput generates an SQL snippit and associated input locations for the given input part.
func prepareInput(ti typeNameToInfo, p *inputPart, n int) (string, []inputLoc, error) {

	var inLocs = make([]inputLoc, 0)

	// Check the input structs and their tags are valid.
	for _, s := range p.source {
		info, ok := ti[s.prefix]
		if !ok {
			return "", nil, fmt.Errorf(`type %s unknown, have: %s`, s.prefix, strings.Join(getKeys(ti), ", "))
		}
		if s.name != "*" {
			f, ok := info.tagToField[s.name]
			if !ok {
				return "", nil, fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), s.name)
			}
			// For a none star expression we record output destinations here.
			// For a star expression we fill out the destinations as we generate the columns.
			inLocs = append(inLocs, inputLoc{info.structType, f})
		}
	}

	if err := starCheckInput(p); err != nil {
		return "", nil, err
	}

	// Case 1: A simple standalone input expression e.g. "$P.name".
	if len(p.cols) == 0 {
		if len(p.source) != 1 {
			return "", nil, fmt.Errorf("internal error: cannot group standalone input expressions")
		}
		return "@sqlair_" + strconv.Itoa(n), inLocs, nil
	}

	// Case 2: A VALUES expression (probably inside an INSERT)
	cols := []string{}
	// Case 2.1: An Asterisk VALUES expression e.g. "... VALUES $P.*".
	if p.source[0].name == "*" {
		info, _ := ti[p.source[0].prefix]
		// Case 2.1.1 e.g. "(*) VALUES ($P.*)"
		if p.cols[0] == "*" {
			for _, tag := range info.tags {
				cols = append(cols, tag)
				inLocs = append(inLocs, inputLoc{info.structType, info.tagToField[tag]})
			}
			return printList(cols) + " VALUES " + nParams(n, len(cols)), inLocs, nil
		}
		// Case 2.1.2 e.g. "(col1, col2, col3) VALUES ($P.*)"
		for _, col := range p.cols {
			f, ok := info.tagToField[col]
			if !ok {
				return "", nil, fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), col)
			}
			cols = append(cols, col)
			inLocs = append(inLocs, inputLoc{info.structType, f})
		}
		return printList(cols) + " VALUES " + nParams(n, len(cols)), inLocs, nil
	}
	// Case 2.2: explicit for both e.g. (mycol1, mycol2) VALUES ($Person.col1, $Address.col1)
	cols = p.cols
	return printList(p.cols) + " VALUES " + nParams(n, len(p.cols)), inLocs, nil
}

// starCheckOutput checks that the statement is well formed with regard to
// asterisks and the number of sources and targets.
func starCheckOutput(p *outputPart) error {
	numTargets := len(p.target)
	numSources := len(p.source)
	if numSources == 0 || (numSources == 1 && p.source[0].name == "*") {
		return nil
	}
	if numSources > 1 && starCount(p.source) > 0 {
		return fmt.Errorf("invalid asterisk in: %s", p.raw())
	}
	if numSources > 0 && !((numTargets == 1 && p.target[0].name == "*") || (starCount(p.target) == 0 && numTargets == numSources)) {
		return fmt.Errorf("cannot match columns to targets in: %s", p.raw())
	}
	return nil
}

type outputBuilder struct {
	outCols  []fullName
	outDests []outputDest
}

// prepareOutput checks that the output expressions are correspond to a known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) ([]fullName, []outputDest, error) {

	var info *info
	var ok bool

	ob := outputBuilder{}

	add := func(typeName string, tag string, col fullName) error {
		info, ok = ti[typeName]
		if !ok {
			return fmt.Errorf(`type %s unknown, have: %s`, typeName, strings.Join(getKeys(ti), ", "))
		}

		f, ok := info.tagToField[tag]
		if !ok {
			return fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), tag)
		}
		ob.outCols = append(ob.outCols, col)
		ob.outDests = append(ob.outDests, outputDest{info.structType, f})
		return nil
	}

	// Check the asterisk are well formed (if present).
	if err := starCheckOutput(p); err != nil {
		return nil, nil, err
	}

	// Generate columns to inject into SQL query.

	// Case 1: sqlair generates columns e.g. "* AS (&P.*, &A.id)" or "&P.*".
	if len(p.source) == 0 || p.source[0].name == "*" {
		pref := ""
		// Prepend table name. E.g. the "t" in "t.* AS &P.*".
		if len(p.source) > 0 {
			pref = p.source[0].prefix
		}
		for _, t := range p.target {
			if t.name == "*" {
				// Generate columns for Star target.
				info, ok = ti[t.prefix]
				if !ok {
					return nil, nil, fmt.Errorf(`type %s unknown, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
				}
				for _, tag := range info.tags {
					if err := add(t.prefix, tag, fullName{pref, tag}); err != nil {
						return nil, nil, err
					}
				}
			} else {
				// Generate Columns for none star target.
				if err := add(t.prefix, t.name, fullName{pref, t.name}); err != nil {
					return nil, nil, err
				}
			}
		}
		return ob.outCols, ob.outDests, nil
	}
	// Case 2: Explicit columns with star e.g. "(name, id) AS (&P.*)".
	// There must only be a single target in this case.
	if p.target[0].name == "*" {
		for _, c := range p.source {
			if err := add(p.target[0].prefix, c.name, c); err != nil {
				return nil, nil, err
			}
		}
		return ob.outCols, ob.outDests, nil
	}

	// Case 3: Explicit columns and targets e.g. "(col1, col2) AS (&P.name, &P.id)".
	// The number of each must be equal here.
	for i, c := range p.source {
		if err := add(p.target[i].prefix, p.target[i].name, c); err != nil {
			return nil, nil, err
		}
	}
	return ob.outCols, ob.outDests, nil
}

// Prepare takes a parsed expression and struct instantiations of all the types
// mentioned in it.
// The IO parts of the statement are checked for validity against the types
// and expanded if necessary.
func (pe *ParsedExpr) Prepare(args ...any) (expr *PreparedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot prepare expression: %s", err)
		}
	}()

	var ti = make(typeNameToInfo)

	// Generate and save reflection info.
	for _, arg := range args {
		info, err := typeInfo(arg)
		if err != nil {
			return nil, err
		}
		ti[info.structType.Name()] = info
	}

	var sql bytes.Buffer
	// n counts the inputs.
	var n int
	// m counts the outputs.
	var m int

	var outputs = make([]outputDest, 0)
	var inputs = make([]inputLoc, 0)

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			s, ins, err := prepareInput(ti, p, n)
			n += len(ins)
			if err != nil {
				return nil, err
			}
			sql.WriteString(s)
			inputs = append(inputs, ins...)
		case *outputPart:
			outCols, outDests, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}
			for i, c := range outCols {
				sql.WriteString(c.String())
				sql.WriteString(" AS _sqlair_")
				sql.WriteString(strconv.Itoa(m))
				if i != len(outCols)-1 {
					sql.WriteString(", ")
				}
				m++
			}
			outputs = append(outputs, outDests...)

		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: inputs, outputs: outputs, SQL: sql.String()}, nil
}
