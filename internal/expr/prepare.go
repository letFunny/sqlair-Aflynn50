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
	inputs  []inputLocation
	SQL     string
}

type outputDest struct {
	structType reflect.Type
	field      field
}

// We get the position in the query from its position in inputLocation
type inputLocation struct {
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

func nParams(start int, num int) string {
	var s bytes.Buffer
	s.WriteString("(")
	for i := start; i < start+num; i++ {
		s.WriteString("@_sqlair_")
		s.WriteString(strconv.Itoa(i))
		if i < start+num-1 {
			s.WriteString(", ")
		}
	}
	s.WriteString(")")
	return s.String()
}

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
		return fmt.Errorf("invalid asterisk in input expression: %s", p)
	}
	if !starSource && (numColumns > 0 && (numColumns != numSources)) {
		return fmt.Errorf("mismatched number of inputs and cols in input expression: %s", p)
	}
	return nil
}

// prepareInput first checks the types mentioned in the expression are known, it
// then checks the expression is valid and generates the SQL to print in place
// of it.
// As well as the SQL string it also returns a list of fullNames containing the
// type and tag of each input parameter. These are used in the complete stage to
// extract the arguments from the relevent structs.
func prepareInput(ti typeNameToInfo, p *inputPart, n int) (string, []inputLocation, error) {

	var inLocs = make([]inputLocation, 0)

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
			inLocs = append(inLocs, inputLocation{info.structType, f})
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
		return "@_sqlair_" + strconv.Itoa(n), inLocs, nil
	}

	// Case 2: A VALUES expression (probably inside an INSERT)
	cols := []string{}
	// Case 2.1: An Asterisk VALUES expression e.g. "... VALUES $P.*".
	if p.source[0].name == "*" {
		info, _ := ti[p.source[0].prefix]
		// Case 2.1.1 e.g. "(*) VALUES ($P.*)"
		if p.cols[0] == "*" {
			for _, tag := range getKeys(info.tagToField) {
				cols = append(cols, tag)
				inLocs = append(inLocs, inputLocation{info.structType, info.tagToField[tag]})
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
			inLocs = append(inLocs, inputLocation{info.structType, f})
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
	numSources := len(p.source)
	numTargets := len(p.target)

	targetStars := starCount(p.target)
	sourceStars := starCount(p.source)
	starTarget := targetStars == 1
	starSource := sourceStars == 1

	if targetStars > 1 || sourceStars > 1 || (sourceStars == 1 && targetStars == 0) ||
		(starTarget && numTargets > 1) || (starSource && numSources > 1) {
		return fmt.Errorf("invalid asterisk in output expression: %s", p)
	}
	if !starTarget && (numSources > 0 && (numTargets != numSources)) {
		return fmt.Errorf("mismatched number of cols and targets in output expression: %s", p)
	}
	return nil
}

// prepareOutput checks that the output expressions are correspond to a known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) ([]fullName, []outputDest, error) {

	var outCols = make([]fullName, 0)
	var outDests = make([]outputDest, 0)

	// Check the asterisk are well formed (if present).
	if err := starCheckOutput(p); err != nil {
		return nil, nil, err
	}

	// Check target struct type and its tags are valid.
	var info *info
	var ok bool

	for _, t := range p.target {
		info, ok = ti[t.prefix]
		if !ok {
			return nil, nil, fmt.Errorf(`type %s unknown, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
		}

		if t.name != "*" {
			f, ok := info.tagToField[t.name]
			if !ok {
				return nil, nil, fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), t.name)
			}
			// For a none star expression we record output destinations here.
			// For a star expression we fill out the destinations as we generate the columns.
			outDests = append(outDests, outputDest{info.structType, f})

		}
	}

	// Generate columns to inject into SQL query.

	// Case 1: Star target cases e.g. "...&P.*".
	if p.target[0].name == "*" {
		info, _ := ti[p.target[0].prefix]

		// Case 1.1: Single star i.e. "t.* AS &P.*" or "&P.*"
		if len(p.source) == 0 || p.source[0].name == "*" {
			pref := ""

			// Prepend table name. E.g. "t" in "t.* AS &P.*".
			if len(p.source) > 0 {
				pref = p.source[0].prefix
			}

			// getKeys also sorts the keys.
			tags := getKeys(info.tagToField)
			for _, tag := range tags {
				outCols = append(outCols, fullName{pref, tag})
				outDests = append(outDests, outputDest{info.structType, info.tagToField[tag]})
			}
			return outCols, outDests, nil
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if len(p.source) > 0 {
			for _, c := range p.source {
				f, ok := info.tagToField[c.name]
				if !ok {
					return nil, nil, fmt.Errorf(`type %s has no %q db tag`, info.structType.Name(), c.name)
				}
				outCols = append(outCols, c)
				outDests = append(outDests, outputDest{info.structType, f})
			}
			return outCols, outDests, nil
		}
	}

	// Case 2: None star target cases e.g. "...(&P.name, &P.id)".

	// Case 2.1: Explicit columns e.g. "name_1 AS P.name".
	if len(p.source) > 0 {
		for _, c := range p.source {
			outCols = append(outCols, c)
		}
		return outCols, outDests, nil
	}

	// Case 2.2: No columns e.g. "(&P.name, &P.id)".
	for _, t := range p.target {
		outCols = append(outCols, fullName{name: t.name})
	}
	return outCols, outDests, nil
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
	var inputs = make([]inputLocation, 0)

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
