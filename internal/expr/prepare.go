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
	outputs []field
	inputs  []field
	sql     string
}

const markerPrefix = "_sqlair_"

func markerName(n int) string {
	return markerPrefix + strconv.Itoa(n)
}

// markerIndex returns the int X from the string "_sqlair_X".
func markerIndex(s string) (int, bool) {
	if strings.HasPrefix(s, markerPrefix) {
		n, err := strconv.Atoi(s[len(markerPrefix):])
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

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

// starCheckOutput checks that the output expression is well formed.
func starCheckOutput(p *outputPart) error {
	numColumns := len(p.columns)
	numTypes := len(p.types)

	typeStars := starCount(p.types)
	columnsStars := starCount(p.columns)
	starType := typeStars == 1
	starColumn := columnsStars == 1

	if typeStars > 1 || columnsStars > 1 || (columnsStars == 1 && typeStars == 0) ||
		(starType && numTypes > 1) || (starColumn && numColumns > 1) {
		return fmt.Errorf("invalid asterisk in output expression: %s", p.raw)
	}
	if !starType && (numColumns > 0 && (numTypes != numColumns)) {
		return fmt.Errorf("cannot match columns to types in output expression: %s", p.raw)
	}
	return nil
}

// starCheckInput checks that the input expression is well formed.
func starCheckInput(p *inputPart) error {
	numTypes := len(p.types)
	numCols := len(p.columns)
	starTypes := starCount(p.types)
	starCols := starCount(p.columns)

	if numCols == 1 && starCols == 1 {
		return nil
	}

	// Input types grouped togther not in VALUES expression
	if numCols == 0 && numTypes > 1 {
		return fmt.Errorf("internal error: cannot group standalone input expressions")
	}

	// Cannot have multiple star columns or multiple star types
	if (numCols > 1 && starCols > 0) || numCols == 0 && starTypes > 0 {
		return fmt.Errorf("invalid asterisk in input expression: %s", p.raw)
	}

	// Explicit columns and not star type and the number of columns does not equal number of fields specified.
	if numCols > 0 && starCols == 0 && !((numTypes == 1 && starTypes == 1) || (starTypes == 0 && numTypes == numCols)) {
		return fmt.Errorf("cannot match columns to types in input expression: %s", p.raw)
	}
	return nil
}

// prepareInput checks that the input expression corresponds to a known type.
func prepareInput(ti typeNameToInfo, p *inputPart) ([]fullName, []field, error) {
	var inCols = make([]fullName, 0)
	var fields = make([]field, 0)

	// Check the asterisks are well formed (if present).
	if err := starCheckInput(p); err != nil {
		return nil, nil, err
	}

	// Check target struct type and its tags are valid.
	var info *info
	var ok bool

	// Check the input structs and their tags are valid.
	for _, t := range p.types {
		info, ok = ti[t.prefix]
		if !ok {
			return nil, nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
		}

		if t.name != "*" {
			f, ok := info.tagToField[t.name]
			if !ok {
				return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ.Name(), t.name)
			}
			// For a none star expression we record output destinations here.
			// For a star expression we fill out the destinations as we generate the columns.
			fields = append(fields, f)
		}
	}

	// Generate columns to inject into SQL query.

	// Case 0: A simple standalone input expression e.g. "$P.name".
	if len(p.columns) == 0 {
		return []fullName{}, fields, nil
	}

	// Case 1: Star type cases e.g. "... VALUES $P.*".
	if p.types[0].name == "*" {
		info, _ := ti[p.types[0].prefix]

		// Case 1.1: Single star i.e. "* VALUES $P.*"
		if p.columns[0].name == "*" {
			for _, tag := range info.tags {
				inCols = append(inCols, fullName{name: tag})
				fields = append(fields, info.tagToField[tag])
			}
			return inCols, fields, nil
		}

		// Case 1.2: Explicit columns e.g. "(col1, col2) VALUES $P.*".
		for _, c := range p.columns {
			f, ok := info.tagToField[c.name]
			if !ok {
				return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ.Name(), c.name)
			}
			inCols = append(inCols, c)
			fields = append(fields, f)
		}
		return inCols, fields, nil
	}

	// Case 2: None star type cases e.g. "... VALUES ($P.name, $P.id)".

	// Case 2.1: Star column e.g. "* VALUES ($P.name, $P.id)".
	if p.columns[0].name == "*" {
		for _, t := range p.types {
			inCols = append(inCols, fullName{name: t.name})
		}
		return inCols, fields, nil
	}

	// Case 2.2: Renamed explicit columns e.g. "(name_1) VALUES $P.name".
	for _, c := range p.columns {
		inCols = append(inCols, c)
	}
	return inCols, fields, nil
}

// prepareOutput checks that the output expressions correspond to known types.
// It then checks they are formatted correctly and finally generates the columns for the query.
func prepareOutput(ti typeNameToInfo, p *outputPart) ([]fullName, []field, error) {

	var outCols = make([]fullName, 0)
	var fields = make([]field, 0)

	// Check the asterisks are well formed (if present).
	if err := starCheckOutput(p); err != nil {
		return nil, nil, err
	}

	// Check target struct type and its tags are valid.
	var info *info
	var ok bool

	for _, t := range p.types {
		info, ok = ti[t.prefix]
		if !ok {
			return nil, nil, fmt.Errorf(`type %q not passed as a parameter, have: %s`, t.prefix, strings.Join(getKeys(ti), ", "))
		}

		if t.name != "*" {
			f, ok := info.tagToField[t.name]
			if !ok {
				return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ.Name(), t.name)
			}
			// For a none star expression we record output destinations here.
			// For a star expression we fill out the destinations as we generate the columns.
			fields = append(fields, f)
		}
	}

	// Generate columns to inject into SQL query.

	// Case 1: Star types cases e.g. "...&P.*".
	if p.types[0].name == "*" {
		info, _ := ti[p.types[0].prefix]

		// Case 1.1: Single star i.e. "t.* AS &P.*" or "&P.*"
		if len(p.columns) == 0 || p.columns[0].name == "*" {
			pref := ""

			// Prepend table name. E.g. "t" in "t.* AS &P.*".
			if len(p.columns) > 0 {
				pref = p.columns[0].prefix
			}

			for _, tag := range info.tags {
				outCols = append(outCols, fullName{pref, tag})
				fields = append(fields, info.tagToField[tag])
			}
			return outCols, fields, nil
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if len(p.columns) > 0 {
			for _, c := range p.columns {
				f, ok := info.tagToField[c.name]
				if !ok {
					return nil, nil, fmt.Errorf(`type %q has no %q db tag`, info.typ.Name(), c.name)
				}
				outCols = append(outCols, c)
				fields = append(fields, f)
			}
			return outCols, fields, nil
		}
	}

	// Case 2: None star types cases e.g. "...(&P.name, &P.id)".

	// Case 2.1: Explicit columns e.g. "name_1 AS P.name".
	if len(p.columns) > 0 {
		for _, c := range p.columns {
			outCols = append(outCols, c)
		}
		return outCols, fields, nil
	}

	// Case 2.2: No columns e.g. "(&P.name, &P.id)".
	for _, t := range p.types {
		outCols = append(outCols, fullName{name: t.name})
	}
	return outCols, fields, nil
}

type typeNameToInfo map[string]*info

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
		if arg == nil {
			return nil, fmt.Errorf("need struct, got nil")
		}
		t := reflect.TypeOf(arg)
		if t.Kind() != reflect.Struct {
			if t.Kind() == reflect.Pointer {
				return nil, fmt.Errorf("need struct, got pointer to %s", t.Elem().Kind())
			}
			return nil, fmt.Errorf("need struct, got %s", t.Kind())
		}
		if t.Name() == "" {
			return nil, fmt.Errorf("cannot use anonymous %s", t.Kind())
		}
		info, err := typeInfo(arg)
		if err != nil {
			return nil, err
		}
		ti[info.typ.Name()] = info
	}

	var sql bytes.Buffer

	var inCount int
	var outCount int

	var outputs = make([]field, 0)
	var inputs = make([]field, 0)

	// Check and expand each query part.
	for _, part := range pe.queryParts {
		switch p := part.(type) {
		case *inputPart:
			inCols, fields, err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			if len(p.columns) == 0 {
				sql.WriteString("@sqlair_" + strconv.Itoa(inCount))
				inCount += 1
			} else {
				sql.WriteString(printCols(inCols))
				sql.WriteString(" VALUES ")
				sql.WriteString(namedParams(inCount, len(inCols)))
				inCount += len(inCols)
			}
			inputs = append(inputs, fields...)
		case *outputPart:
			outCols, fields, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}
			for i, c := range outCols {
				sql.WriteString(c.String())
				sql.WriteString(" AS ")
				sql.WriteString(markerName(outCount))
				if i != len(outCols)-1 {
					sql.WriteString(", ")
				}
				outCount++
			}
			outputs = append(outputs, fields...)

		case *bypassPart:
			sql.WriteString(p.chunk)
		default:
			return nil, fmt.Errorf("internal error: unknown query part type %T", part)
		}
	}

	return &PreparedExpr{inputs: inputs, outputs: outputs, sql: sql.String()}, nil
}

// printCols prints a bracketed, comma seperated list of fullNames.
func printCols(cs []fullName) string {
	var s bytes.Buffer
	s.WriteString("(")
	for i, c := range cs {
		s.WriteString(c.String())
		if i < len(cs)-1 {
			s.WriteString(", ")
		}
	}
	s.WriteString(")")
	return s.String()
}

// namedParams returns n incrementing parameters with the first index being start.
func namedParams(start int, n int) string {
	var s bytes.Buffer
	s.WriteString("(")
	for i := start; i < start+n; i++ {
		s.WriteString("@sqlair_")
		s.WriteString(strconv.Itoa(i))
		if i < start+n-1 {
			s.WriteString(", ")
		}
	}
	s.WriteString(")")
	return s.String()
}
