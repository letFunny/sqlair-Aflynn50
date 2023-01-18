package expr

import (
	"bytes"
	"fmt"
	"sort"
)

// PreparedExpr represents an SQL expression after the input and output parts
// have been replaced by their corresponding expansions.
type PreparedExpr struct {
	ParsedExpr *ParsedExpr
	SQL        string
}

type typeNameToInfo map[string]*info

// prepareInput checks that the input expression corresponds to a Go struct
// passed to Prepare.
func prepareInput(ti typeNameToInfo, p *inputPart) error {
	if inf, ok := ti[p.source.prefix]; ok {
		if _, ok := inf.tagToField[p.source.name]; ok {
			return nil
		}
		return fmt.Errorf(`there is no tag with name "%s" in "%s"`,
			p.source.name, inf.structType.Name())
	}
	return fmt.Errorf(`unknown type: "%s"`, p.source.prefix)
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

func prepareOutput(ti typeNameToInfo, p *outputPart) ([]string, error) {

	var outCols []string = make([]string, 0)

	// Check target struct type and its tags are valid.
	for _, t := range p.target {
		inf, ok := ti[t.prefix]
		if !ok {
			return nil, fmt.Errorf("unknown type: %s", t.prefix)
		}

		_, ok = inf.tagToField[t.name]
		if !ok && t.name != "*" {
			return nil, fmt.Errorf(`there is no tag with name "%s" in "%s"`, t.name, inf.structType.Name())
		}
	}

	// Check asterisk are in correct places.

	sct := starCount(p.target)
	scc := starCount(p.source)

	if sct > 1 || scc > 1 || (scc == 1 && sct == 0) {
		return nil, fmt.Errorf("invalid asterisk in output expression")
	}

	starTarget := sct == 1
	starColumn := scc == 1

	lenS := len(p.source)
	lenT := len(p.target)

	if (starTarget && lenT > 1) || (starColumn && lenS > 1) {
		return nil, fmt.Errorf("invalid mix of asterisk and none asterisk columns in output expression")
	}

	if !starTarget && (lenS > 0 && (lenT != lenS)) {
		return nil, fmt.Errorf("mismatched number of cols and targets in output expression")
	}

	// Case 1: Star target cases e.g. "...&P.*".
	// In parse we ensure that if p.target[0] is a * then len(p.target) == 1
	if starTarget {

		inf, _ := ti[p.target[0].prefix]

		// Case 1.1: Single star e.g. "t.* AS &P.*" or "&P.*"
		if starColumn || lenS == 0 {
			pref := ""

			// Prepend table name. E.g. "t" in "t.* AS &P.*".
			if lenS > 0 && p.source[0].prefix != "" {
				pref = p.source[0].prefix + "."
			}

			for tag := range inf.tagToField {
				outCols = append(outCols, pref+tag)
			}

			// The strings are sorted to give a deterministic order for
			// testing.
			sort.Strings(outCols)
			return outCols, nil
		}

		// Case 1.2: Explicit columns e.g. "(col1, t.col2) AS &P.*".
		if lenS > 0 {
			for _, c := range p.source {
				if _, ok := inf.tagToField[c.name]; !ok {
					return nil, fmt.Errorf(`there is no tag with name "%s" in "%s"`,
						c.name, inf.structType.Name())
				}
				outCols = append(outCols, c.String())
			}
			return outCols, nil
		}
	}

	// Case 2: None star target cases e.g. "...&(P.name, P.id)".

	// Case 2.1: Explicit columns e.g. "name_1 AS P.name".
	if lenS > 0 {
		for _, c := range p.source {
			outCols = append(outCols, c.String())
		}
		return outCols, nil
	}

	// Case 2.2: No columns e.g. "&(P.name, P.id)".
	for _, t := range p.target {
		outCols = append(outCols, t.name)
	}
	return outCols, nil
}

// Prepare takes a parsed expression and all the Go objects mentioned in it.
// The IO parts of the statement are checked for validity against the Go objects
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
		inf, err := typeInfo(arg)
		if err != nil {
			return nil, err
		}
		ti[inf.structType.Name()] = inf
	}

	var sql bytes.Buffer
	// Check and expand each query part.
	for _, part := range pe.queryParts {
		if p, ok := part.(*inputPart); ok {
			err := prepareInput(ti, p)
			if err != nil {
				return nil, err
			}
			sql.WriteString(p.toSQL([]string{}))
			continue
		}

		if p, ok := part.(*outputPart); ok {
			outCols, err := prepareOutput(ti, p)
			if err != nil {
				return nil, err
			}
			sql.WriteString(p.toSQL(outCols))
			continue
		}

		p := part.(*bypassPart)
		sql.WriteString(p.toSQL([]string{}))
	}

	return &PreparedExpr{ParsedExpr: pe, SQL: sql.String()}, nil
}
