package expr

import (
	"fmt"
	"reflect"
)

type CompletedExpr struct {
	ParsedExpr  *ParsedExpr
	InputValues []any
}

type typeNameToValue = map[string]any

// Complete extracts query arguments specified in inputParts from structs.
func (pe *PreparedExpr) Complete(args ...any) (ce *CompletedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot complete expression: %s", err)
		}
	}()

	var tv = make(typeNameToValue)
	for _, arg := range args {
		if arg == (any)(nil) {
			return nil, fmt.Errorf("cannot reflect nil value")
		}
		tv[reflect.TypeOf(arg).Name()] = arg
	}

	// Query parameteres.
	qargs := []any{}

	for _, p := range pe.ParsedExpr.queryParts {
		if p, ok := p.(*inputPart); ok {
			v, ok := tv[p.source.prefix]
			if !ok {
				return nil, fmt.Errorf(`type %#v not passed as a parameter`, p.source.prefix)
			}
			qp, err := fieldValue(v, p.source)
			if err != nil {
				return nil, err
			}
			qargs = append(qargs, qp)
		}
	}

	return &CompletedExpr{pe.ParsedExpr, qargs}, nil
}
