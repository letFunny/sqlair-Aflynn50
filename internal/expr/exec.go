package expr

import (
	"database/sql"
	"fmt"
	"reflect"
)

type ResultExpr struct {
	outputs typeToCols
	rows    *sql.Rows
	rs      []res
}

// How does Joe want this to look?
func (pe *PreparedExpr) Query(db *sql.DB, args ...any) (*ResultExpr, error) {
	inputArgs, err := pe.Complete(args...)
	if err != nil {
		return nil, fmt.Errorf("argument error: %s", err)
	}
	rows, err := db.Query(pe.SQL, inputArgs...)
	if err != nil {
		return nil, fmt.Errorf("database error: %s", err)
	}

	// We fill in colInfo on the first call to Next to avoid its generation in
	// cases where the user disregards the results
	return &ResultExpr{pe.outputs, rows, nil}, nil
}

type typeNameToValue = map[string]any

// Complete extracts query arguments specified in inputParts from structs.
func (pe *PreparedExpr) Complete(args ...any) ([]any, error) {
	var tv = make(typeNameToValue)
	for _, arg := range args {
		if arg == (any)(nil) {
			return nil, fmt.Errorf("cannot reflect nil value")
		}
		tv[reflect.TypeOf(arg).Name()] = arg
	}

	// Query parameteres.
	qargs := []any{}

	for _, p := range pe.inputs {
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

	return qargs, nil
}
