package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
)

type CompletedExpr struct {
	outputs []loc
	SQL     string
	Args    []any
}

// Complete gathers the query arguments that are specified in inputParts from
// structs passed as parameters.
func (pe *PreparedExpr) Complete(args ...any) (*CompletedExpr, error) {
	var tv = make(map[reflect.Type]reflect.Value)
	for _, arg := range args {
		if arg == nil {
			return nil, fmt.Errorf("nil parameter")
		}
		v := reflect.ValueOf(arg)
		tv[v.Type()] = v
	}

	// Query parameteres.
	qargs := []any{}

	for i, in := range pe.inputs {
		v, ok := tv[in.typ]
		if !ok {
			return nil, fmt.Errorf(`type %s not passed as a parameter`, in.typ.Name())
		}

		if in.typ.Kind() == reflect.Struct {
			switch f := in.field.(type) {
			case field:
				named := sql.Named("sqlair_"+strconv.Itoa(i), v.FieldByIndex(f.index).Interface())
				qargs = append(qargs, named)
			case mapKey:
				return nil, fmt.Errorf("internal error, field not found for struct type %s", in.typ.Name())
			}
		} else if in.typ.Kind() == reflect.Map {
			// todo: add check for type name to be M
			switch f := in.field.(type) {
			case field:
				return nil, fmt.Errorf("internal error, key not found for map type %s", in.typ.Name())
			case mapKey:
				named := sql.Named("sqlair_"+strconv.Itoa(i), v.MapIndex(reflect.ValueOf(f.name)).Interface())
				qargs = append(qargs, named)
			}
		}
	}

	return &CompletedExpr{outputs: pe.outputs, SQL: pe.SQL, Args: qargs}, nil
}
