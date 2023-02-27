package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type CompletedExpr struct {
	outputs []loc
	SQL     string
	Args    []any
}

// Complete gathers the query arguments that are specified in inputParts from
// structs passed as parameters.
func (pe *PreparedExpr) Complete(args ...any) (ce *CompletedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("parameter issue: %s", err)
		}
	}()

	var tv = make(map[reflect.Type]reflect.Value)

	// var m map[string]any
	// var foundMap bool

	var typeNames []string
	for _, arg := range args {
		if arg == nil {
			return nil, fmt.Errorf("nil parameter")
		}
		v := reflect.ValueOf(arg)
		t := reflect.TypeOf(arg)

		switch t.Kind() {
		case reflect.Struct:
			tv[t] = v
		case reflect.Map:
			if err := CheckValidMapType(t); err != nil {
				return nil, err
			}

			tv[reflect.TypeOf(M{})] = v
			// m = arg.(map[string]any)
			// foundMap = true

			// tv[reflect.TypeOf(M{})] = reflect.ValueOf(m)

		default:
			return nil, fmt.Errorf("unsupported type: need a struct or map M")
		}

		typeNames = append(typeNames, t.String())
	}

	// Query parameteres.
	qargs := []any{}

	// var foundMapKey bool

	for i, in := range pe.inputs {
		v, ok := tv[in.typ]
		if !ok {
			return nil, fmt.Errorf(`type %s not found, have: %s`, in.typ.String(), strings.Join(typeNames, ", "))
		}

		switch in.field.(type) {
		case field:
			f := in.field.(field)
			named := sql.Named("sqlair_"+strconv.Itoa(i), v.FieldByIndex(f.index).Interface())
			qargs = append(qargs, named)
		case mapKey:
			m := in.field.(mapKey)
			// foundMapKey = true
			// if foundMap {
			// 	v, ok := m[f.name]
			// 	if !ok {
			// 		return nil, fmt.Errorf(`key %q not found in map`, f.name)
			// 	}
			// 	named := sql.Named("sqlair_"+strconv.Itoa(i), v)
			// 	qargs = append(qargs, named)
			// }

			if v.MapIndex(reflect.ValueOf(m.name)) == reflect.Value(reflect.ValueOf(nil)) {
				return nil, fmt.Errorf(`key %q not found in map`, m.name)
			}
			named := sql.Named("sqlair_"+strconv.Itoa(i), v.MapIndex(reflect.ValueOf(m.name)).Interface())
			qargs = append(qargs, named)
		default:
			return nil, fmt.Errorf("internal error: field type %s not supported", in.typ.Name())
		}
	}

	// if !foundMap && foundMapKey {
	// 	return nil, fmt.Errorf(`map key found, but there is no map input`)
	// }
	// if foundMap && !foundMapKey {
	// 	return nil, fmt.Errorf(`map input received but there is no matching map key`)
	// }

	return &CompletedExpr{outputs: pe.outputs, SQL: pe.SQL, Args: qargs}, nil
}
