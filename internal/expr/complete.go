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

	var typeNames []string

	for _, arg := range args {
		if arg == nil {
			return nil, fmt.Errorf("nil parameter")
		}
		v := reflect.ValueOf(arg)
		t := reflect.TypeOf(arg)

		if t.Kind() != reflect.Struct && t.Kind() != reflect.Map {
			return nil, fmt.Errorf("unsupported type: need a struct or map")
		}

		if _, ok := tv[t]; ok {
			return nil, fmt.Errorf("multiple type %#v passed in as parameter", t)
		}

		if t.Kind() == reflect.Map {
			if err = CheckValidMap(t); err != nil {
				return nil, err
			}
		}

		tv[t] = v

		typeNames = append(typeNames, t.String())
	}

	// Query parameteres.
	qargs := []any{}

	for i, in := range pe.inputs {
		v, ok := tv[in.typ]
		if !ok {
			return nil, fmt.Errorf(`type %s not found, have: %s`, in.typ.String(), strings.Join(typeNames, ", "))
		}

		switch in.typ.Kind() {
		case reflect.Struct:
			f := in.field.(field)
			named := sql.Named("sqlair_"+strconv.Itoa(i), v.FieldByIndex(f.index).Interface())
			qargs = append(qargs, named)
		case reflect.Map:
			k := in.field.(mapKey)

			var foundKey bool
			for _, key := range v.MapKeys() {
				if strings.Compare(key.String(), k.name) == 0 {
					foundKey = true
				}
			}
			if !foundKey {
				return nil, fmt.Errorf(`key %q not found in map`, k.name)
			}

			named := sql.Named("sqlair_"+strconv.Itoa(i), v.MapIndex(reflect.ValueOf(k.name)).Interface())
			qargs = append(qargs, named)
		default:
			return nil, fmt.Errorf("internal error: field type %s not supported", in.typ.Name())
		}
	}

	return &CompletedExpr{outputs: pe.outputs, SQL: pe.SQL, Args: qargs}, nil
}
