package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type CompletedExpr struct {
	outputs []field
	sql     string
	args    []any
}

// Complete returns a completed expression ready for execution, using the provided values to
// substitute the input placeholders in the prepared expression. These placeholders use
// the syntax "$Person.fullname", where Person would be a type such as:
//
//	type Person struct {
//	        Name string `db:"fullname"`
//	}
func (pe *PreparedExpr) Complete(args ...any) (ce *CompletedExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("invalid input parameter: %s", err)
		}
	}()

	var typeInQuery = make(map[reflect.Type]bool)
	for _, in := range pe.inputs {
		typeInQuery[in.structType] = true
	}

	var typeValue = make(map[reflect.Type]reflect.Value)
	var typeNames []string
	for _, arg := range args {
		if arg == nil {
			return nil, fmt.Errorf("need struct, got nil")
		}
		v := reflect.ValueOf(arg)
		v = reflect.Indirect(v)
		t := v.Type()

		if t.Kind() != reflect.Struct {
			return nil, fmt.Errorf("need struct, got %s", t.Kind())
		}

		if _, ok := typeValue[t]; ok {
			return nil, fmt.Errorf("more than one instance of type %q. To input different instances of the same struct a type alias must be used", t.Name())
		}
		typeValue[t] = v
		typeNames = append(typeNames, t.Name())

		if !typeInQuery[t] {
			// Check if we have a type with the same name from a different package.
			for _, in := range pe.inputs {
				if t.Name() == in.structType.Name() {
					return nil, fmt.Errorf("type %s not found, have %s", in.structType.String(), t.String())
				}
			}
			return nil, fmt.Errorf("%s not referenced in query", t.Name())
		}
	}

	// Query parameteres.
	qargs := []any{}

	for i, in := range pe.inputs {
		v, ok := typeValue[in.structType]
		if !ok {
			if len(typeNames) == 0 {
				return nil, fmt.Errorf(`type %q not found, no input structs were found`, in.structType.Name())
			} else {
				return nil, fmt.Errorf(`type %q not found, have: %s`, in.structType.Name(), strings.Join(typeNames, ", "))
			}
		}
		named := sql.Named("sqlair_"+strconv.Itoa(i), v.FieldByIndex(in.index).Interface())
		qargs = append(qargs, named)
	}

	return &CompletedExpr{outputs: pe.outputs, sql: pe.sql, args: qargs}, nil
}
