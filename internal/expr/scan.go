package expr

import (
	"fmt"
	"reflect"
	"strconv"
)

func (re *ResultExpr) One(args ...any) error {
	ok, err := re.Next()
	if err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("no results")
	}
	err = re.Decode(args...)
	if err != nil {
		return err
	}
	re.Close()
	return nil
}

// getTypes returns the types in the order they appear in the query
func getTypes(ods []loc) []reflect.Type {
	isDup := make(map[reflect.Type]bool)
	ts := []reflect.Type{}
	for _, od := range ods {
		if t := od.typ; !isDup[t] {
			isDup[t] = true
			ts = append(ts, t)
		}
	}
	return ts
}

// All returns a slice containing all rows returned in the query. Each row is
// a slice of any types that contains all structs mentioned in the output expressions
// of the query.
func (re *ResultExpr) All() ([][]any, error) {
	var s [][]any

	ts := getTypes(re.outputs)

	for {
		ok, err := re.Next()
		if err != nil {
			return [][]any{}, err
		} else if !ok {
			break
		}

		rs := []any{}
		for _, t := range ts {
			rp := reflect.New(t)
			// We need to unwrap the struct inside the interface{}.
			r := rp.Elem()
			err := re.decodeValue(r)
			if err != nil {
				return [][]any{}, err
			}
			rs = append(rs, r.Interface())
		}

		s = append(s, rs)
	}

	re.Close()
	return s, nil
}

func (re *ResultExpr) Next() (bool, error) {
	if !re.rows.Next() {
		return false, nil
	}

	cols, err := re.rows.Columns()

	if err != nil {
		return false, fmt.Errorf("cannot advance row: %s", err)
	}
	ptrs := make([]any, len(cols))
	vs := make([]any, len(cols))
	for i, _ := range cols {
		ptrs[i] = &vs[i]
	}
	re.rows.Scan(ptrs...)

	offset := 0

	rs := []any{}

	for i, col := range cols {
		if col == "_sqlair_"+strconv.Itoa(i-offset) {
			rs = append(rs, vs[i])
		} else {
			offset++
		}
	}

	re.rs = rs

	return true, nil
}

func (re *ResultExpr) Decode(args ...any) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot decode expression: %s", err)
		}
	}()

	for _, arg := range args {
		if arg == nil {
			return fmt.Errorf("nil parameter")
		}

		v := reflect.ValueOf(arg)
		if v.Kind() != reflect.Pointer {
			return fmt.Errorf("none pointer parameter")
		}
		v = reflect.Indirect(v)

		re.decodeValue(v)
	}

	return nil
}

// decodeValue sets the fields in the reflected struct "v" which have tags
// corresponding to columns in current row of the query results.
func (re *ResultExpr) decodeValue(v reflect.Value) error {
	typeFound := false
	for i, outDest := range re.outputs {
		if outDest.typ == v.Type() {
			typeFound = true
			err := setValue(v, outDest.field, re.rs[i])
			if err != nil {
				return fmt.Errorf("struct %s: %s", v.Type().Name(), err)
			}
		}
	}
	if !typeFound {
		return fmt.Errorf("no output expression of type %s", v.Type().Name())
	}
	return nil
}

func setValue(dest reflect.Value, fInfo fielder, val any) error {
	var isZero bool

	v := reflect.ValueOf(val)
	typ := fInfo.Type()
	name := fInfo.Name()

	switch dest.Type().Kind() {
	case reflect.Struct:
		f, ok := fInfo.(field)
		if !ok {
			return fmt.Errorf("internal error: argument of type %#v has no member of type %#v", dest.Type(), f)
		}

		if val == nil {
			if f.omitEmpty {
				return nil
			}
			isZero = true
			v = reflect.Zero(typ)
		}
		if !isZero && v.Type() != typ {
			return fmt.Errorf("result of type %#v but field %#v is type %#v", v.Type().Name(), name, typ.Name())
		}
		itsField := dest.FieldByIndex(f.index)
		if !itsField.CanSet() {
			return fmt.Errorf("cannot set field %#v. CanAddr=%v", f.name, itsField.CanAddr())
		}
		itsField.Set(v)
		return nil
	case reflect.Map:
		m, ok := fInfo.(mapKey)
		if !ok {
			return fmt.Errorf("internal error: argument of type %#v has no member of type %#v", dest.Type(), m)
		}

		if val == nil {
			isZero = true
			v = reflect.Zero(typ)
		}
		if !isZero && v.Type() != typ {
			return fmt.Errorf("result of type %#v but map value %#v is type %#v", v.Type().Name(), name, typ.Name())
		}
		itsKey := dest.MapIndex(reflect.ValueOf(name))
		itsKey.Set(v)
		return nil
	default:
		return fmt.Errorf("unsupported argument of kind %#v received when setting its value", dest.Type().Kind())
	}
}

func (re *ResultExpr) Close() error {
	return re.rows.Close()
}
