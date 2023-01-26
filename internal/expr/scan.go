package expr

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
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

// For now this assumes you have a single struct as output.
// And that we pass an empty slice.
// This version of all gets the type from the slice, but this can only ever work
// with query returning one type (because we cant mix types in the slice.

func (re *ResultExpr) All(s any) error {
	if s == nil {
		return fmt.Errorf("cannot reflect nil value")
	}

	sv := reflect.ValueOf(s)

	if sv.Kind() != reflect.Pointer {
		return fmt.Errorf("not a pointer")
	}

	sv = sv.Elem()

	if sv.Kind() != reflect.Slice {
		return fmt.Errorf("cannot populate none slice type")
	}

	// Get element type of slice
	et := sv.Type().Elem()

	// Create a copy to avoid using value.Set every loop.
	svCopy := sv

	for {
		rp := reflect.New(et)

		r := rp.Elem()

		ok, err := re.Next()
		if err != nil {
			return err
		} else if !ok {
			break
		}

		err = re.Decode(&r)
		if err != nil {
			return err
		}
		svCopy = reflect.Append(svCopy, r)
	}
	sv.Set(svCopy)

	re.Close()
	return nil
}

// This version returns a slice rather than populating one
func (re *ResultExpr) AllV2() ([][]any, error) {
	var s [][]any
	var ts []reflect.Type
	// var ts = make([]reflect.Type, len(re.outputs))
	for _, os := range re.outputs {
		ts = append(ts, os.info.structType)
	}

	for {
		ok, err := re.Next()
		if err != nil {
			return [][]any{}, err
		} else if !ok {
			break
		}

		rs := []any{}
		rps := []any{}
		for _, t := range ts {
			rp := reflect.New(t)
			// We could leave this as a reflected value to avoid reflecting
			// again in decode.
			// r is an any value continaing a struct
			r := rp.Elem()
			//return nil, fmt.Errorf("can set: %v can addr: %v, its type is %T", reflect.ValueOf(&r).CanSet(), reflect.ValueOf(&r).CanAddr(), r)
			rs = append(rs, r)
			rps = append(rps, &r)
		}

		//return nil, fmt.Errorf("can set: %v can addr: %v", reflect.ValueOf(rps[0]).CanSet(), reflect.ValueOf(rps[0]).CanAddr())
		//return nil, fmt.Errorf("rps is %#v", rps)
		err = re.Decode(rps...)
		if err != nil {
			return [][]any{}, err
		}

		rips := []any{}
		for _, r := range rs {
			rip := r.(reflect.Value).Interface()
			rips = append(rips, rip)
		}
		s = append(s, rips)
	}

	re.Close()
	return s, nil
}

func (re *ResultExpr) Next(args ...any) (bool, error) {
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

	vals := []any{}
	offset := 0

	for i, col := range cols {
		if strings.HasPrefix(col, "_sqlair") && strings.HasSuffix(col, strconv.Itoa(i)) {
			vals = append(vals, vs[i-offset])
		} else {
			offset++
		}
	}

	re.vals = &vals

	return true, nil
}

func (re *ResultExpr) Decode(args ...any) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("cannot decode expression: %s", err)
		}
	}()
	if len(args) != len(re.outputs) {
		return fmt.Errorf("query has %d outputs but %d objects were provided", len(re.outputs), len(args))
	}

	i := 0

	for j, out := range re.outputs {
		arg := args[j]
		if arg == nil {
			return fmt.Errorf("nil parameter")
		}

		var a reflect.Value

		// 		// We need this if we are given something hiding behind an
		// 		// interface
		// 		if a.Kind() == reflect.Interface {
		// 			a = a.Elem()
		// 		}

		// Check if we have already been given a reflected value. This makes
		// All() much more efficient.
		if ap, ok := arg.(*reflect.Value); ok {
			a = *ap
		} else {
			a = reflect.ValueOf(arg)
			if a.Kind() != reflect.Pointer {
				return fmt.Errorf("none pointer paramter")
			}
			a = reflect.Indirect(a)
		}

		at := a.Type()

		if out.info.structType != at {
			return fmt.Errorf("output expression of type %#v but argument of type %#v", out.info.structType.Name(), at.Name())
		}

		for _, c := range out.columns {
			f, ok := out.info.tagToField[c]
			if !ok {
				return fmt.Errorf("no tag %#v in struct %#v", c, out.info.structType.Name())
			}
			err := setValue(a, f, (*re.vals)[i])
			if err != nil {
				return fmt.Errorf("struct %#v: %s", out.info.structType.Name(), err)
			}
			i++
		}

	}
	return nil
}

func setValue(a reflect.Value, f field, res any) error {
	var isZero bool

	v := reflect.ValueOf(res)

	if res == (any)(nil) {
		if f.omitEmpty {
			return nil
		}
		isZero = true
		v = reflect.Zero(f.fieldType)
	}

	if !isZero && v.Type() != f.fieldType {
		return fmt.Errorf("result of type %#v but field %#v is type %#v", v.Type().Name(), f.name, f.fieldType.Name())
	}
	af := a.Field(f.index)
	if !af.CanSet() {
		return fmt.Errorf("cannot set field %#v. CanAddr=%v", f.name, af.CanAddr())
	}
	af.Set(v)
	return nil
}

func (re *ResultExpr) Close() error {
	return re.rows.Close()
}
