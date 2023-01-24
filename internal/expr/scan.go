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

func (re *ResultExpr) All(args ...any) error {
	return nil
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
			return fmt.Errorf("cannot reflect nil value")
		}

		a := reflect.ValueOf(arg)
		a = reflect.Indirect(a)
		at := a.Type()

		if out.info.structType != at {
			return fmt.Errorf(`output expression of type "%s" but argument of type "%s"`, out.info.structType.Name(), at.Name())
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
		return fmt.Errorf("field %#v is not exported", f.name)
	}
	af.Set(v)
	return nil
}

func (re *ResultExpr) Close() error {
	return re.rows.Close()
}
