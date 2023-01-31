package expr

import (
	"fmt"
	"reflect"
	"sort"
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

// getTypes returns the types in the order they are in the query
func getTypes(m typeToCols) []reflect.Type {
	i := 0
	keys := make([]reflect.Type, len(m))
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool { return m[keys[i]].firstCol < m[keys[j]].firstCol })
	return keys
}

// This version returns a slice rather than populating one
func (re *ResultExpr) AllV2() ([][]any, error) {
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

	rs := []res{}
	offset := 0

	for i, col := range cols {
		a := strings.Split(col, "_")
		if a[0] == "sqlair" {
			pos, err := strconv.Atoi(a[2])
			if err != nil {
				return false, fmt.Errorf("Invalid sqlair column name: %s", col)
			}
			rs = append(rs, res{[1]), pos, vs[i-offset]})
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

	for _, arg := range args {
		if arg == nil {
			return fmt.Errorf("nil parameter")
		}

		v := reflect.ValueOf(arg)
		if v.Kind() != reflect.Pointer {
			return fmt.Errorf("none pointer paramter")
		}
		v = reflect.Indirect(v)

		err := re.decodeValue(v)
		if err != nil {
			return err
		}
	}
	return nil
}

// decodeValue sets the fields in the reflected struct "v" which have tags
// corrosponding to columns in current row of the query results.
func (re *ResultExpr) decodeValue(v reflect.Value) error {
	info, err := typeInfoFromCache(v.Type())
	if err != nil {
		return err
	}

	r, ok := re.outputs[info.structType]
	if !ok {
		return fmt.Errorf("no output expression of type %s", info.structType.Name())
	}

	for i := r.firstCol; i <= r.lastCol; i++ {
		// f is in the map, we checked in the prepare stage
		f := info.tagToField[re.rs[i].tag]
		err := setValue(v, f, re.rs[i].val)
		if err != nil {
			return fmt.Errorf("struct %s: %s", info.structType.Name(), err)
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
