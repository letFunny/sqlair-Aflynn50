package parse

import (
	"fmt"
	"reflect"
)

// Check that the args match then try and put the column inside
func (p *OutputPart) matchWithArg(value any) {
	// as
	return
} // Do this in Scan for now

func (p *OutputPart) isOutputColumn(s string) bool {
	if len(p.Columns) == 0 {
		return true
	}
	for _, c := range p.Columns {
		if c.Name == s || c.Name == "*" {
			return true
		}
	}
	return false

}

func (re *ResultExpr) Next() (bool, error) {
	var err error
	// Initialise colInfo
	if re.colInfo == nil {
		var colInfo colInfo

		colInfo.columns, err = re.rows.Columns() // move this stuff to Exec
		if err != nil {
			return false, fmt.Errorf("error when calling Next:i %s", err)
		}

		colInfo.values = make([]interface{}, len(colInfo.columns))
		colInfo.valuePtrs = make([]interface{}, len(colInfo.columns))
		colInfo.colToIndex = map[string]int{}
		for j, colName := range colInfo.columns {
			colInfo.valuePtrs[j] = &colInfo.values[j]
			colInfo.colToIndex[colName] = j
		}
		re.colInfo = &colInfo
	}

	if re.rows.Next() {
		return true, nil
	}
	return false, nil
}
func (re *ResultExpr) Close() error {
	return re.rows.Close()
}

func setOutput(colInfo *colInfo, p *OutputPart, arg any) error {

	v, err := GetReflectValue(arg)
	if err != nil {
		return err
	}

	typ := v.Type()

	if typ.Name() != p.Target.Prefix {
		return fmt.Errorf("name of the query output struct is %s but the argument struct has name %s", p.Target.Prefix, typ.Name())
	}
	switch v.Kind() {
	case reflect.Struct:
		// Cycle through the fields of the struct we're dumping into, if
		// a tag matches one of the column titles of the &OutputExrp
		// then place it in (or simply if it has a tag then look for a
		// matching column, if none found, no problem)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			// Fields without a "db" tag are outside of Sqlair's remit.
			tag := field.Tag.Get("db")
			if tag == "" {
				continue
			}

			tag, omitEmpty, err := ParseTag(tag)
			if err != nil {
				return err
			}
			// I have a tag, in the output field, first I want to check
			// that we're putting it into the output struct so I need to
			// check if its in the columns (or columns is empty and we
			// just go for it)
			if p.isOutputColumn(tag) && !omitEmpty {
				// Then this is struct we want to output into.
				// look for sql result column with this tag name.
				colIndex, ok := colInfo.colToIndex[tag]
				if !ok {
					return fmt.Errorf("no column in results with title %s", tag)
				}
				resVal := colInfo.values[colIndex]

				outputField := v.Field(i)
				resValType := reflect.TypeOf(resVal)
				if !outputField.CanSet() {
					return fmt.Errorf("the field %s of %s is not exported", field.Name, p.Target.Prefix)
				}
				if resValType == outputField.Type() {
					outputField.Set(reflect.ValueOf(resVal))
				} else {
					return fmt.Errorf("the column %s is type %s but the struct %s has type %s", tag, resValType, field.Name, outputField.Type())
				}
			}
		}
	case reflect.Map:
		// Should we check if it specfically the SQLair type M?
		if typ.Key().Kind() != reflect.String {
			return fmt.Errorf("key type of output map: %s is not string", typ.Name())
		}

		// If the type is
		if p.Target.Name == "*" {
			for _, dbCol := range colInfo.columns {
				// SetMapIndex requires the both the key and the value to be of
				// type reflect.Value.
				resVal := reflect.ValueOf(colInfo.values[colInfo.colToIndex[dbCol]])
				key := reflect.ValueOf(dbCol)
				v.SetMapIndex(key, resVal)
			}
		} else {
			// Check that the column to put in the map exists in the results.
			if colIndex, ok := colInfo.colToIndex[p.Target.Name]; ok {
				resVal := reflect.ValueOf(colInfo.values[colIndex])
				key := reflect.ValueOf(colInfo.columns[colIndex])
				v.SetMapIndex(key, resVal)
			} else {
				return fmt.Errorf("the column %s specified in the map key is a column returned by the query", p.Target.Name)
			}
		}
	}
	return nil
}

func (re *ResultExpr) Scan(outputArgs ...any) error {
	if re.colInfo == nil {
		return fmt.Errorf("the Next of ResultExpr must be called before the first Scan")
	}

	re.rows.Scan(re.colInfo.valuePtrs...)

	// Cycle through outputParts in the parsed expression, check them against the
	// outputVars passed to Scan, use reflect to put the correct column names
	// into the tagged field of the object
	var i int
	for _, part := range re.parsed.queryParts {
		if p, ok := part.(*OutputPart); ok {
			if len(outputArgs) <= i {
				return fmt.Errorf("not enough input values provided")
			}
			setOutput(re.colInfo, p, outputArgs[i])
		}
	}
	return nil
}
