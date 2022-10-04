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

func (re *ResultExpr) Next() bool {
	if re.rows.Next() {
		return true
	}
	return false
}

func (re *ResultExpr) Scan(outputArgs ...any) error {

	columns, _ := re.rows.Columns() // move this stuff to Exec
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	colToIndex := map[string]int{}
	for j, colName := range columns {
		valuePtrs[j] = &values[j]
		colToIndex[colName] = j
	}
	re.rows.Scan(valuePtrs...)

	// Cycle through outputParts in the parsed expression, check them against the
	// outputVars passed to Scan, use reflect to put the correct column names
	// into the tagged field of the object
	var i int
	for _, part := range re.parsed.queryParts {
		if p, ok := part.(*OutputPart); ok {
			if len(outputArgs) <= i {
				return fmt.Errorf("not enough input values provided")
			}

			v, err := GetReflectValue(outputArgs[i])
			if err != nil {
				return err
			}

			switch v.Kind() {
			case reflect.Struct:
				typ := v.Type()

				if typ.Name() != p.Target.Prefix {
					return fmt.Errorf("name of the query output struct is %s but the argument struct has name %s", p.Target.Prefix, typ.Name())
				}
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
						colIndex, ok := colToIndex[tag]
						if !ok {
							return fmt.Errorf("no column in results with title %s", tag)
						}
						resVal := values[colIndex]

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
			}

		}
	}
	return nil
}
