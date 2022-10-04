package parse

import (
	"fmt"
	"reflect"
	"strings"
)

func GetReflectValue(value any) (reflect.Value, error) {
	var v reflect.Value
	if value == (any)(nil) {
		return v, fmt.Errorf("cannot use nil value as an argument")
	}

	v = reflect.ValueOf(value)
	if v.IsNil() {
		return v, fmt.Errorf("cannot use nil value as an argument")
	}

	// Get the reflect.value of the thing v points to
	v = reflect.Indirect(v)
	return v, nil

}

// parseTag parses the input tag string and returns its
// name and whether it contains the "omitempty" option.
func ParseTag(tag string) (string, bool, error) {
	options := strings.Split(tag, ",")

	var omitEmpty bool
	if len(options) > 1 {
		if strings.ToLower(options[1]) != "omitempty" {
			return "", false, fmt.Errorf("unexpected tag value %q", options[1])
		}
		omitEmpty = true
	}

	return options[0], omitEmpty, nil
}
