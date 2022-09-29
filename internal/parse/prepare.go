package parse

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

type PreparedExpr struct {
	parsed    *ParsedExpr
	inputArgs []any
}

// parseTag parses the input tag string and returns its
// name and whether it contains the "omitempty" option.
func parseTag(tag string) (string, bool, error) {
	options := strings.Split(tag, ",")

	var omitEmpty bool
	if len(options) > 1 {
		if strings.ToLower(options[1]) != "omitempty" {
			return "", false, errors.Errorf("unexpected tag value %q", options[1])
		}
		omitEmpty = true
	}

	return options[0], omitEmpty, nil
}

// Can this function do the full process of checking and returning the value to
// plug in? We can factorise out the reflect stuff later if it has cominalities
// with the output.

// Check the name of the inputPart matches the name of the argument
// 	We do have to generate tag info since we look up the name of the field by
// 	tag rather than by field.

// Cases:
// 	Struct
//   Use the tag data to get the value out. Also check the name of the struct
//   matches
//  Map:

func reflectInputValue(p *InputPart, value any) (any, error) {

	if value == (any)(nil) {
		return nil, fmt.Errorf("cannot use nil value as an argument")
	}

	v := reflect.ValueOf(value)
	if v.IsNil() {
		return nil, fmt.Errorf("cannot use nil value as an argument")
	}

	// Get the reflect.value of the thing v points to
	v = reflect.Indirect(v)

	switch v.Kind() {
	case reflect.Struct:
		typ := v.Type()
		if typ.Name() != p.Prefix {
			return nil, fmt.Errorf("name of parameter struct is %s but the argument struct has name %s", p.Prefix, typ.Name())
		}
		if p.Name == "" {
			return nil, fmt.Errorf("cannot use a struct as a parameter")
		}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			// Fields without a "db" tag are outside of Sqlair's remit.
			tag := field.Tag.Get("db")
			if tag == "" {
				continue
			}

			tag, omitEmpty, err := parseTag(tag)
			if err != nil {
				return nil, err
			}
			// If the input parameters has a * use the first field with a tag
			if (p.Name == tag || p.Name == "*") && !omitEmpty {
				// Return the field as an interface of type any
				return v.Field(i).Interface(), nil
			}
		}
		return nil, fmt.Errorf("there is no tag with name %s in %s", p.Name, p.Prefix)
	case reflect.Map:
		// Also check that the argument is actually of our own type M
		if p.Name != "M" {
			return nil, fmt.Errorf("use sqlair type M use a map as an argument")
		}
		for _, key := range v.MapKeys() {
			// This has to be a string becuse M is a map: map[string]any
			if key.String() == p.Name {
				return v.MapIndex(key).Interface(), nil
			}
		}
		return nil, fmt.Errorf("key %s not in input map", p.Name)
	// default is its a variable
	default:
		return v.Interface(), nil
	}
}

// Prepare checks that the arguments provided match the names in the query and
// stores the information required to fit the resulting columns into the output.
// In it we first check that the arguments match the I/O expressions in the
// query then store the pairings of which resulting columns should go in which
// output expression. Since the column names are the same as the struct tags
// this also conatins the information about them.
// We also should work out if we need to output into a map or some such thing.

// Prepare should just take input arguments right?
// Say we just ignore all output values and only worry about them once we have
// the scan, will that theoretically work?
func (pe *ParsedExpr) Prepare(inputArgs ...any) (*PreparedExpr, error) {
	var i int
	var args []any

	for _, part := range pe.queryParts {
		// *part is used instead of part because only *InputPart implements the
		// interface queryPart, not InputPart
		if p, ok := part.(*InputPart); ok {
			if len(inputArgs) <= i {
				return nil, fmt.Errorf("not enough input values provided")
			}
			arg, err := reflectInputValue(p, inputArgs[i])
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			i++
		}
	}
	if i < len(inputArgs) {
		return nil, fmt.Errorf("%v inputs in query but %v inputs provided", i, len(inputArgs))
	}
	return &PreparedExpr{pe, args}, nil

}
