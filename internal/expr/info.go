package expr

import (
	"fmt"
	"reflect"
)

type fielder interface {
	//Type() reflect.Type
	Name() string
}

// field represents reflection information about a field from some struct type.
type field struct {
	typ reflect.Type

	name string

	// index sequence for Type.FieldByIndex
	index []int

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	omitEmpty bool
}

// Info represents reflected information about a struct type.
type info struct {
	typ reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]field
}

func (f field) Name() string {
	return f.name
}

type M map[string]any

type mapKey struct {
	// Key name
	name string
}

func (mk mapKey) Name() string {
	return mk.name
}

// CheckValidMapType should only be called on maps. It takes a reflect type and checks whether it is a map, the type name is M,
// and the key type of the map is string. It returns an error if any of these conditions is not true.
func CheckValidMapType(mt reflect.Type) error {
	if mt.Kind() != reflect.Map {
		return fmt.Errorf(`internal error: input type %s is not a map`, mt.Name())
	}
	// Map type name must be M
	if mt.Name() != "M" {
		return fmt.Errorf(`map type of name %s found, expected M`, mt.Name())
	}
	var s string
	// Map must have string keys
	if mt.Key() != reflect.TypeOf(s) {
		return fmt.Errorf(`map type %s must have key type string; found type %s`, mt.Name(), mt.Key().Name())
	}

	// Value must be of type any
	var v any
	if mt.Elem() != reflect.TypeOf(&v).Elem() {
		return fmt.Errorf(`map type %s must have value type any; found type %s`, mt.Name(), mt.Elem().Name())
	}
	return nil
}

// IsValidMapType takes a reflect type and checks whether it is a map, the type name is M, and the key type of the map is string.
func IsValidMType(mt reflect.Type) bool {
	var s string
	var v any
	return mt.Kind() == reflect.Map && mt.Name() == reflect.TypeOf(M{}).Name() && mt.Key() == reflect.TypeOf(s) && mt.Elem() == reflect.TypeOf(&v).Elem()
}
