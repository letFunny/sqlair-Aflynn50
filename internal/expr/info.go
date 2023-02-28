package expr

import (
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

var mType = reflect.TypeOf(M{})

type mapKey struct {
	// Key name
	name string
}

func (mk mapKey) Name() string {
	return mk.name
}
