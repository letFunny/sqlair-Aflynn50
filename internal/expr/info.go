package expr

import (
	"reflect"
)

// Field represents a single field from a struct type.
type field struct {
	typ reflect.Type

	// Name is the name of the struct field.
	name string

	// Index sequence for Type.FieldByIndex
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

	// Relate tag names to fields.
	tagToField map[string]field
}
