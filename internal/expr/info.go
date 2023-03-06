package expr

import (
	"reflect"
)

// sqlair-provided M-type map.
type M map[string]any

// fielder is an interface for struct field or a map key.
type fielder interface {
	Name() string
}

// infoType is an interface for info on a struct type or an sqlair map type.
type infoType interface {
	Type() reflect.Type
}

// field represents reflection information about a field from some struct type.
type field struct {
	typ reflect.Type

	name string

	// index sequence for Type.FieldByIndex.
	index []int

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	omitEmpty bool
}

func (f field) Name() string {
	return f.name
}

// Info represents reflected information about a struct type.
type structInfo struct {
	typ reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]field
}

// Type returns the type of a struct for which sqlair keeps cached info.
func (in *structInfo) Type() reflect.Type {
	return in.typ
}

type mapKey struct {
	// Key name
	name string
}

// Name returns the string name of a map key.
func (mk mapKey) Name() string {
	return mk.name
}

type mapInfo struct {
	// map's type
	typ reflect.Type
}

// Type returns the type of a map for which sqlair keeps cached info.
func (m *mapInfo) Type() reflect.Type {
	return m.typ
}
