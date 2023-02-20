package expr

import (
	"reflect"
)

type M map[string]any

type mapInfo struct {
	typ reflect.Type
}

func (m *mapInfo) Type() reflect.Type {
	return m.typ
}

type mapKey struct {
	// Type of the value that this key maps to
	typ reflect.Type

	// Key name
	name string
}

func (mk mapKey) Name() string {
	return mk.name
}

func (mk mapKey) Type() reflect.Type {
	return mk.typ
}
