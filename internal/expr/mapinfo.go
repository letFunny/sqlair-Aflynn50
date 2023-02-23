package expr

import (
	"fmt"
	"reflect"
)

type M map[string]any

//type keyValueTypes map[string]reflect.Type

// type mapInfo struct {
// 	// map's type
// 	typ reflect.Type

// 	// key-value pairs this map is expected to retrieve
// 	kvtypes keyValueTypes
// }

// func (m *mapInfo) Type() reflect.Type {
// 	return m.typ
// }

type mapKey struct {
	// Type of the value that this key maps to
	//typ reflect.Type

	// Key name
	name string
}

func (mk mapKey) Name() string {
	return mk.name
}

// func (mk mapKey) Type() reflect.Type {
// 	return mk.typ
// }

func CheckValidMapType(mt reflect.Type) error {
	// Map type name must be M
	if mt.Name() != reflect.TypeOf(M{}).Name() {
		return fmt.Errorf(`map type of name %s found, expected %s`, mt.Name(), reflect.TypeOf(M{}).Name())
	}
	var s string
	// Map must have string keys
	if mt.Key() != reflect.TypeOf(s) {
		return fmt.Errorf(`map type %s must have key type %s; found type %s`, mt.Name(), reflect.TypeOf(s).Name(), mt.Key().Name())
	}
	return nil
}
