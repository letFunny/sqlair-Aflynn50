package expr

import (
	"fmt"
	"reflect"
)

type M map[string]any

// type mapKeyValuePair struct {
// 	// key name
// 	name string
// 	// value type
// 	typ reflect.Type
// }

type keyValueTypes map[string]reflect.Type

type mapInfo struct {
	// map's type
	typ reflect.Type

	// types of values this map is expected to get
	//vtypes []mapKeyValuePair
	kvtypes keyValueTypes
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

func CheckValidMType(mt reflect.Type) error {
	// Map type name must be M
	if mt.Name() != reflect.TypeOf(M{}).Name() {
		return fmt.Errorf(`map type of name %s found, expected %s`, mt.Name(), reflect.TypeOf(M{}).Name())
	}
	var s string
	// Map must have string keys
	if mt.Key() != reflect.TypeOf(s) {
		return fmt.Errorf(`map type %s must have key type %s; found type %s`, mt.Name(), reflect.TypeOf(s).Name(), mt.Key().Name())
	}
	// Map must have interface{} values
	var a any
	if mt.Elem() != reflect.TypeOf(a) {
		return fmt.Errorf(`map type %s must have value type %s; found type %s`, mt.Name(), reflect.TypeOf(a).Name(), mt.Elem().Name())
	}
	return nil
}
