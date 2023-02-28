package expr

import (
	"fmt"
	"reflect"
)

type M map[string]any

var mType = reflect.TypeOf(M{})

type mapKey struct {
	// Key name
	name string
}

func (mk mapKey) Name() string {
	return mk.name
}

// CheckValidMapType takes a reflect type and checks whether it is a map, the type name is M, and the key type of the map is string, and
// returns an error if any of these conditions is not true.
func CheckValidMapType(mt reflect.Type) error {
	if mt != mType {
		return fmt.Errorf(`map type is: %s, expected: %s`, mt.Name(), mType.Name())
	}
	/*
		// Map type name must be M
		if mt.Name() != reflect.TypeOf(M{}).Name() {
			return fmt.Errorf(`map type of name %s found, expected %s`, mt.Name(), reflect.TypeOf(M{}).Name())
		}
		var s string
		// Map must have string keys
		if mt.Key() != reflect.TypeOf(s) {
			return fmt.Errorf(`map type %s must have key type %s; found type %s`, mt.Name(), reflect.TypeOf(s).Name(), mt.Key().Name())
		}
	*/
	return nil
}

// IsValidMapType takes a reflect type and checks whether it is a map, the type name is M, and the key type of the map is string.
func IsValidMType(mt reflect.Type) bool {
	return mt == mType
	// var s string
	// return mt.Kind() == reflect.Map && mt.Name() == reflect.TypeOf(M{}).Name() && mt.Key() == reflect.TypeOf(s)
}
