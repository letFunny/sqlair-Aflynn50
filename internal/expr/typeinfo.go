package expr

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

var scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

type typeMember interface {
	outerType() reflect.Type
	memberName() string
}

type mapKey struct {
	name    string
	mapType reflect.Type
}

func (mk *mapKey) outerType() reflect.Type {
	return mk.mapType
}

func (mk mapKey) memberName() string {
	return mk.name
}

// structField represents reflection information about a field from some struct type.
type structField struct {
	name string

	// The type of the containing struct.
	structType reflect.Type

	// Index for Type.Field.
	index []int

	// The tag assosiated with this field
	tag string

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	omitEmpty bool
}

func (f *structField) outerType() reflect.Type {
	return f.structType
}

func (f structField) memberName() string {
	return f.tag
}

type typeInfo interface {
	typ() reflect.Type
}

type structInfo struct {
	structType reflect.Type

	// Ordered list of tags
	tags []string

	tagToField map[string]*structField
}

func (si *structInfo) typ() reflect.Type {
	return si.structType
}

type mapInfo struct {
	mapType reflect.Type
}

func (mi *mapInfo) typ() reflect.Type {
	return mi.mapType
}

var cacheMutex sync.RWMutex
var cache = make(map[reflect.Type]typeInfo)

// Reflect will return the typeInfo of a given type,
// generating and caching as required.
func getTypeInfo(value any) (typeInfo, error) {
	if value == (any)(nil) {
		return nil, fmt.Errorf("cannot reflect nil value")
	}

	t := reflect.TypeOf(value)

	cacheMutex.RLock()
	typeInfo, found := cache[t]
	cacheMutex.RUnlock()
	if found {
		return typeInfo, nil
	}

	typeInfo, err := generateTypeInfo(t)
	if err != nil {
		return nil, err
	}

	cacheMutex.Lock()
	cache[t] = typeInfo
	cacheMutex.Unlock()

	return typeInfo, nil
}

// fieldIsStruct checks if the field type is an embedded/nested struct or
// pointer to an embedded/nested struct.
// If a pointer to the field type implementes Scanner then it is not a nested
// struct.
func fieldIsStruct(field reflect.StructField) bool {
	ft := field.Type
	k := ft.Kind()
	// Check if it is a
	return (k == reflect.Struct && !reflect.PointerTo(ft).Implements(scannerInterface)) ||
		(k == reflect.Pointer && ft.Elem().Kind() == reflect.Struct && !ft.Implements(scannerInterface))
}

// getStructFields returns relevent reflection information about all struct
// fields included nested/embedded fields.
// The caller is required to check that t is a struct.
func getStructFields(structType reflect.Type) ([]*structField, error) {
	var fields []*structField
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if !field.IsExported() {
			return nil, fmt.Errorf("field %q of struct %s not exported", field.Name, structType.Name())
		}
		if fieldIsStruct(field) {
			fieldType := field.Type
			if fieldType.Kind() == reflect.Pointer {
				fieldType = fieldType.Elem()
			}
			// Promote the nested struct fields into the current parent struct
			// scope, making sure to update the Index list for navigation back
			// to the original nested location.
			nestedFields, err := getStructFields(fieldType)
			if err != nil {
				return nil, err
			}
			for _, nestedField := range nestedFields {
				nestedField.index = append([]int{i}, nestedField.index...)
				nestedField.structType = structType
			}
			fields = append(fields, nestedFields...)
		} else {
			// Fields without a "db" tag are outside of SQLair's remit.
			tag := field.Tag.Get("db")
			if tag == "" {
				continue
			}
			tag, omitEmpty, err := parseTag(tag)
			if err != nil {
				return nil, fmt.Errorf("cannot parse tag for field %s.%s: %s", structType.Name(), field.Name, err)
			}
			fields = append(fields, &structField{
				name:       field.Name,
				index:      field.Index,
				omitEmpty:  omitEmpty,
				tag:        tag,
				structType: structType,
			})
		}
	}
	return fields, nil
}

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for SQLair operation.
func generateTypeInfo(t reflect.Type) (typeInfo, error) {
	switch t.Kind() {
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf(`map type %s must have key type string, found type %s`, t.Name(), t.Key().Kind())
		}
		return &mapInfo{mapType: t}, nil
	case reflect.Struct:
		info := structInfo{
			tagToField: make(map[string]*structField),
			structType: t,
		}
		fields, err := getStructFields(t)
		if err != nil {
			return nil, err
		}
		tags := []string{}
		for _, field := range fields {
			tags = append(tags, field.tag)
			if dup, ok := info.tagToField[field.tag]; ok {
				return nil, fmt.Errorf("db tag %q appears in both field %q and field %q of struct %q",
					field.tag, field.name, dup.name, t.Name())
			}
			info.tagToField[field.tag] = field
		}

		sort.Strings(tags)
		info.tags = tags

		return &info, nil
	default:
		return nil, fmt.Errorf("internal error: cannot obtain type information for type that is not map or struct: %s.", t)
	}
}

// This expression should be aligned with the bytes we allow in isNameByte in
// the parser.
var validColNameRx = regexp.MustCompile(`^([a-zA-Z_])+([a-zA-Z_0-9])*$`)

// parseTag parses the input tag string and returns its
// name and whether it contains the "omitempty" option.
func parseTag(tag string) (string, bool, error) {
	options := strings.Split(tag, ",")

	var omitEmpty bool
	if len(options) > 1 {
		for _, flag := range options[1:] {
			if flag == "omitempty" {
				omitEmpty = true
			} else {
				return "", omitEmpty, fmt.Errorf("unsupported flag %q in tag %q", flag, tag)
			}
		}
	}

	name := options[0]
	if len(name) == 0 {
		return "", false, fmt.Errorf("empty db tag")
	}

	if !validColNameRx.MatchString(name) {
		return "", false, fmt.Errorf("invalid column name in 'db' tag: %q", name)
	}

	return name, omitEmpty, nil
}
