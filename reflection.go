package liteorm

import (
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"strconv"
	"strings"
)

// getObjectValue receives either a struct or pointer to struct type as argument, and returns its reflect.Value. If
// pointer, the value is dereferenced.
func getObjectValue(arg interface{}) (reflect.Value, error) {
	argv := reflect.ValueOf(arg)

	if argv.Kind() == reflect.Ptr {
		argv = argv.Elem()
	}

	if argv.Kind() != reflect.Struct {
		return reflect.Value{}, errors.New("provided argument is not a struct or pointer to struct")
	}

	return argv, nil
}

// getObjectType receives either a struct or pointer to struct type as argument, and returns its reflect.Type. If
// pointer, the value is dereferenced.
func getObjectType(arg interface{}) (reflect.Type, error) {
	argt := reflect.TypeOf(arg)

	if argt.Kind() == reflect.Ptr {
		argt = argt.Elem()
	}

	if argt.Kind() != reflect.Struct {
		return nil, errors.New("provided argument is not a pointer")
	}

	return argt, nil
}

// getSliceValue receives a pointer to a slice type as argument (of type interface{}) and returns the value (of type
// reflect.Value).
func getSliceValue(arg interface{}) (reflect.Value, error) {
	if reflect.TypeOf(arg).Kind() == reflect.Ptr {
		// dereference the pointer to get the slice
		slice := reflect.ValueOf(arg).Elem()
		// verify that derefenced value is a slice
		if slice.Kind() == reflect.Slice {
			return slice, nil
		} else {
			return reflect.Value{}, errors.New("provided argument is not a pointer to a slice")
		}
	} else {
		return reflect.Value{}, errors.New("provided argument is not a pointer")
	}
}

// getSliceElemType receives a pointer to a slice type as argument and returns the type of the slice elements.
func getSliceElemType(arg interface{}) (reflect.Type, error) {
	if reflect.TypeOf(arg).Kind() != reflect.Ptr {
		return nil, errors.New("provided argument is not a pointer type")
	}

	// dereference the pointer to recover the slice
	slice := reflect.ValueOf(arg).Elem()

	if slice.Kind() != reflect.Slice {
		return nil, errors.New("provided argument is not a pointer to a slice")
	}

	// recover the type of the elements in the slice
	// the first call to .Elem() is to get the type of the slice (i.e. a pointer type)
	// the second call to .Elem() is to get the type of the pointer
	return slice.Type().Elem().Elem(), nil
}

// idColumnType is the PostgreSQL column type for ID columns.
var idColumnType = "bigserial"

// mapColumnType maps a reflect.StructField object to a PostgreSQL column type.
func mapColumnType(field reflect.StructField) (string, error) {
	switch field.Type.Kind() {
	// basic types
	case reflect.Int:
		return "int", nil

	case reflect.Int64:
		return "bigint", nil

	case reflect.String:
		var lenTag int
		lenTag, err := getLengthTag(field)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("varchar(%d)", lenTag), nil

	// composite types
	case reflect.Struct:
		switch fieldTypeName := fmt.Sprintf("%s.%s", field.Type.PkgPath(), field.Type.Name()); fieldTypeName {
		case "time.Time":
			return "timestamp", nil
		default:
			return "", errors.New(fmt.Sprintf("unsupported struct type - %s", fieldTypeName))
		}

	// slice types
	case reflect.Slice:
		switch fieldSliceKind := field.Type.Elem().Kind(); fieldSliceKind {
		case reflect.Uint8:
			return "bytea", nil
		default:
			return "", errors.New(fmt.Sprintf("unsupported slice kind - %s", fieldSliceKind))
		}

	default:
		return "", errors.New(fmt.Sprintf("unsupported field kind - %s", field.Type.Kind()))
	}
}

// getLengthTag returns the integer associated with the "pglen" tag of a reflect.StructField.
func getLengthTag(field reflect.StructField) (int, error) {
	stag := field.Tag.Get("pglen")
	if stag == "" {
		return 0, fmt.Errorf("len tag not present for field %s", field.Name)
	}

	itag, err := strconv.Atoi(stag)
	if err != nil {
		return 0, fmt.Errorf("len tag of field %s cannot be converted to int", field.Name)
	}

	return itag, nil
}

// setIDValue sets the ID field of the object received as argument.
func setIDValue(arg interface{}, value int64) error {
	argv, err := getObjectValue(arg)
	if err != nil {
		return err
	}

	idField := argv.FieldByName("ID")
	if idField.IsValid() == false || idField.CanSet() == false {
		return errors.New("could not set the ID field after inserting the object")
	}
	idField.SetInt(value)

	return nil
}

// getIDValue gets the ID field of the object received as argument.
func getIDValue(arg interface{}) (int64, error) {
	argv, err := getObjectValue(arg)
	if err != nil {
		return -1, err
	}

	idField := argv.FieldByName("ID")
	if idField.IsValid() == false {
		return -1, errors.New("could not set the ID field after inserting the object")
	}

	return idField.Int(), nil
}

// buildTableName generates the table name from the type name. It sets all characters to lower and adds an extra "s" for
// the plural form of the noun.
func BuildTableName(t reflect.Type) string {
	return fmt.Sprintf("%ss", strings.ToLower(t.Name()))
}

// buildSliceFromFields generates an slice of type []interface{}, where each element is of the same type as the fields of
// the first argument.
func buildSliceFromFields(arg reflect.Type) []interface{} {
	slice := make([]interface{}, arg.NumField())
	for i := 0; i < arg.NumField(); i++ {
		// in the line below, we are creating a new object of the type of the field; this is a pointer stored as a
		// reflect.Value object; we then use the .Interface() method to obtain the pointer to the newly created object
		slice[i] = reflect.New(arg.Field(i).Type).Interface()
	}
	return slice
}

// setObjectFields sets the values for each field of the object passed as first argument.
func setObjectFields(arg interface{}, values ...interface{}) error {
	argv, err := getObjectValue(arg)
	if err != nil {
		return err
	}

	if len(values) != argv.NumField() {
		return errors.New("mismatch between number of fields and number of values")
	}

	for i := 0; i < argv.NumField(); i++ {
		// in the line below, we are taking one interface{} which is actually a pointer to a specific object
		// and turning that into a reflect.Value object via reflect.ValueOf; afterwards, the .Elem() method
		// is called to dereference the pointer and get the underlying value
		argv.Field(i).Set(reflect.ValueOf(values[i]).Elem())
	}

	return nil
}

// makeSlice creates a slice where each element is of the type passed as argument.
func makeSlice(t reflect.Type) reflect.Value {
	return reflect.MakeSlice(reflect.SliceOf(t), 0, 0)
}
