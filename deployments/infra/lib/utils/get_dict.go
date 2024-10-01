package utils

import (
	"fmt"
	"reflect"
)

// GetDictFromStruct returns a map of environment variables from a struct
// with the env tag
func GetDictFromStruct(s interface{}) map[string]string {
	envVars := make(map[string]string)
	v := reflect.ValueOf(s)

	// If s is a pointer, get the element it points to
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			// Return empty map or handle the nil pointer as needed
			return envVars
		}
		v = v.Elem()
	}

	// Ensure that v is a struct
	if v.Kind() != reflect.Struct {
		// Handle the case where s is neither a struct nor a pointer to a struct
		return envVars
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() == reflect.Ptr {
			if !field.IsNil() {
				envKey := t.Field(i).Tag.Get("env")
				elem := field.Elem()
				if elem.Kind() == reflect.String {
					envVars[envKey] = elem.String()
				} else {
					envVars[envKey] = fmt.Sprintf("%v", elem.Interface())
				}
			}
		}
	}
	return envVars
}
