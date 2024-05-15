package utils

import "reflect"

// GetDictFromStruct returns a map of environment variables from a struct
// with the env tag
func GetDictFromStruct(s interface{}) map[string]string {
	envVars := make(map[string]string)
	v := reflect.ValueOf(s)
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() == reflect.Ptr {
			if !field.IsNil() {
				envVars[t.Field(i).Tag.Get("env")] = field.Elem().String()
			}
		}
	}
	return envVars
}
