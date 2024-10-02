package utils

import (
	"fmt"
	"reflect"
	"strings"
)

type ParameterDescriptor struct {
	FieldName      string
	EnvName        string
	SSMPath        string
	EnvValue       string
	IsSecure       bool
	IsSSMParameter bool
}

// GetParameterDescriptors extracts environment and SSM parameter descriptors from the provided struct.
//
// This function accepts a non-nil pointer to a struct and introspects its fields for `env` and `ssm` tags.
// Fields without an `env` tag are ignored. For each field with an `env` tag, a `ParameterDescriptor` is created,
// capturing the field name and corresponding environment variable name.
//
// If a field also includes an `ssm` tag, the function parses it to extract the SSM parameter path and determines
// whether the parameter is secure based on the presence of the "secure" option.
//
// Returns a slice of `ParameterDescriptor` containing metadata for each relevant field, or an error if
// the input does not meet the required criteria (i.e., not a non-nil pointer to a struct).
//
// Example:
//
//	type Config struct {
//	    DatabaseURL string `env:"DB_URL" ssm:"/prod/db/url,secure"`
//		Name 		string `env:"NAME"`
//	}
//
//	descriptors, err := GetParameterDescriptors(&Config{})
//	if err != nil {
//	    // handle error
//	}
//	// Use descriptors as needed
func GetParameterDescriptors(params interface{}) ([]ParameterDescriptor, error) {
	var descriptors []ParameterDescriptor

	val := reflect.ValueOf(params)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return nil, fmt.Errorf("params must be a non-nil pointer")
	}
	elem := val.Elem()
	if elem.Kind() != reflect.Struct {
		return nil, fmt.Errorf("params must point to a struct")
	}
	typ := elem.Type()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		envTag := field.Tag.Get("env")
		ssmTag := field.Tag.Get("ssm")

		if envTag == "" {
			continue // Skip fields without 'env' tag
		}

		var envValue string
		fieldValue := elem.Field(i)

		switch fieldValue.Kind() {
		case reflect.String:
			envValue = fieldValue.String()
		case reflect.Ptr:
			if !fieldValue.IsNil() && fieldValue.Elem().Kind() == reflect.String {
				envValue = fieldValue.Elem().String()
			} else {
				envValue = "" // Or handle nil pointers as needed
			}
		default:
			return nil, fmt.Errorf("unsupported field type for env tag: %s", field.Type)
		}

		descriptor := ParameterDescriptor{
			FieldName: field.Name,
			EnvName:   envTag,
			EnvValue:  envValue,
		}

		if ssmTag != "" {
			parts := strings.Split(ssmTag, ",")
			paramName := parts[0]
			descriptor.SSMPath = paramName
			descriptor.IsSSMParameter = true

			for _, option := range parts[1:] {
				if strings.TrimSpace(option) == "secure" {
					descriptor.IsSecure = true
					break
				}
			}
		} else {
			descriptor.IsSSMParameter = false
		}

		descriptors = append(descriptors, descriptor)
	}

	return descriptors, nil
}
