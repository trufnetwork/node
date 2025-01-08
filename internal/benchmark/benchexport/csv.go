package benchexport

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type SavedResults struct {
	Procedure       string `json:"procedure"`
	BranchingFactor int    `json:"branching_factor"`
	QtyStreams      int    `json:"qty_streams"`
	DataPoints      int    `json:"data_points"`
	DurationMs      int64  `json:"duration_ms"`
	Visibility      string `json:"visibility"`
	Samples         int    `json:"samples"`
	UnixOnly        bool   `json:"unix_only"`
}

// SaveOrAppendToCSV saves a slice of any struct type to a CSV file, using JSON tags for headers.
// - appends the data to the file if it already exists, or creates it if it doesn't exist.
// - writes the header based on the struct tags if the file is empty.
// - uses reflection to get the struct tags and write the header and data to the file.
func SaveOrAppendToCSV[T any](data []T, filePath string) error {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Check if the file is empty
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		// Write header based on struct tags
		header, err := getHeaderFromStruct(reflect.TypeOf((*T)(nil)).Elem())
		if err != nil {
			return err
		}
		if err = writer.Write(header); err != nil {
			return err
		}
	}

	// Write data rows
	for _, item := range data {
		row, err := getRowFromStruct(item)
		if err != nil {
			return err
		}
		if err = writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

// getHeaderFromStruct extracts field names from JSON tags of a struct
func getHeaderFromStruct(t reflect.Type) ([]string, error) {
	var header []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		header = append(header, strings.Split(tag, ",")[0])
	}
	if len(header) == 0 {
		return nil, fmt.Errorf("no valid JSON tags found in struct")
	}
	return header, nil
}

// getRowFromStruct extracts field values from a struct
func getRowFromStruct(item interface{}) ([]string, error) {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()

	var row []string
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		value := fmt.Sprintf("%v", v.Field(i).Interface())
		row = append(row, value)
	}
	return row, nil
}

// LoadCSV loads a slice of any struct type from a CSV file, using JSON tags for fields.
// - gets the header from the first line of the CSV file, maps the header to the struct fields
// - reads the CSV file and converts each row to a JSON string.
// - unmarshals the JSON string to a struct.
// - returns the slice of structs.
func LoadCSV[T any](reader io.Reader) ([]T, error) {
	csvReader := csv.NewReader(reader)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 1 {
		return nil, fmt.Errorf("CSV file is empty")
	}

	var data []T

	header := records[0]
	records = records[1:]

	headerMap := make(map[string]int)
	for i, h := range header {
		headerMap[h] = i
	}

	t := reflect.TypeOf((*T)(nil)).Elem()

	for _, record := range records {
		item := reflect.New(t).Interface()
		v := reflect.ValueOf(item).Elem()

		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			tag := field.Tag.Get("json")
			if tag == "" || tag == "-" {
				continue
			}
			tagName := strings.Split(tag, ",")[0]

			if index, ok := headerMap[tagName]; ok && index < len(record) {
				value := record[index]
				if err := setField(v.Field(i), value); err != nil {
					return nil, fmt.Errorf("error setting field %s: %v", field.Name, err)
				}
			}
		}

		data = append(data, reflect.ValueOf(item).Elem().Interface().(T))
	}

	return data, nil
}

// setField sets the value of a struct field based on its type
func setField(field reflect.Value, value string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if intValue, err := strconv.ParseInt(value, 10, 64); err == nil {
			field.SetInt(intValue)
		} else {
			return err
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if uintValue, err := strconv.ParseUint(value, 10, 64); err == nil {
			field.SetUint(uintValue)
		} else {
			return err
		}
	case reflect.Float32, reflect.Float64:
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			field.SetFloat(floatValue)
		} else {
			return err
		}
	case reflect.Bool:
		if boolValue, err := strconv.ParseBool(value); err == nil {
			field.SetBool(boolValue)
		} else {
			return err
		}
	default:
		return fmt.Errorf("unsupported field type: %v", field.Kind())
	}

	return nil
}
