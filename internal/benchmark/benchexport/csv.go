package benchexport

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
)

type SavedResults struct {
	Procedure  string `json:"procedure"`
	Depth      int    `json:"depth"`
	Days       int    `json:"days"`
	DurationMs int64  `json:"duration_ms"`
	Visibility string `json:"visibility"`
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
// - reads the CSV file and converts each row to a JSON string.
// - unmarshals the JSON string to a struct.
// - returns the slice of structs.
func LoadCSV[T any](reader io.Reader) ([]T, error) {
	csvReader := csv.NewReader(reader)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	var data []T

	for _, record := range records {
		item := new(T)
		jsonStr := "{" + strings.Join(record, ",") + "}"
		if err := json.Unmarshal([]byte(jsonStr), item); err != nil {
			return nil, err
		}
		data = append(data, *item)
	}

	return data, nil
}
