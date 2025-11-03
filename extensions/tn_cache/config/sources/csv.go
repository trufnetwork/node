package sources

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/trufnetwork/kwil-db/core/log"
)

// CSVSource handles stream configurations loaded from CSV files
type CSVSource struct {
	filePath string
	basePath string // Base path for resolving relative file paths
	logger   log.Logger
}

// NewCSVSource creates a new CSV configuration source
func NewCSVSource(filePath, basePath string, logger log.Logger) *CSVSource {
	return &CSVSource{
		filePath: filePath,
		basePath: basePath,
		logger:   logger,
	}
}

// Name returns the name of this configuration source
func (s *CSVSource) Name() string {
	return fmt.Sprintf("file:%s", s.filePath)
}

// Load reads the CSV file and returns StreamSpec objects
func (s *CSVSource) Load(ctx context.Context, rawConfig map[string]string) ([]StreamSpec, error) {
	resolvedPath := s.resolveFilePath()

	if s.logger != nil {
		s.logger.Info("Loading streams from CSV file", "resolved_path", resolvedPath, "original_path", s.filePath, "base_path", s.basePath)
	}

	file, err := os.Open(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file %s: %w", resolvedPath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true
	reader.Comment = '#'        // Allow comments in CSV files
	reader.FieldsPerRecord = -1 // Allow variable number of fields per record

	var specs []StreamSpec
	lineNumber := 0
	headerIndices := map[string]int{}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV file %s at line %d: %w", resolvedPath, lineNumber+1, err)
		}

		lineNumber++

		// Skip empty lines and lines that are all whitespace
		if len(record) == 0 || (len(record) == 1 && strings.TrimSpace(record[0]) == "") {
			continue
		}

		// Skip header if present
		if lineNumber == 1 && isHeader(record) {
			headerIndices = buildHeaderIndex(record)
			continue
		}

		spec, err := s.parseCSVRecord(record, headerIndices, lineNumber)
		if err != nil {
			return nil, fmt.Errorf("error parsing CSV file %s at line %d: %w", resolvedPath, lineNumber, err)
		}

		// Set source for debugging
		spec.Source = fmt.Sprintf("file:%s", resolvedPath)
		specs = append(specs, spec)
	}

	if s.logger != nil {
		s.logger.Info("Successfully loaded stream specifications from CSV", "file", resolvedPath, "count", len(specs))
	}

	return specs, nil
}

// Validate checks if the CSV file exists and is readable
func (s *CSVSource) Validate(rawConfig map[string]string) error {
	resolvedPath := s.resolveFilePath()

	// Check if file exists
	if _, err := os.Stat(resolvedPath); os.IsNotExist(err) {
		return fmt.Errorf("CSV file does not exist: %s", resolvedPath)
	} else if err != nil {
		return fmt.Errorf("error accessing CSV file %s: %w", resolvedPath, err)
	}

	// Check if file is readable
	file, err := os.Open(resolvedPath)
	if err != nil {
		return fmt.Errorf("CSV file is not readable: %s: %w", resolvedPath, err)
	}
	file.Close()

	return nil
}

// resolveFilePath resolves relative paths against the base path
func (s *CSVSource) resolveFilePath() string {
	if filepath.IsAbs(s.filePath) {
		return s.filePath
	}

	if s.basePath == "" {
		return s.filePath
	}

	return filepath.Join(s.basePath, s.filePath)
}

// isHeader checks if the record matches the expected CSV header
func isHeader(record []string) bool {
	if len(record) < 3 {
		return false
	}
	return strings.TrimSpace(record[0]) == "data_provider" &&
		strings.TrimSpace(record[1]) == "stream_id" &&
		strings.TrimSpace(record[2]) == "cron_schedule"
}

func buildHeaderIndex(record []string) map[string]int {
	header := make(map[string]int, len(record))
	for idx, col := range record {
		name := strings.ToLower(strings.TrimSpace(col))
		if name == "" {
			continue
		}
		header[name] = idx
	}
	return header
}

// parseCSVRecord converts a CSV record into a StreamSpec.
// Expected CSV format: data_provider,stream_id,cron_schedule[,from][,include_children][,base_time]
func (s *CSVSource) parseCSVRecord(record []string, header map[string]int, lineNumber int) (StreamSpec, error) {
	if len(record) < 3 {
		return StreamSpec{}, fmt.Errorf("insufficient columns: expected at least 3 (data_provider, stream_id, cron_schedule), got %d", len(record))
	}

	spec := StreamSpec{IncludeChildren: false, Source: s.Name()}

	if len(header) > 0 {
		getField := func(name string) (string, bool) {
			idx, ok := header[strings.ToLower(name)]
			if !ok || idx >= len(record) {
				return "", false
			}
			return strings.TrimSpace(record[idx]), true
		}

		if value, ok := getField("data_provider"); ok {
			spec.DataProvider = value
		}
		if spec.DataProvider == "" {
			return StreamSpec{}, fmt.Errorf("data_provider cannot be empty")
		}

		if value, ok := getField("stream_id"); ok {
			spec.StreamID = value
		}
		if spec.StreamID == "" {
			return StreamSpec{}, fmt.Errorf("stream_id cannot be empty")
		}

		if value, ok := getField("cron_schedule"); ok {
			spec.CronSchedule = value
		}
		if spec.CronSchedule == "" {
			return StreamSpec{}, fmt.Errorf("cron_schedule cannot be empty")
		}

		if value, ok := getField("from"); ok && value != "" {
			from, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return StreamSpec{}, fmt.Errorf("invalid from timestamp '%s': %w", value, err)
			}
			spec.From = &from
		}

		if value, ok := getField("include_children"); ok && value != "" {
			includeChildren, err := strconv.ParseBool(value)
			if err != nil {
				return StreamSpec{}, fmt.Errorf("invalid include_children value '%s': expected true/false, got %w", value, err)
			}
			spec.IncludeChildren = includeChildren
		}

		if value, ok := getField("base_time"); ok && value != "" {
			baseTime, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return StreamSpec{}, fmt.Errorf("invalid base_time value '%s': %w", value, err)
			}
			spec.BaseTime = &baseTime
		}

		return spec, nil
	}

	spec.DataProvider = strings.TrimSpace(record[0])
	spec.StreamID = strings.TrimSpace(record[1])
	spec.CronSchedule = strings.TrimSpace(record[2])

	// Parse optional columns sequentially: from, include_children, base_time
	columnIndex := 3

	// Optional from timestamp
	if len(record) > columnIndex {
		fromStr := strings.TrimSpace(record[columnIndex])
		if fromStr != "" {
			from, err := strconv.ParseInt(fromStr, 10, 64)
			if err != nil {
				return StreamSpec{}, fmt.Errorf("invalid from timestamp '%s': %w", fromStr, err)
			}
			spec.From = &from
		}
		columnIndex++
	}

	// Optional include_children boolean or base_time (if include_children omitted)
	if len(record) > columnIndex {
		value := strings.TrimSpace(record[columnIndex])
		if value != "" {
			if strings.EqualFold(value, "true") || strings.EqualFold(value, "false") {
				includeChildren, _ := strconv.ParseBool(value)
				spec.IncludeChildren = includeChildren
				columnIndex++
			} else if baseTime, err := strconv.ParseInt(value, 10, 64); err == nil {
				spec.BaseTime = &baseTime
				columnIndex++
			} else {
				return StreamSpec{}, fmt.Errorf("invalid include_children/base_time value '%s': expected true/false or integer", value)
			}
		} else {
			columnIndex++
		}
	}

	// Optional base_time column if include_children present
	if len(record) > columnIndex {
		baseTimeStr := strings.TrimSpace(record[columnIndex])
		if baseTimeStr != "" {
			baseTime, err := strconv.ParseInt(baseTimeStr, 10, 64)
			if err != nil {
				return StreamSpec{}, fmt.Errorf("invalid base_time value '%s': %w", baseTimeStr, err)
			}
			spec.BaseTime = &baseTime
		}
	}

	// Basic validation of required fields
	if spec.DataProvider == "" {
		return StreamSpec{}, fmt.Errorf("data_provider cannot be empty")
	}
	if spec.StreamID == "" {
		return StreamSpec{}, fmt.Errorf("stream_id cannot be empty")
	}
	if spec.CronSchedule == "" {
		return StreamSpec{}, fmt.Errorf("cron_schedule cannot be empty")
	}

	return spec, nil
}
