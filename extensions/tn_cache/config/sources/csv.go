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

		spec, err := s.parseCSVRecord(record, lineNumber)
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

// parseCSVRecord converts a CSV record into a StreamSpec
// Expected CSV format: data_provider,stream_id,cron_schedule[,from][,include_children]
func (s *CSVSource) parseCSVRecord(record []string, lineNumber int) (StreamSpec, error) {
	if len(record) < 3 {
		return StreamSpec{}, fmt.Errorf("insufficient columns: expected at least 3 (data_provider, stream_id, cron_schedule), got %d", len(record))
	}

	spec := StreamSpec{
		DataProvider:    strings.TrimSpace(record[0]),
		StreamID:        strings.TrimSpace(record[1]),
		CronSchedule:    strings.TrimSpace(record[2]),
		IncludeChildren: false, // Default to false
		Source:          s.Name(),
	}

	// Parse optional fourth column (from timestamp)
	if len(record) > 3 && strings.TrimSpace(record[3]) != "" {
		fromStr := strings.TrimSpace(record[3])
		from, err := strconv.ParseInt(fromStr, 10, 64)
		if err != nil {
			return StreamSpec{}, fmt.Errorf("invalid from timestamp '%s': %w", fromStr, err)
		}
		spec.From = &from
	}

	// Parse optional fifth column (include_children boolean)
	if len(record) > 4 && strings.TrimSpace(record[4]) != "" {
		includeChildrenStr := strings.TrimSpace(record[4])
		includeChildren, err := strconv.ParseBool(includeChildrenStr)
		if err != nil {
			return StreamSpec{}, fmt.Errorf("invalid include_children value '%s': expected true/false, got %w", includeChildrenStr, err)
		}
		spec.IncludeChildren = includeChildren
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
