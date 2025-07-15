package sources

import (
	"context"
	"fmt"
)

// StreamSpec represents a declarative stream caching specification
// This is shared across all sources to avoid import cycles
type StreamSpec struct {
	DataProvider     string `json:"data_provider"`              // Ethereum address or "*"
	StreamID         string `json:"stream_id"`                  // Stream ID or "*" for provider-wide
	CronSchedule     string `json:"cron_schedule"`              // Standard cron expression
	From             *int64 `json:"from,omitempty"`             // Optional start timestamp
	IncludeChildren  bool   `json:"include_children,omitempty"` // Include children of composed streams (default: false)
	Source           string `json:"-"`                          // For debugging: "inline", "file:path.csv"
}

// ConfigSource represents a source of stream configuration data
// This interface allows for different configuration sources (inline JSON, CSV files, etc.)
type ConfigSource interface {
	// Name returns a human-readable name for this configuration source
	Name() string

	// Load loads stream specifications from this source
	// The context can be used for cancellation and the rawConfig contains
	// the raw configuration data from the TOML file
	Load(ctx context.Context, rawConfig map[string]string) ([]StreamSpec, error)

	// Validate performs any source-specific validation that can be done
	// without actually loading the data (e.g., checking if files exist)
	Validate(rawConfig map[string]string) error
}

// SourceFactory creates configuration sources based on the raw configuration
type SourceFactory struct{}

// NewSourceFactory creates a new source factory
func NewSourceFactory() *SourceFactory {
	return &SourceFactory{}
}

// CreateSources analyzes the raw configuration and creates appropriate sources
func (f *SourceFactory) CreateSources(rawConfig map[string]string) ([]ConfigSource, error) {
	var sources []ConfigSource
	hasInlineStreams := false
	hasCSVFile := false

	// Check for inline streams configuration
	if streamsJSON, exists := rawConfig["streams_inline"]; exists && streamsJSON != "" {
		hasInlineStreams = true
		sources = append(sources, NewInlineSource(streamsJSON))
	}

	// Check for CSV file-based configuration
	if csvFile, exists := rawConfig["streams_csv_file"]; exists && csvFile != "" {
		hasCSVFile = true
		// Base path for resolving relative paths
		basePath := rawConfig["config_base_path"]
		
		sources = append(sources, NewCSVSource(csvFile, basePath))
	}

	// Error if both inline and CSV are provided (mutually exclusive)
	if hasInlineStreams && hasCSVFile {
		return nil, fmt.Errorf("configuration error: cannot specify both 'streams_inline' and 'streams_csv_file' - use either inline JSON or CSV file, not both")
	}

	return sources, nil
}