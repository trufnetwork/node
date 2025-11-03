package sources

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/trufnetwork/kwil-db/core/log"
)

// Configuration keys
const (
	ConfigKeyStreamsInline  = "streams_inline"
	ConfigKeyStreamsCSVFile = "streams_csv_file"
)

// CLI flags and environment variables
const (
	RootFlagLong   = "--root"
	RootFlagShort  = "-r"
	RootEnvVar     = "KWILD_ROOT"
	DefaultRootDir = ".kwild"
)

// StreamSpec represents a declarative stream caching specification
// This is shared across all sources to avoid import cycles
type StreamSpec struct {
	DataProvider    string `json:"data_provider"`              // Ethereum address or "*"
	StreamID        string `json:"stream_id"`                  // Stream ID or "*" for provider-wide
	CronSchedule    string `json:"cron_schedule"`              // Standard cron expression
	From            *int64 `json:"from,omitempty"`             // Optional start timestamp
	BaseTime        *int64 `json:"base_time,omitempty"`        // Optional base_time variant
	IncludeChildren bool   `json:"include_children,omitempty"` // Include children of composed streams (default: false)
	Source          string `json:"-"`                          // For debugging: "inline", "file:path.csv"
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
type SourceFactory struct {
	logger log.Logger
}

// NewSourceFactory creates a new source factory
func NewSourceFactory(logger log.Logger) *SourceFactory {
	return &SourceFactory{logger: logger}
}

// inferRootDir parses os.Args to find the --root (or -r) flag value.
// Falls back to env var KWILD_ROOT, then default (~/.kwild) if not found.
//
// Rationale: Kwil-DB's RootDir (set via --root flag or default) determines
// where config.toml and other files live, but it's not part of the TOML
// config struct nor passed to extensions via common.Service. To auto-resolve
// relative paths (e.g., for streams_csv_file) consistently without core
// changes or extra user config, we parse it from os.Args at runtime. This
// mirrors Kwil's CLI logic precisely, handling flags, env, and defaults.
// Alternatives like global vars or context passing were more invasive.
func inferRootDir() (string, error) {
	// Helper for default
	defaultRoot := func() (string, error) {
		u, err := user.Current()
		if err != nil {
			return "", fmt.Errorf("failed to get user home: %w", err)
		}
		return filepath.Join(u.HomeDir, DefaultRootDir), nil
	}

	// Step 1: Check os.Args for --root or -r
	args := os.Args
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == RootFlagLong || arg == RootFlagShort {
			if i+1 >= len(args) {
				return "", fmt.Errorf("missing value for %s flag", arg)
			}
			val := args[i+1]
			if val == "" || strings.HasPrefix(val, "-") {
				return "", fmt.Errorf("invalid value for %s flag", arg)
			}
			return val, nil
		} else if strings.HasPrefix(arg, RootFlagLong+"=") || strings.HasPrefix(arg, RootFlagShort+"=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) != 2 || parts[1] == "" || strings.HasPrefix(parts[1], "-") {
				return "", fmt.Errorf("invalid value for %s flag", parts[0])
			}
			return parts[1], nil
		}
	}

	// Step 2: Fallback to env var (if set, e.g., KWILD_ROOT=/custom)
	if envRoot := os.Getenv(RootEnvVar); envRoot != "" {
		return envRoot, nil
	}

	// Step 3: Default
	return defaultRoot()
}

// CreateSources analyzes the raw configuration and creates appropriate sources
func (f *SourceFactory) CreateSources(rawConfig map[string]string) ([]ConfigSource, error) {
	var sources []ConfigSource
	hasInlineStreams := false
	hasCSVFile := false

	// Check for inline streams configuration
	if streamsJSON, exists := rawConfig[ConfigKeyStreamsInline]; exists && streamsJSON != "" {
		hasInlineStreams = true
		sources = append(sources, NewInlineSource(streamsJSON))
	}

	// Check for CSV file-based configuration
	if csvFile, exists := rawConfig[ConfigKeyStreamsCSVFile]; exists && csvFile != "" {
		hasCSVFile = true

		// Always infer base path from root directory
		basePath, err := inferRootDir()
		if err != nil {
			return nil, fmt.Errorf("failed to infer root directory: %w", err)
		}

		sources = append(sources, NewCSVSource(csvFile, basePath, f.logger))
	}

	// Error if both inline and CSV are provided (mutually exclusive)
	if hasInlineStreams && hasCSVFile {
		return nil, fmt.Errorf("configuration error: cannot specify both '%s' and '%s' - use either inline JSON or CSV file, not both", ConfigKeyStreamsInline, ConfigKeyStreamsCSVFile)
	}

	return sources, nil
}
