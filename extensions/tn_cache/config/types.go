package config

import (
	"github.com/trufnetwork/kwil-db/core/types"
)

// RawConfig represents the raw TOML configuration from the node config file
type RawConfig struct {
	Enabled            string `toml:"enabled"`
	ResolutionSchedule string `toml:"resolution_schedule,omitempty"` // Cron for re-resolving wildcards/children (default: daily)
	StreamsInline      string `toml:"streams_inline,omitempty"`      // JSON array as string
	StreamsCSVFile     string `toml:"streams_csv_file,omitempty"`    // Path to CSV file

	// Sync-aware configuration
	MaxBlockAge types.Duration `toml:"max_block_age,omitempty"` // Max age of block to consider synced (e.g., "1h", "30m"; default: "1h")
}

// Note: StreamSpec is now defined in sources package to avoid import cycles

// ProcessedConfig is the final validated, immutable configuration
// This is what the extension will use at runtime
type ProcessedConfig struct {
	Enabled            bool             `json:"enabled"`
	ResolutionSchedule string           `json:"resolution_schedule"` // Cron expression for re-resolution
	Directives         []CacheDirective `json:"directives"`
	Sources            []string         `json:"sources"`       // Track config sources for debugging
	MaxBlockAge        int64            `json:"max_block_age"` // Max age in seconds to consider node synced (0 = disabled)
}

// CacheDirective represents a validated cache policy for a stream or set of streams
type CacheDirective struct {
	ID              string        `json:"id"`                  // Unique identifier for this directive
	Type            DirectiveType `json:"type"`                // Type of caching directive
	DataProvider    string        `json:"data_provider"`       // Normalized ethereum address (lowercase)
	StreamID        string        `json:"stream_id"`           // Specific stream ID or "*"
	BaseTime        *int64        `json:"base_time,omitempty"` // Optional base_time variant
	Schedule        Schedule      `json:"schedule"`            // Parsed cron schedule
	TimeRange       TimeRange     `json:"time_range"`          // From/To timestamps
	IncludeChildren bool          `json:"include_children"`    // Include children of composed streams (default: false)
	Metadata        Metadata      `json:"metadata"`            // Source tracking and priority
}

// DirectiveType defines the type of caching directive
type DirectiveType string

const (
	DirectiveSpecific         DirectiveType = "specific"          // Cache a specific stream
	DirectiveProviderWildcard DirectiveType = "provider_wildcard" // Cache all streams from provider
)

// Schedule holds both the raw cron expression and its parsed form
type Schedule struct {
	CronExpr string `json:"cron_expr"` // Original cron expression
	// Note: Parsed cron.Schedule is not included here to avoid import cycles
	// It will be parsed when needed in the execution layer
}

// TimeRange defines the time boundaries for caching
// Only 'from' timestamp is used - data is cached from this point forward
type TimeRange struct {
	From *int64 `json:"from,omitempty"` // Start timestamp (unix seconds)
}

// Metadata tracks the source and priority of configuration directives
type Metadata struct {
	Source   string `json:"source"`   // Source identifier: "inline", "file:streams.csv"
	Priority int    `json:"priority"` // Priority for conflict resolution (higher wins)
}
