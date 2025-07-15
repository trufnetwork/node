package instructions

import (
	"time"

	"github.com/robfig/cron/v3"
)

// Directive represents a validated, executable cache instruction
// This is the final form that the cache execution engine will use
type Directive struct {
	ID           string        `json:"id"`            // Unique identifier
	Type         DirectiveType `json:"type"`          // "specific" | "provider_wildcard"
	DataProvider string        `json:"data_provider"` // Normalized (lowercase) ethereum address
	StreamID     string        `json:"stream_id"`     // Specific ID or "*"
	Schedule     Schedule      `json:"schedule"`      // Parsed cron schedule
	TimeRange    TimeRange     `json:"time_range"`    // From/To timestamps
	Metadata     Metadata      `json:"metadata"`      // Source tracking, priority, etc.
}

// DirectiveType defines the behavior of a cache directive
type DirectiveType string

const (
	// DirectiveSpecific caches a specific stream from a specific provider
	DirectiveSpecific DirectiveType = "specific"
	
	// DirectiveProviderWildcard caches all streams from a specific provider
	DirectiveProviderWildcard DirectiveType = "provider_wildcard"
)

// Schedule contains both the original cron expression and the parsed schedule
type Schedule struct {
	CronExpr string        `json:"cron_expr"`     // Original cron expression for serialization
	Parsed   cron.Schedule `json:"-"`             // Parsed schedule object (not serialized)
}

// TimeRange defines the temporal boundaries for cache operations
// Only 'from' timestamp is used - data is cached from this point forward
type TimeRange struct {
	From *int64 `json:"from,omitempty"` // Start timestamp (unix seconds)
}

// Metadata contains additional information about the directive
type Metadata struct {
	Source   string `json:"source"`   // Configuration source: "inline", "file:path.csv"
	Priority int    `json:"priority"` // For conflict resolution (higher priority wins)
}

// IsProviderWildcard returns true if this directive caches all streams from a provider
func (d *Directive) IsProviderWildcard() bool {
	return d.Type == DirectiveProviderWildcard
}

// IsSpecific returns true if this directive caches a specific stream
func (d *Directive) IsSpecific() bool {
	return d.Type == DirectiveSpecific
}

// GetCacheKey returns a unique key for this cache directive
// Used for deduplication and conflict resolution
func (d *Directive) GetCacheKey() string {
	return d.DataProvider + ":" + d.StreamID
}

// NextExecution returns the next time this directive should execute
func (d *Directive) NextExecution() time.Time {
	if d.Schedule.Parsed == nil {
		// Return zero time if schedule is not parsed
		return time.Time{}
	}
	return d.Schedule.Parsed.Next(time.Now())
}