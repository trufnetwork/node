// Package cache provides configuration options for cache extension testing
package cache

import (
	"encoding/json"
	"fmt"
	"time"
)

// CacheOptions provides a fluent API for configuring the tn_cache extension
type CacheOptions struct {
	enabled            bool
	resolutionSchedule string
	maxBlockAge        string
	streams            []StreamConfig
}

// StreamConfig represents a single stream to cache
type StreamConfig struct {
	DataProvider    string `json:"data_provider"`
	StreamID        string `json:"stream_id"`
	CronSchedule    string `json:"cron_schedule"`
	From            *int64 `json:"from,omitempty"`
	BaseTime        *int64 `json:"base_time,omitempty"`
	IncludeChildren bool   `json:"include_children,omitempty"`
}

// NewCacheOptions creates a new cache configuration with defaults
func NewCacheOptions() *CacheOptions {
	return &CacheOptions{
		enabled: false, // Default to disabled
	}
}

// WithEnabled enables the cache extension
func (c *CacheOptions) WithEnabled() *CacheOptions {
	c.enabled = true
	return c
}

// WithDisabled explicitly disables the cache extension (default)
func (c *CacheOptions) WithDisabled() *CacheOptions {
	c.enabled = false
	return c
}

// WithResolutionSchedule sets the schedule for re-resolving wildcards (default: daily at midnight)
func (c *CacheOptions) WithResolutionSchedule(schedule string) *CacheOptions {
	c.resolutionSchedule = schedule
	return c
}

// WithMaxBlockAge sets the maximum age of blocks to consider the node synced
func (c *CacheOptions) WithMaxBlockAge(duration time.Duration) *CacheOptions {
	c.maxBlockAge = duration.String()
	return c
}

// WithStream adds a stream to cache
func (c *CacheOptions) WithStream(dataProvider, streamID, cronSchedule string) *CacheOptions {
	c.streams = append(c.streams, StreamConfig{
		DataProvider: dataProvider,
		StreamID:     streamID,
		CronSchedule: cronSchedule,
	})
	return c
}

// WithStreamBaseTime adds a stream with a fixed base_time variant.
func (c *CacheOptions) WithStreamBaseTime(dataProvider, streamID, cronSchedule string, baseTime int64) *CacheOptions {
	c.streams = append(c.streams, StreamConfig{
		DataProvider: dataProvider,
		StreamID:     streamID,
		CronSchedule: cronSchedule,
		BaseTime:     &baseTime,
	})
	return c
}

// WithStreamBaseTimeFromTime adds a stream with both base_time and from timestamp specified.
func (c *CacheOptions) WithStreamBaseTimeFromTime(dataProvider, streamID, cronSchedule string, baseTime, fromTime int64) *CacheOptions {
	c.streams = append(c.streams, StreamConfig{
		DataProvider: dataProvider,
		StreamID:     streamID,
		CronSchedule: cronSchedule,
		BaseTime:     &baseTime,
		From:         &fromTime,
	})
	return c
}

// WithStreamFromTime adds a stream with a specific start time
func (c *CacheOptions) WithStreamFromTime(dataProvider, streamID, cronSchedule string, fromTime int64) *CacheOptions {
	c.streams = append(c.streams, StreamConfig{
		DataProvider: dataProvider,
		StreamID:     streamID,
		CronSchedule: cronSchedule,
		From:         &fromTime,
	})
	return c
}

// WithComposedStream adds a composed stream with children included
func (c *CacheOptions) WithComposedStream(dataProvider, streamID, cronSchedule string, includeChildren bool) *CacheOptions {
	c.streams = append(c.streams, StreamConfig{
		DataProvider:    dataProvider,
		StreamID:        streamID,
		CronSchedule:    cronSchedule,
		IncludeChildren: includeChildren,
	})
	return c
}

// WithWildcardProvider adds all streams from a provider
func (c *CacheOptions) WithWildcardProvider(dataProvider, cronSchedule string) *CacheOptions {
	c.streams = append(c.streams, StreamConfig{
		DataProvider: dataProvider,
		StreamID:     "*",
		CronSchedule: cronSchedule,
	})
	return c
}

// Build converts the options into the metadata map for the extension
func (c *CacheOptions) Build() map[string]string {
	metadata := make(map[string]string)

	// Always set enabled status
	metadata["enabled"] = fmt.Sprintf("%v", c.enabled)

	// Only add other options if cache is enabled
	if c.enabled {
		if c.resolutionSchedule != "" {
			metadata["resolution_schedule"] = c.resolutionSchedule
		}

		if c.maxBlockAge != "" {
			metadata["max_block_age"] = c.maxBlockAge
		}

		if len(c.streams) > 0 {
			// Convert streams to JSON
			streamsJSON, err := json.Marshal(c.streams)
			if err != nil {
				panic(fmt.Sprintf("failed to marshal cache streams: %v", err))
			}
			metadata["streams_inline"] = string(streamsJSON)
		}
	}

	return metadata
}

// IsEnabled returns whether the cache is enabled
func (c *CacheOptions) IsEnabled() bool {
	return c.enabled
}

// GetStreams returns the configured streams
func (c *CacheOptions) GetStreams() []StreamConfig {
	return c.streams
}
