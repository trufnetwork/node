package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/node/types/sql"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	// make sure the extension is loaded
	"github.com/trufnetwork/node/extensions/tn_cache"
)

func Ptr[T any](v T) *T {
	return &v
}

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
func (c *CacheOptions) Build() map[string]any {
	metadata := make(map[string]any)

	// Always set enabled status
	metadata["enabled"] = c.enabled

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

// Options extends kwilTesting.Options with cache configuration
type Options struct {
	*kwilTesting.Options
	Cache        *CacheOptions
	DisableCache bool // Explicitly disable cache (overrides default)
}

// Common cache configurations

// DisabledCache returns cache options with cache disabled (default)
func DisabledCache() *CacheOptions {
	return NewCacheOptions()
}

// SimpleCache returns cache options with basic hourly caching
func SimpleCache(dataProvider, streamID string) *CacheOptions {
	return NewCacheOptions().
		WithEnabled().
		WithStream(dataProvider, streamID, "0 0 * * * *") // Hourly
}

// TestCache returns cache options suitable for testing with manual refresh
func TestCache(dataProvider, streamID string) *CacheOptions {
	return NewCacheOptions().
		WithEnabled().
		WithMaxBlockAge(-1*time.Second).                   // Disable sync checking for tests
		WithStream(dataProvider, streamID, "0 0 0 31 2 *") // Only on Feb 31st (never happens)
}

// ProductionCache returns cache options suitable for production with daily updates
func ProductionCache() *CacheOptions {
	return NewCacheOptions().
		WithEnabled().
		WithMaxBlockAge(1 * time.Hour).       // Default 1 hour
		WithResolutionSchedule("0 0 0 * * *") // Re-resolve daily at midnight
}

// Example usage:
//
// Basic cache disabled (default):
//   testutils.GetTestOptions()
//
// Simple cache for one stream:
//   testutils.GetTestOptions(testutils.SimpleCache("0x123...", "stream1"))
//
// Complex cache configuration:
//   cache := testutils.NewCacheOptions().
//     WithEnabled().
//     WithMaxBlockAge(30 * time.Minute).
//     WithStream("0x123...", "stream1", "0 0 * * * *").        // Hourly
//     WithStreamFromTime("0x456...", "stream2", "0 */30 * * * *", 1700000000). // Every 30 min from timestamp
//     WithComposedStream("0x789...", "composed1", "0 0 0 * * *", true).        // Daily with children
//     WithWildcardProvider("0xabc...", "0 0 */6 * * *")                        // All streams every 6 hours
//   testutils.GetTestOptions(cache)

// GetTestOptions returns the common test options with optional cache configuration
// By default, cache is DISABLED to maintain backward compatibility
// 
// DEPRECATED: This returns *kwilTesting.Options for backward compatibility.
// New tests should use GetTestOptionsWithCache() or the testutils.RunSchemaTest wrapper.
func GetTestOptions(cacheOpts ...*CacheOptions) *kwilTesting.Options {
	// For backward compatibility, return kwilTesting.Options
	// Cache configuration is ignored when using kwilTesting.RunSchemaTest directly
	return &kwilTesting.Options{
		UseTestContainer: true,
	}
}

// GetTestOptionsWithCache returns the extended options for use with testutils.RunSchemaTest
// By default, cache is DISABLED to maintain backward compatibility
func GetTestOptionsWithCache(cacheOpts ...*CacheOptions) *Options {
	opts := &Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
		DisableCache: true, // Default to disabled for backward compatibility
	}
	
	// If cache options are provided, enable cache with those options
	if len(cacheOpts) > 0 && cacheOpts[0] != nil {
		opts.Cache = cacheOpts[0]
		opts.DisableCache = false
	}
	
	return opts
}

// CacheTestHelper provides utilities for cache-enabled tests
type CacheTestHelper struct {
	cacheConfig *CacheOptions
	platform    *kwilTesting.Platform
}

// SetupCacheTest is a helper to set up cache configuration and test DB injection
// It should be called at the beginning of your test function
func SetupCacheTest(ctx context.Context, platform *kwilTesting.Platform, cacheConfig *CacheOptions) *CacheTestHelper {
	// Set up test database configuration (uses test container defaults)
	tn_cache.SetTestDBConfiguration(config.DBConfig{
		Host:   "localhost",
		Port:   "52853", // Default test container port
		User:   "kwild",
		Pass:   "kwild",
		DBName: "kwil_test_db",
	})

	// Inject the test's database connection
	tn_cache.SetTestDB(platform.DB)

	// Configure the extension
	configMap := make(map[string]string)
	for k, v := range cacheConfig.Build() {
		configMap[k] = fmt.Sprintf("%v", v)
	}
	tn_cache.SetTestConfiguration(configMap)

	return &CacheTestHelper{
		cacheConfig: cacheConfig,
		platform:    platform,
	}
}

// Cleanup should be called with defer to clean up test injection
func (h *CacheTestHelper) Cleanup() {
	tn_cache.SetTestDB(nil)
}

// RefreshCache manually triggers a cache refresh for a stream
func (h *CacheTestHelper) RefreshCache(ctx context.Context, dataProvider, streamID string) (int, error) {
	helper := tn_cache.GetTestHelper()
	if helper == nil {
		return 0, fmt.Errorf("cache test helper not available")
	}
	return helper.RefreshCacheSync(ctx, dataProvider, streamID)
}

// IsCacheEnabled returns whether the cache is enabled
func (h *CacheTestHelper) IsCacheEnabled() bool {
	return h.cacheConfig.enabled
}

// GetDB returns the test database for direct queries
func (h *CacheTestHelper) GetDB() sql.DB {
	return h.platform.DB
}

// WithCache is a test wrapper that sets up and tears down cache configuration
// Usage:
//
//	testutils.WithCache(t, ctx, platform, cacheConfig, func(cache *CacheTestHelper) {
//	    // Your test code here
//	})
func WithCache(t *testing.T, ctx context.Context, platform *kwilTesting.Platform, cacheConfig *CacheOptions, testFunc func(*CacheTestHelper)) {
	helper := SetupCacheTest(ctx, platform, cacheConfig)
	defer helper.Cleanup()
	testFunc(helper)
}

// Helper functions for creating Options

// DefaultOptions returns options with cache enabled by default
func DefaultOptions() *Options {
	return &Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
		// Cache is enabled by default with no specific configuration
	}
}

// OptionsWithCache returns options with a specific cache configuration
func OptionsWithCache(cache *CacheOptions) *Options {
	return &Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
		Cache: cache,
	}
}

// OptionsWithoutCache returns options with cache explicitly disabled
func OptionsWithoutCache() *Options {
	return &Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
		DisableCache: true,
	}
}
