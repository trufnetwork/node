package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"
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
func (c *CacheOptions) Build() map[string]string {
	metadata := make(map[string]string)

	// Always set enabled status
	metadata["enabled"] = strconv.FormatBool(c.enabled)

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
	cacheConfig          *CacheOptions
	platform             *kwilTesting.Platform
	*tn_cache.TestHelper // Embedded core helper
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

	helper := &CacheTestHelper{
		cacheConfig: cacheConfig,
		platform:    platform,
	}
	helper.TestHelper = tn_cache.GetTestHelper()
	return helper
}

// Cleanup should be called with defer to clean up test injection
func (h *CacheTestHelper) Cleanup() {
	tn_cache.SetTestDB(nil)
}

// IsCacheEnabled returns whether the cache is enabled
func (h *CacheTestHelper) IsCacheEnabled() bool {
	return h.cacheConfig.enabled
}

// GetDB returns the test database for direct queries
func (h *CacheTestHelper) GetDB() sql.DB {
	return h.platform.DB
}

// SetDB sets the test database for direct queries
func (h *CacheTestHelper) SetDB(db sql.DB) {
	h.platform.DB = db
	tn_cache.SetTestDB(db)

	// Also ensure the scheduler uses the correct namespace for tests
	// In tests, the seed scripts create actions without namespace prefix,
	// which means they go into the test database's default namespace.
	// Since Engine.Call with empty string looks in "main", but tests might not have "main",
	// we need to figure out the right namespace.
	//
	// For now, we'll keep the default empty namespace and let the resolution
	// be retried when TriggerStreamResolution is called.
	// The initial resolution failure is logged but not fatal.
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

// RunInTx runs the provided test function within a database transaction.
// It automatically sets the cache DB to the transaction DB before running the function,
// then restores the original DB after. This ensures cache operations see uncommitted changes
// made within the transaction (e.g., newly created streams).
//
// Usage:
//
//	helper.RunInTx(t, func(t *testing.T, txPlatform *kwilTesting.Platform) {
//	    // Your test code here - create streams, trigger resolution, etc.
//	})
func (h *CacheTestHelper) RunInTx(t *testing.T, testFn func(t *testing.T, txPlatform *kwilTesting.Platform)) {
	WithTx(h.platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
		// Save original DB and set tx DB
		originalDB := h.GetDB()
		h.SetDB(txPlatform.DB)
		defer h.SetDB(originalDB) // Restore after

		testFn(t, txPlatform)
	})(t)
}

// RunSchemaTest is a wrapper around kwilTesting.RunSchemaTest that automatically
// handles cache setup. By default, cache is enabled with a test fallback.
func RunSchemaTest(t *testing.T, s kwilTesting.SchemaTest, options *Options) {
	// Convert to kwilTesting.Options
	kwilOpts := &kwilTesting.Options{
		UseTestContainer: true,
		Logger:           t,
	}
	if options != nil && options.Options != nil {
		kwilOpts = options.Options
	}

	// Default to enabled with fallback (as original schema.go)
	var cacheConfig *CacheOptions
	if options == nil || (options.Cache == nil && !options.DisableCache) {
		// Fallback: Enabled, no auto-refresh for tests
		cacheConfig = NewCacheOptions().
			WithEnabled().
			WithMaxBlockAge(-1 * time.Second).     // Disable sync check
			WithResolutionSchedule("0 0 0 31 2 *") // Never auto-resolve
	} else if options != nil && options.Cache != nil {
		cacheConfig = options.Cache
	}

	// Run with wrapper
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:          s.Name,
		SeedScripts:   s.SeedScripts,
		FunctionTests: wrapWithCacheSetup(context.Background(), s.FunctionTests, cacheConfig, kwilOpts),
	}, kwilOpts)
}

// wrapWithCacheSetup wraps test functions with full cache initialization
func wrapWithCacheSetup(ctx context.Context, originalFuncs []kwilTesting.TestFunc, cacheConfig *CacheOptions, opts *kwilTesting.Options) []kwilTesting.TestFunc {
	if cacheConfig == nil || !cacheConfig.enabled {
		return originalFuncs
	}

	wrapped := make([]kwilTesting.TestFunc, len(originalFuncs))
	for i, fn := range originalFuncs {
		originalFn := fn
		wrapped[i] = func(ctx context.Context, platform *kwilTesting.Platform) error {
			// Partial setup (DB config, injection, config map)
			helper := SetupCacheTest(ctx, platform, cacheConfig)
			defer helper.Cleanup()

			// Full init (restore original schema.go logic)
			mockService := &common.Service{
				Logger: log.NewStdoutLogger(),
				LocalConfig: &config.Config{
					DB: config.DBConfig{
						Host:   "localhost",
						Port:   "52853",
						User:   "kwild",
						Pass:   "kwild",
						DBName: "kwil_test_db",
					},
					Extensions: map[string]map[string]string{
						tn_cache.ExtensionName: cacheConfig.Build(),
					},
				},
			}

			processedConfig, err := tn_cache.ParseConfig(mockService)
			if err != nil {
				return fmt.Errorf("failed to parse config: %w", err)
			}

			ext, err := tn_cache.SetupCacheExtension(ctx, processedConfig, platform.Engine, mockService)
			if err != nil {
				return fmt.Errorf("failed to setup extension: %w", err)
			}
			tn_cache.SetExtension(ext)
			defer tn_cache.SetExtension(nil) // Cleanup

			// Run original test
			return originalFn(ctx, platform)
		}
	}
	return wrapped
}
