package testutils

import (
	"context"
	"fmt"
	"testing"

	"github.com/trufnetwork/kwil-db/config"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/extensions/tn_cache"
)

// RunSchemaTest is a wrapper around kwilTesting.RunSchemaTest that automatically
// handles cache setup. By default, cache is enabled with a simple configuration.
// This allows existing tests to work with minimal changes.
func RunSchemaTest(t *testing.T, s kwilTesting.SchemaTest, options *Options) {
	// Convert our Options to kwilTesting.Options
	kwilOpts := &kwilTesting.Options{
		UseTestContainer: true,
		Logger:           t,
	}
	
	// Copy any kwilTesting.Options fields if provided
	if options != nil && options.Options != nil {
		kwilOpts = options.Options
	}
	
	// Determine cache configuration
	var cacheConfig *CacheOptions
	if options != nil && options.Cache != nil {
		cacheConfig = options.Cache
	} else if options == nil || !options.DisableCache {
		// By default, enable cache with a simple configuration
		// that won't auto-refresh (scheduled for Feb 31st)
		cacheConfig = NewCacheOptions().
			WithEnabled().
			WithMaxBlockAge(-1). // Disable sync checking for tests
			WithResolutionSchedule("0 0 0 31 2 *") // Never auto-refresh
	}
	
	// Set cache configuration BEFORE running the test
	// This ensures the extension sees the config during engineReadyHook
	if cacheConfig != nil {
		configMap := make(map[string]string)
		for k, v := range cacheConfig.Build() {
			configMap[k] = fmt.Sprintf("%v", v)
		}
		tn_cache.SetTestConfiguration(configMap)
		
		// Also set test DB configuration
		tn_cache.SetTestDBConfiguration(config.DBConfig{
			Host:   "localhost",
			Port:   "52853", // Default test container port
			User:   "kwild",
			Pass:   "kwild",
			DBName: "kwil_test_db",
		})
		
		// Clean up after all tests complete
		defer tn_cache.SetTestConfiguration(nil)
		defer tn_cache.SetTestDB(nil)
	}
	
	// If cache is configured, wrap test functions to inject DB connection
	if cacheConfig != nil && cacheConfig.enabled {
		s.FunctionTests = wrapTestFunctionsWithCache(s.FunctionTests, cacheConfig)
	}
	
	// Run the test with kwilTesting
	kwilTesting.RunSchemaTest(t, s, kwilOpts)
}


// wrapTestFunctionsWithCache wraps each test function to inject the test DB connection
func wrapTestFunctionsWithCache(funcs []kwilTesting.TestFunc, cacheConfig *CacheOptions) []kwilTesting.TestFunc {
	wrapped := make([]kwilTesting.TestFunc, len(funcs))
	
	for i, fn := range funcs {
		// Capture fn in closure
		originalFn := fn
		wrapped[i] = func(ctx context.Context, platform *kwilTesting.Platform) error {
			// Inject the test's database connection
			// Configuration was already set in RunSchemaTest
			tn_cache.SetTestDB(platform.DB)
			
			// Clean up after test
			defer tn_cache.SetTestDB(nil)
			
			// Run the original test function
			return originalFn(ctx, platform)
		}
	}
	
	return wrapped
}

