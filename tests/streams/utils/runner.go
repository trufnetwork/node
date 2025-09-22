//go:build kwiltest

// Package testutils provides the main test orchestration logic for cache and ERC-20 bridge testing
package testutils

import (
	"context"
	"fmt"
	"testing"
	"time"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	// Import extensions for registration
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/tests/streams/utils/cache"
	"github.com/trufnetwork/node/tests/streams/utils/erc20"
	"github.com/trufnetwork/node/tests/streams/utils/service"

	erc20shim "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	orderedsync "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
)

// Options extends kwilTesting.Options with both cache and ERC-20 bridge configuration
type Options struct {
	*kwilTesting.Options
	Cache              *cache.CacheOptions
	ERC20Bridge        *erc20.ERC20BridgeConfig
	DisableCache       bool // Explicitly disable cache (overrides default)
	DisableERC20Bridge bool // Explicitly disable ERC-20 bridge (overrides default)
}

// RunSchemaTest is a wrapper around kwilTesting.RunSchemaTest that automatically
// handles both cache and ERC-20 bridge setup.
// it's necessary to use this instead of kwilTesting.RunSchemaTest because we need to clean up the ordered-sync extension after each test.
// otherwise, we will get the "already initialized" error
func RunSchemaTest(t TestingT, s kwilTesting.SchemaTest, options *Options) {
	// Convert to kwilTesting.Options - use type assertion since TestingT is an alias for testing.T
	testT := t.(*testing.T)
	kwilOpts := &kwilTesting.Options{
		UseTestContainer: true,
		Logger:           testT,
		SetupMetaStore:   true, // Enable kwild_chain schema for blockchain height testing
		InitialHeight:    1,    // Set initial blockchain height for tests (match production default)
	}
	if options != nil && options.Options != nil {
		kwilOpts = options.Options
		// Ensure meta store is always enabled for cache extension tests
		kwilOpts.SetupMetaStore = true
		if kwilOpts.InitialHeight <= 0 {
			kwilOpts.InitialHeight = 1 // Default test height matches production
		}
	}

	// Handle cache configuration
	var cacheConfig *cache.CacheOptions
	if options == nil || (options.Cache == nil && !options.DisableCache) {
		// Fallback: Enabled, no auto-refresh for tests
		cacheConfig = cache.NewCacheOptions().
			WithEnabled().
			WithMaxBlockAge(-1 * time.Second).   // Disable sync check
			WithResolutionSchedule("0 0 31 2 *") // Never auto-resolve
	} else if options.Cache != nil {
		cacheConfig = options.Cache
	}

	// Handle ERC-20 bridge configuration
	var erc20Config *erc20.ERC20BridgeConfig
	if options != nil && options.ERC20Bridge != nil && !options.DisableERC20Bridge {
		erc20Config = options.ERC20Bridge
	}

	// Create wrapped function tests
	wrappedTests := s.FunctionTests
	if cacheConfig != nil || erc20Config != nil {
		wrappedTests = wrapWithExtensionsSetup(context.Background(), s.FunctionTests, cacheConfig, erc20Config, kwilOpts)
	}

	// here we still don't support test cases
	// let's make it fatal error
	// but if needed it's easy to support it in the future
	if len(s.TestCases) > 0 {
		testT.Fatalf("test cases are not supported yet")
	}

	// Run each function test in isolation so that ordered-sync is reset
	// before the interpreter is created for that function. Resetting inside
	// the function is too late because EngineReadyHooks run at interpreter init.
	if len(wrappedTests) <= 1 {
		// Single function: simple path
		orderedsync.ForTestingReset()
		erc20shim.ForTestingResetSingleton()
		kwilTesting.RunSchemaTest(testT, kwilTesting.SchemaTest{
			Name:           s.Name,
			SeedScripts:    s.SeedScripts,
			SeedStatements: s.SeedStatements,
			FunctionTests:  wrappedTests,
			Owner:          s.Owner,
		}, kwilOpts)
		return
	}

	for _, fn := range wrappedTests {
		orderedsync.ForTestingReset()
		erc20shim.ForTestingResetSingleton()
		kwilTesting.RunSchemaTest(testT, kwilTesting.SchemaTest{
			Name:           s.Name,
			SeedScripts:    s.SeedScripts,
			SeedStatements: s.SeedStatements,
			FunctionTests:  []kwilTesting.TestFunc{fn},
			Owner:          s.Owner,
		}, kwilOpts)
	}
}

// wrapWithExtensionsSetup wraps test functions with cache extension initialization
// ERC20 setup removed - now handled directly in tests using WithERC20TestSetup
func wrapWithExtensionsSetup(ctx context.Context, originalFuncs []kwilTesting.TestFunc, cacheConfig *cache.CacheOptions, erc20Config *erc20.ERC20BridgeConfig, opts *kwilTesting.Options) []kwilTesting.TestFunc {
	wrapped := make([]kwilTesting.TestFunc, len(originalFuncs))
	for i, fn := range originalFuncs {
		originalFn := fn
		wrapped[i] = func(ctx context.Context, platform *kwilTesting.Platform) (testErr error) {
			// Collect cleanup functions to run after the test completes
			var cleanups []func()
			defer func() {
				for i := len(cleanups) - 1; i >= 0; i-- {
					if cleanups[i] != nil {
						cleanups[i]()
					}
				}
			}()

			// Setup cache extension if configured
			if cacheConfig != nil && cacheConfig.IsEnabled() {
				cleanup, err := setupCacheExtension(ctx, cacheConfig, platform)
				if err != nil {
					testErr = fmt.Errorf("failed to setup cache extension: %w", err)
					return
				}
				cleanups = append(cleanups, cleanup)
			}

			// Run original test
			err := originalFn(ctx, platform)
			if err != nil {
				testErr = err
			}

			return
		}
	}
	return wrapped
}

// setupCacheExtension sets up the cache extension for testing
func setupCacheExtension(ctx context.Context, cacheConfig *cache.CacheOptions, platform *kwilTesting.Platform) (func(), error) {
	// Setup basic cache test
	helper := cache.SetupCacheTest(ctx, platform, cacheConfig)

	// Create mock service for extension setup
	mockService := service.CreateDefaultService()

	// Parse and setup extension
	processedConfig, err := tn_cache.ParseConfig(mockService)
	if err != nil {
		helper.Cleanup()
		return nil, fmt.Errorf("failed to parse cache config: %w", err)
	}

	ext, err := tn_cache.SetupCacheExtension(ctx, processedConfig, platform.Engine, mockService)
	if err != nil {
		helper.Cleanup()
		return nil, fmt.Errorf("failed to setup cache extension: %w", err)
	}

	// Register extension for cleanup
	tn_cache.SetExtension(ext)

	cleanup := func() {
		// Stop background tasks
		if ext.Scheduler() != nil {
			if stopErr := ext.Scheduler().Stop(); stopErr != nil {
				mockService.Logger.Error("failed to stop scheduler", "error", stopErr)
			}
		}
		if ext.SyncChecker() != nil {
			ext.SyncChecker().Stop()
		}
		ext.Close()
		tn_cache.SetExtension(nil)
		// Cleanup helper last
		helper.Cleanup()
	}

	return cleanup, nil
}

// setupERC20Bridge sets up the ERC-20 bridge for testing
// ERC20 setup removed - now handled directly in tests using WithERC20TestSetup

// TestingT interface for test functions
type TestingT interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// WithExtensions removed - ERC20 setup now handled directly in tests
