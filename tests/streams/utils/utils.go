//go:build kwiltest

// Package testutils provides utilities for testing Kwil schemas and extensions.
// This package maintains backward compatibility while organizing functionality into focused subpackages.
//
// For cache testing: see cache package
// For ERC-20 bridge testing: see erc20 package
// For combined testing: see runner package
package testutils

import (
	"time"

	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	// Extension registration
	"github.com/trufnetwork/node/extensions/database-size"
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/tests/streams/utils/cache"
)

// init registers extension precompiles globally for tests
func init() {
	err := precompiles.RegisterInitializer(tn_cache.ExtensionName, tn_cache.InitializeCachePrecompile)
	if err != nil {
		panic("failed to register tn_cache precompiles: " + err.Error())
	}

	err = precompiles.RegisterInitializer(database_size.ExtensionName, database_size.InitializeDatabaseSizePrecompile)
	if err != nil {
		panic("failed to register database_size precompiles: " + err.Error())
	}
}

// ============================================================================
// BACKWARD COMPATIBILITY RE-EXPORTS
// ============================================================================

// Type Aliases for backward compatibility
type CacheOptions = cache.CacheOptions
type StreamConfig = cache.StreamConfig

// ERC20 types removed - using simple pattern instead of complex fixtures
type CacheTestHelper = cache.CacheTestHelper

// ============================================================================
// CACHE FUNCTIONS (RE-EXPORTS)
// ============================================================================

// Cache configuration builders
func NewCacheOptions() *CacheOptions { return cache.NewCacheOptions() }

// ============================================================================
// ERC-20 FUNCTIONS (RE-EXPORTS)
// ============================================================================

// ERC-20 configuration builders removed - using simple pattern

// ============================================================================
// CONVENIENCE FUNCTIONS
// ============================================================================

func GetTestOptions() *kwilTesting.Options {
	return &kwilTesting.Options{
		UseTestContainer: true,
	}
}

func GetTestOptionsWithCache(cacheOpts ...*CacheOptions) *Options {
	opts := &Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
	}

	if len(cacheOpts) > 0 && cacheOpts[0] != nil {
		opts.Cache = cacheOpts[0]
		opts.DisableCache = false
	}

	return opts
}

func SimpleCache(dataProvider, streamID string) *CacheOptions {
	return NewCacheOptions().
		WithEnabled().
		WithMaxBlockAge(-1*time.Second).                 // Disable sync checking for tests
		WithResolutionSchedule("0 0 31 2 *").            // Never auto-resolve (Feb 31st)
		WithStream(dataProvider, streamID, "0 0 31 2 *") // Never auto-refresh (Feb 31st)
}

// ERC-20 functions removed - using simple pattern instead of complex configurations

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

func Ptr[T any](v T) *T {
	return &v
}
