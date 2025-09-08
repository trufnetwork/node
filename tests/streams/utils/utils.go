// Package testutils provides utilities for testing Kwil schemas and extensions.
// This package maintains backward compatibility while organizing functionality into focused subpackages.
//
// For cache testing: see cache package
// For ERC-20 bridge testing: see erc20 package
// For combined testing: see runner package
package testutils

import (
	"time"

	"github.com/trufnetwork/kwil-db/extensions/listeners"
	"github.com/trufnetwork/kwil-db/extensions/precompiles"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	// Extension registration
	"github.com/trufnetwork/node/extensions/tn_cache"
	"github.com/trufnetwork/node/tests/streams/utils/cache"
	"github.com/trufnetwork/node/tests/streams/utils/erc20"
)

// init registers tn_cache precompiles globally for tests
func init() {
	err := precompiles.RegisterInitializer(tn_cache.ExtensionName, tn_cache.InitializeCachePrecompile)
	if err != nil {
		panic("failed to register tn_cache precompiles: " + err.Error())
	}
}

// ============================================================================
// BACKWARD COMPATIBILITY RE-EXPORTS
// ============================================================================

// Type Aliases for backward compatibility
type CacheOptions = cache.CacheOptions
type StreamConfig = cache.StreamConfig
type ERC20BridgeConfig = erc20.ERC20BridgeConfig
type ERC20ChainConfig = erc20.ERC20ChainConfig
type ERC20BridgeTestHelper = erc20.ERC20BridgeTestHelper
type CacheTestHelper = cache.CacheTestHelper

// ============================================================================
// CACHE FUNCTIONS (RE-EXPORTS)
// ============================================================================

// Cache configuration builders
func NewCacheOptions() *CacheOptions { return cache.NewCacheOptions() }

// ============================================================================
// ERC-20 FUNCTIONS (RE-EXPORTS)
// ============================================================================

// ERC-20 configuration builders
func NewERC20BridgeConfig() *ERC20BridgeConfig { return erc20.NewERC20BridgeConfig() }

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

func GetTestOptionsWithERC20Bridge(erc20Opts *ERC20BridgeConfig) *Options {
	return &Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
		ERC20Bridge: erc20Opts,
	}
}

func GetTestOptionsWithBoth(cacheOpts *CacheOptions, erc20Opts *ERC20BridgeConfig) *Options {
	opts := &Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
	}

	if cacheOpts != nil {
		opts.Cache = cacheOpts
		opts.DisableCache = false
	}

	if erc20Opts != nil {
		opts.ERC20Bridge = erc20Opts
		opts.DisableERC20Bridge = false
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

func SimpleERC20Bridge(rpcURL, signerKey string) *ERC20BridgeConfig {
	return NewERC20BridgeConfig().
		WithRPC("sepolia", rpcURL).
		WithSigner("test_bridge", signerKey)
}

func MockERC20Bridge(mockListener listeners.ListenFunc) *ERC20BridgeConfig {
	return NewERC20BridgeConfig().
		WithRPC("mock", "ws://mock-rpc").
		WithSigner("mock_bridge", "/dev/null").
		WithMockListener("evm_sync", mockListener)
}

// MockERC20Listener creates a mock listener for testing ERC-20 bridge functionality
func MockERC20Listener(expectedEvents []string) listeners.ListenFunc {
	return erc20.MockERC20Listener(expectedEvents)
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

func Ptr[T any](v T) *T {
	return &v
}
