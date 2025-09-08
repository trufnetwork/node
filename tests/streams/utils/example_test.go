package testutils

import (
	"testing"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

func TestExampleERC20BridgeTesting(t *testing.T) {
	// Example of how to use the reorganized ERC-20 bridge testing utilities

	// Setup ERC-20 bridge configuration
	erc20Config := NewERC20BridgeConfig().
		WithRPC("sepolia", "wss://your-sepolia-provider.com").
		WithSigner("test_bridge", "/path/to/signer/key").
		WithAutoStart()

	// Create test options
	opts := GetTestOptionsWithERC20Bridge(erc20Config)

	// Define your schema test
	schemaTest := kwilTesting.SchemaTest{
		Name: "ERC-20 Bridge Test",
		// ... your test cases
	}

	// Note: In a real test, you would call RunSchemaTest(t, schemaTest, opts)
	// For this example, we just verify the configuration is valid
	_ = schemaTest
	_ = opts
}

func TestExampleMockERC20BridgeTesting(t *testing.T) {
	// Example of mock testing
	mockListener := MockERC20Listener([]string{"erc20_transfer"})
	erc20Config := MockERC20Bridge(mockListener)
	opts := GetTestOptionsWithERC20Bridge(erc20Config)

	// Use in tests
	_ = opts
}

func TestExampleCombinedCacheAndERC20Testing(t *testing.T) {
	// Example of using both cache and ERC-20 bridge
	cacheOpts := SimpleCache("0x123...", "stream1")
	erc20Opts := SimpleERC20Bridge("wss://rpc.com", "/key/path")
	opts := GetTestOptionsWithBoth(cacheOpts, erc20Opts)

	// Use in tests
	_ = opts
}

func TestReorganizationWorks(t *testing.T) {
	// This test verifies that the reorganized utilities compile and work

	// Test cache options
	cacheOpts := NewCacheOptions().
		WithEnabled().
		WithStream("test_provider", "test_stream", "0 * * * *")

	if !cacheOpts.IsEnabled() {
		t.Error("Cache should be enabled")
	}

	// Test ERC-20 bridge options
	erc20Opts := NewERC20BridgeConfig().
		WithRPC("test_chain", "wss://test-rpc.com").
		WithSigner("test_signer", "/test/key")

	if erc20Opts.RPC["test_chain"] != "wss://test-rpc.com" {
		t.Error("RPC URL should be set")
	}

	// Test combined options
	combinedOpts := GetTestOptionsWithBoth(cacheOpts, erc20Opts)
	if combinedOpts.Cache == nil || combinedOpts.ERC20Bridge == nil {
		t.Error("Combined options should have both cache and ERC-20 bridge")
	}
}
