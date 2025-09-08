// Package erc20 provides configuration and setup for ERC-20 bridge testing
package erc20

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/extensions/listeners"
)

// ERC20BridgeConfig provides configuration for ERC-20 bridge testing
type ERC20BridgeConfig struct {
	RPC           map[string]string
	Signers       map[string]string
	Chains        map[string]*ERC20ChainConfig
	Timeout       time.Duration
	AutoStart     bool
	MockListeners map[string]listeners.ListenFunc
}

// ERC20ChainConfig represents per-chain configuration for ERC-20 bridge testing
type ERC20ChainConfig struct {
	BlockSyncChunkSize   string
	MaxRetries           int64
	ReconnectionInterval int64
}

// NewERC20BridgeConfig creates a new ERC-20 bridge configuration
func NewERC20BridgeConfig() *ERC20BridgeConfig {
	return &ERC20BridgeConfig{
		RPC:           make(map[string]string),
		Signers:       make(map[string]string),
		Chains:        make(map[string]*ERC20ChainConfig),
		Timeout:       30 * time.Second,
		AutoStart:     false,
		MockListeners: make(map[string]listeners.ListenFunc),
	}
}

// WithRPC adds an RPC endpoint for a chain
func (c *ERC20BridgeConfig) WithRPC(chain, rpcURL string) *ERC20BridgeConfig {
	c.RPC[chain] = rpcURL
	return c
}

// WithSigner adds a signer configuration
func (c *ERC20BridgeConfig) WithSigner(name, keyPath string) *ERC20BridgeConfig {
	c.Signers[name] = keyPath
	return c
}

// WithChainConfig adds chain-specific configuration
func (c *ERC20BridgeConfig) WithChainConfig(chain string, config *ERC20ChainConfig) *ERC20BridgeConfig {
	c.Chains[chain] = config
	return c
}

// WithTimeout sets the listener timeout
func (c *ERC20BridgeConfig) WithTimeout(timeout time.Duration) *ERC20BridgeConfig {
	c.Timeout = timeout
	return c
}

// WithAutoStart enables automatic listener startup
func (c *ERC20BridgeConfig) WithAutoStart() *ERC20BridgeConfig {
	c.AutoStart = true
	return c
}

// WithMockListener adds a mock listener for testing
func (c *ERC20BridgeConfig) WithMockListener(name string, listener listeners.ListenFunc) *ERC20BridgeConfig {
	c.MockListeners[name] = listener
	return c
}

// BuildERC20BridgeConfig converts the test config to kwil-db config format
func (c *ERC20BridgeConfig) BuildERC20BridgeConfig() *config.ERC20BridgeConfig {
	erc20Config := &config.ERC20BridgeConfig{
		RPC:                c.RPC,
		BlockSyncChuckSize: make(map[string]string),
		Signer:             c.Signers,
	}

	// Add chain-specific configurations
	for chain, chainConfig := range c.Chains {
		if chainConfig.BlockSyncChunkSize != "" {
			erc20Config.BlockSyncChuckSize[chain] = chainConfig.BlockSyncChunkSize
		}
	}

	return erc20Config
}

// Validate performs basic validation on the configuration
func (c *ERC20BridgeConfig) Validate() error {
	// Validate RPC URLs
	for chain, rpcURL := range c.RPC {
		if rpcURL == "" {
			return fmt.Errorf("RPC URL for chain %s cannot be empty", chain)
		}

		// Parse URL to validate format
		u, err := url.Parse(rpcURL)
		if err != nil {
			return fmt.Errorf("invalid RPC URL for chain %s: %w", chain, err)
		}

		// Must be WebSocket protocol
		if u.Scheme != "ws" && u.Scheme != "wss" {
			return fmt.Errorf("RPC URL for chain %s must use WebSocket protocol (ws:// or wss://), got %s", chain, u.Scheme)
		}

		// Must have a host
		if u.Host == "" {
			return fmt.Errorf("RPC URL for chain %s must specify a host", chain)
		}
	}

	// Validate signer configurations
	for name, keyPath := range c.Signers {
		if keyPath == "" {
			return fmt.Errorf("signer key path for %s cannot be empty", name)
		}

		// Skip validation for mock paths
		if keyPath == "/dev/null" {
			continue
		}

		// Check if file exists
		if _, err := os.Stat(keyPath); os.IsNotExist(err) {
			return fmt.Errorf("signer key file for %s does not exist: %s", name, keyPath)
		}
	}

	// Validate timeout
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got %v", c.Timeout)
	}

	// Validate chain configurations
	for chain, chainConfig := range c.Chains {
		if chainConfig.BlockSyncChunkSize != "" {
			// Validate chunk size format (should be a number)
			if !strings.Contains(chainConfig.BlockSyncChunkSize, ".") {
				// If it's an integer, try to parse it
				if _, err := url.Parse(chainConfig.BlockSyncChunkSize); err == nil {
					continue // It's a valid URL-like string
				}
			}
		}

		if chainConfig.MaxRetries < 0 {
			return fmt.Errorf("max retries for chain %s cannot be negative, got %d", chain, chainConfig.MaxRetries)
		}

		if chainConfig.ReconnectionInterval < 0 {
			return fmt.Errorf("reconnection interval for chain %s cannot be negative, got %d", chain, chainConfig.ReconnectionInterval)
		}
	}

	return nil
}
