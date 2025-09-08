// Package service provides helpers for creating common.Service instances for testing
package service

import (
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"
)

// CreateBridgeService creates a service with ERC-20 bridge configuration
func CreateBridgeService(bridgeConfig *config.ERC20BridgeConfig) (*common.Service, error) {
	// Create full config
	fullConfig := &config.Config{
		Erc20Bridge: *bridgeConfig,
	}

	// Create service
	service := &common.Service{
		Logger:      log.NewStdoutLogger(),
		LocalConfig: fullConfig,
		Identity:    []byte("test-erc20-node"),
	}

	return service, nil
}

// CreateServiceWithLocalConfig creates a service with custom local configuration
func CreateServiceWithLocalConfig(localConfig *config.Config) *common.Service {
	return &common.Service{
		Logger:      log.NewStdoutLogger(),
		LocalConfig: localConfig,
		Identity:    []byte("test-node"),
	}
}

// CreateDefaultService creates a basic service for testing
func CreateDefaultService() *common.Service {
	return &common.Service{
		Logger:      log.NewStdoutLogger(),
		LocalConfig: config.DefaultConfig(),
		Identity:    []byte("test-node"),
	}
}
