// Package erc20 provides helper functions for ERC-20 bridge testing
package erc20

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/extensions/listeners"
	_ "github.com/trufnetwork/kwil-db/node/exts/erc20-bridge/erc20"
	_ "github.com/trufnetwork/kwil-db/node/exts/evm-sync"
	_ "github.com/trufnetwork/kwil-db/node/exts/ordered-sync"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/tests/streams/utils/eventstore"
	"github.com/trufnetwork/node/tests/streams/utils/service"
)

// ERC20BridgeTestHelper provides utilities for ERC-20 bridge testing
type ERC20BridgeTestHelper struct {
	config       *ERC20BridgeConfig
	platform     *kwilTesting.Platform
	eventStore   listeners.EventStore
	service      *common.Service
	listenerCtx  context.Context
	cancel       context.CancelFunc
	listenerDone chan error
	running      bool
}

// SetupERC20BridgeTest sets up ERC-20 bridge testing environment
func SetupERC20BridgeTest(ctx context.Context, platform *kwilTesting.Platform, config *ERC20BridgeConfig) (*ERC20BridgeTestHelper, error) {
	helper := &ERC20BridgeTestHelper{
		config:       config,
		platform:     platform,
		listenerDone: make(chan error, 1),
	}

	// Create event store
	helper.eventStore = eventstore.NewMockEventStore()

	// Create service with ERC-20 bridge configuration
	bridgeConfig := config.BuildERC20BridgeConfig()
	svc, err := service.CreateBridgeService(bridgeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create service: %w", err)
	}
	helper.service = svc

	return helper, nil
}

// StartERC20Listener manually starts the ERC-20 bridge listener
func (h *ERC20BridgeTestHelper) StartERC20Listener(ctx context.Context) error {
	if h.running {
		return fmt.Errorf("listener already running")
	}

	// Prefer mock listener if provided, otherwise fallback to registered listener
	var listenerFunc listeners.ListenFunc
	if mockFunc, hasMock := h.config.MockListeners["evm_sync"]; hasMock {
		listenerFunc = mockFunc
	} else {
		lf, exists := listeners.GetListener("evm_sync")
		if !exists {
			return fmt.Errorf("evm_sync listener not registered - ensure kwil-db node/exts/evm-sync is imported")
		}
		listenerFunc = lf
	}

	// Create context with timeout
	h.listenerCtx, h.cancel = context.WithTimeout(ctx, h.config.Timeout)

	// Start listener in background
	h.running = true
	go func() {
		defer func() { h.running = false }()
		err := listenerFunc(h.listenerCtx, h.service, h.eventStore)
		h.listenerDone <- err
	}()

	return nil
}

// StopERC20Listener stops the running listener
func (h *ERC20BridgeTestHelper) StopERC20Listener() error {
	if !h.running {
		return nil
	}

	if h.cancel != nil {
		h.cancel()
	}

	// Wait for listener to stop
	select {
	case err := <-h.listenerDone:
		h.running = false
		// Treat graceful cancellations as successful shutdowns
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil
		}
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("listener did not stop within timeout")
	}
}

// Cleanup cleans up test resources
func (h *ERC20BridgeTestHelper) Cleanup() error {
	return h.StopERC20Listener()
}

// IsListenerRunning returns whether the listener is currently running
func (h *ERC20BridgeTestHelper) IsListenerRunning() bool {
	return h.running
}

// GetService returns the service instance for advanced testing
func (h *ERC20BridgeTestHelper) GetService() *common.Service {
	return h.service
}

// GetEventStore returns the event store for advanced testing
func (h *ERC20BridgeTestHelper) GetEventStore() listeners.EventStore {
	return h.eventStore
}

// WaitForListenerEvent waits for a specific event to be broadcast
func (h *ERC20BridgeTestHelper) WaitForListenerEvent(eventType string, timeout time.Duration) error {
	if mockStore, ok := h.eventStore.(*eventstore.MockEventStore); ok {
		return mockStore.WaitForEvent(eventType, timeout)
	}
	return fmt.Errorf("event store does not support event waiting")
}

// WithERC20Bridge is a test wrapper that sets up and tears down ERC-20 bridge testing
func WithERC20Bridge(t TestingT, ctx context.Context, platform *kwilTesting.Platform, config *ERC20BridgeConfig, testFunc func(*ERC20BridgeTestHelper)) {
	helper, err := SetupERC20BridgeTest(ctx, platform, config)
	if err != nil {
		t.Fatalf("Failed to setup ERC-20 bridge: %v", err)
	}
	defer func() {
		if err := helper.Cleanup(); err != nil {
			t.Errorf("Failed to cleanup ERC-20 bridge: %v", err)
		}
	}()

	// Auto-start listener if configured
	if config.AutoStart {
		if err := helper.StartERC20Listener(ctx); err != nil {
			t.Fatalf("Failed to start ERC-20 listener: %v", err)
		}
	}

	testFunc(helper)
}

// WithERC20BridgeAndListener combines setup and auto-start
func WithERC20BridgeAndListener(t TestingT, ctx context.Context, platform *kwilTesting.Platform, config *ERC20BridgeConfig, testFunc func(*ERC20BridgeTestHelper)) {
	config.AutoStart = true
	WithERC20Bridge(t, ctx, platform, config, testFunc)
}

// TestingT interface for test functions
type TestingT interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}
