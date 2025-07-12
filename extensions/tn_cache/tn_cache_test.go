package tn_cache

import (
	"context"
	"io"
	"testing"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
)

// createTestLogger creates a logger suitable for testing
func createTestLogger(t *testing.T) log.Logger {
	return log.New(log.WithWriter(io.Discard))
}

// mockService implements a minimal Service for testing
type mockService struct {
	logger log.Logger
}

func (m *mockService) Logger() log.Logger {
	return m.logger
}

// createTestEngineContext creates a proper EngineContext for testing
func createTestEngineContext() *common.EngineContext {
	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: context.Background(),
			BlockContext: &common.BlockContext{
				Height: 1,
				ChainContext: &common.ChainContext{
					NetworkParameters: &common.NetworkParameters{},
					MigrationParams:   &common.MigrationContext{},
				},
			},
			Caller:        "test_caller",
			Signer:        []byte("test_caller"),
			Authenticator: "test_authenticator",
		},
	}
}