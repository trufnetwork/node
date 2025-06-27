package testutils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

// withTx is a higher-order function that wraps a sub-test in its own database transaction.
// It creates a new transaction from the parent platform's DB connection, creates a new
// platform scoped to that transaction, and automatically rolls back the transaction
// when the sub-test completes. This provides perfect test isolation for sub-tests
// without needing manual cleanup code.
func WithTx(platform *kwilTesting.Platform, testFn func(t *testing.T, txPlatform *kwilTesting.Platform)) func(*testing.T) {
	return func(t *testing.T) {
		tx, err := platform.DB.BeginTx(context.Background())
		require.NoError(t, err)
		defer tx.Rollback(context.Background()) // Automatically cleans up all changes

		txPlatform := &kwilTesting.Platform{
			Engine:   platform.Engine,
			DB:       tx, // The new platform uses the transaction-specific DB connection
			Deployer: platform.Deployer,
			Logger:   platform.Logger,
		}

		testFn(t, txPlatform)
	}
}
