package tn_cache

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// TestPoolDBWrapper verifies that our wrapper correctly implements the sql.DB interface
func TestPoolDBWrapper(t *testing.T) {
	// Create a mock pool for testing
	// In a real test, you would use a test database
	t.Run("wrapper_implements_sql_DB", func(t *testing.T) {
		// This test verifies compile-time interface compliance
		var pool *pgxpool.Pool // nil is fine for interface check
		wrapper := newPoolDBWrapper(pool)

		// Verify it implements sql.DB
		var _ sql.DB = wrapper
		assert.NotNil(t, wrapper)
	})

	t.Run("wrapper_implements_required_methods", func(t *testing.T) {
		var pool *pgxpool.Pool
		wrapper := newPoolDBWrapper(pool)

		// Check that wrapper has the required methods
		_, ok := wrapper.(sql.Executor)
		assert.True(t, ok, "wrapper should implement sql.Executor")

		_, ok = wrapper.(sql.TxMaker)
		assert.True(t, ok, "wrapper should implement sql.TxMaker")
	})
}

// TestCacheSchedulerWrappedDB verifies the scheduler returns a proper wrapped DB
func TestCacheSchedulerWrappedDB(t *testing.T) {
	// This test would require a more complete test setup
	// For now, it verifies the method exists and returns the correct type
	t.Run("getWrappedDB_returns_sql_DB", func(t *testing.T) {
		// Create a minimal scheduler for testing
		scheduler := &CacheScheduler{
			logger: log.DiscardLogger,
			// cacheDB is nil, so getWrappedDB should handle gracefully
		}

		// With nil cacheDB, it should return nil
		db := scheduler.getWrappedDB()
		assert.Nil(t, db)
	})
}
