package internal

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	kwilsql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/tests/utils"
)

// ExtendedMockDB extends the existing MockDB with missing methods
type ExtendedMockDB struct {
	*utils.MockDB
	QueryScanFnFn func(ctx context.Context, stmt string, scans []any, fn func() error, args ...any) error
	AccessModeFn  func() kwilsql.AccessMode
}

func (m *ExtendedMockDB) QueryScanFn(ctx context.Context, stmt string, scans []any, fn func() error, args ...any) error {
	if m.QueryScanFnFn != nil {
		return m.QueryScanFnFn(ctx, stmt, scans, fn, args...)
	}
	return nil
}

func (m *ExtendedMockDB) AccessMode() kwilsql.AccessMode {
	if m.AccessModeFn != nil {
		return m.AccessModeFn()
	}
	return kwilsql.ReadWrite
}

func newTestDB() *ExtendedMockDB {
	return &ExtendedMockDB{
		MockDB: &utils.MockDB{},
	}
}

// TestCacheDB_GetStreamConfig removed - basic CRUD mocking provides little value over integration tests

// TestCacheDB_CacheEvents removed - trivial insert mocking provides little value over integration tests

// TestCacheDB_GetEvents removed - simple select mocking provides little value over integration tests

// TestCacheDB_ListStreamConfigs removed - basic list mocking provides little value over integration tests

func TestCacheDB_HasCachedData(t *testing.T) {
	// Create mock DB
	mockDB := newTestDB()

	// Set up results for the sequence of queries
	callCount := 0
	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		callCount++
		if callCount == 1 {
			// First query: check if stream is configured
			return &kwilsql.ResultSet{
				Columns: []string{"exists"},
				Rows:    [][]interface{}{{true}},
			}, nil
		}
		// Second query: check if events exist
		return &kwilsql.ResultSet{
			Columns: []string{"count"},
			Rows:    [][]interface{}{{int64(5)}},
		}, nil
	}

	// Set up mock transaction
	mockDB.BeginTxFn = func(ctx context.Context) (kwilsql.Tx, error) {
		return &utils.MockTx{
			ExecuteFn: mockDB.ExecuteFn,
		}, nil
	}

	// Create CacheDB with mock DB
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	// Test HasCachedData
	hasData, err := cacheDB.HasCachedData(context.Background(), "test_provider", "test_stream", 1234567890, 1234567900)
	require.NoError(t, err)
	assert.True(t, hasData)
}

func TestCacheDB_UpdateStreamConfigsAtomic(t *testing.T) {
	// Create mock DB
	mockDB := newTestDB()

	newConfigs := []StreamCacheConfig{
		{
			DataProvider:  "provider1",
			StreamID:      "stream1",
			FromTimestamp: 1234567890,
			LastRefreshed: 1672531200, // 2023-01-01T00:00:00Z
			CronSchedule:  "0 0 * * * *",
		},
	}

	toDelete := []StreamCacheConfig{
		{
			DataProvider: "provider2",
			StreamID:     "stream2",
		},
	}

	// Create CacheDB with mock DB
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	// Test UpdateStreamConfigsAtomic
	err := cacheDB.UpdateStreamConfigsAtomic(context.Background(), newConfigs, toDelete)
	require.NoError(t, err)
}
