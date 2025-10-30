package internal

import (
	"context"
	"fmt"
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
			// First query: return refreshed stream config row
			return &kwilsql.ResultSet{
				Columns: []string{"cache_refreshed_at_timestamp"},
				Rows:    [][]interface{}{{int64(1)}},
			}, nil
		}
		// Second query: check if events exist
		return &kwilsql.ResultSet{
			Columns: []string{"exists"},
			Rows:    [][]interface{}{{true}},
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
	hasData, err := cacheDB.HasCachedData(context.Background(), "test_provider", "test_stream", nil, 1234567890, 1234567900)
	require.NoError(t, err)
	assert.True(t, hasData)
}

func TestCacheDB_HasCachedData_BaseTimeMatch(t *testing.T) {
	mockDB := newTestDB()
	baseTime := int64(42)
	encoded := encodeBaseTime(&baseTime)
	callCount := 0

	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		callCount++
		require.GreaterOrEqual(t, len(args), 4)
		if callCount == 1 {
			require.Equal(t, encoded, args[3].(int64))
			return &kwilsql.ResultSet{Columns: []string{"cache_refreshed_at_timestamp"}, Rows: [][]interface{}{{int64(10)}}}, nil
		}
		require.Equal(t, encoded, args[2].(int64))
		return &kwilsql.ResultSet{Columns: []string{"exists"}, Rows: [][]interface{}{{true}}}, nil
	}

	mockDB.BeginTxFn = func(ctx context.Context) (kwilsql.Tx, error) {
		return &utils.MockTx{ExecuteFn: mockDB.ExecuteFn}, nil
	}

	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	hasData, err := cacheDB.HasCachedData(context.Background(), "provider", "stream", &baseTime, 0, 0)
	require.NoError(t, err)
	assert.True(t, hasData)
}

func TestCacheDB_HasCachedData_BaseTimeMismatch(t *testing.T) {
	mockDB := newTestDB()
	callCount := 0

	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		callCount++
		if callCount == 1 {
			return &kwilsql.ResultSet{Columns: []string{"cache_refreshed_at_timestamp"}, Rows: [][]interface{}{}}, nil
		}
		return nil, fmt.Errorf("unexpected additional query")
	}

	mockDB.BeginTxFn = func(ctx context.Context) (kwilsql.Tx, error) {
		return &utils.MockTx{ExecuteFn: mockDB.ExecuteFn}, nil
	}

	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	hasData, err := cacheDB.HasCachedData(context.Background(), "provider", "stream", nil, 0, 0)
	require.NoError(t, err)
	assert.False(t, hasData)
}

func TestCacheDB_HasCachedData_RefreshedButEmpty(t *testing.T) {
	mockDB := newTestDB()
	baseTime := int64(50)
	callCount := 0

	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		callCount++
		switch callCount {
		case 1:
			require.Len(t, args, 4)
			require.Equal(t, baseTime, args[3].(int64))
			return &kwilsql.ResultSet{Columns: []string{"cache_refreshed_at_timestamp"}, Rows: [][]interface{}{{int64(100)}}}, nil
		case 2:
			return &kwilsql.ResultSet{Columns: []string{"exists"}, Rows: [][]interface{}{{false}}}, nil
		case 3:
			return &kwilsql.ResultSet{Columns: []string{"exists"}, Rows: [][]interface{}{{false}}}, nil
		default:
			return nil, fmt.Errorf("unexpected query")
		}
	}

	mockDB.BeginTxFn = func(ctx context.Context) (kwilsql.Tx, error) {
		return &utils.MockTx{ExecuteFn: mockDB.ExecuteFn}, nil
	}

	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	hasData, err := cacheDB.HasCachedData(context.Background(), "provider", "stream", &baseTime, 0, 0)
	require.NoError(t, err)
	require.True(t, hasData, "refreshed shard with no events should count as cached")
}

func TestCacheDB_HasCachedData_SentinelRefreshedButEmpty(t *testing.T) {
	mockDB := newTestDB()
	callCount := 0

	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		callCount++
		switch callCount {
		case 1:
			return &kwilsql.ResultSet{Columns: []string{"cache_refreshed_at_timestamp"}, Rows: [][]interface{}{{int64(100)}}}, nil
		case 2:
			return &kwilsql.ResultSet{Columns: []string{"exists"}, Rows: [][]interface{}{{false}}}, nil
		case 3:
			return &kwilsql.ResultSet{Columns: []string{"exists"}, Rows: [][]interface{}{{false}}}, nil
		default:
			return nil, fmt.Errorf("unexpected query")
		}
	}

	mockDB.BeginTxFn = func(ctx context.Context) (kwilsql.Tx, error) {
		return &utils.MockTx{ExecuteFn: mockDB.ExecuteFn}, nil
	}

	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	hasData, err := cacheDB.HasCachedData(context.Background(), "provider", "stream", nil, 0, 0)
	require.NoError(t, err)
	require.False(t, hasData, "sentinel shard without data should not count as cached")
}

func TestCacheDB_HasCachedIndexData(t *testing.T) {
	mockDB := newTestDB()
	callCount := 0
	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		callCount++
		switch callCount {
		case 1:
			return &kwilsql.ResultSet{Columns: []string{"cache_refreshed_at_timestamp"}, Rows: [][]interface{}{{int64(100)}}}, nil
		case 2:
			return &kwilsql.ResultSet{Columns: []string{"exists"}, Rows: [][]interface{}{{true}}}, nil
		default:
			return nil, fmt.Errorf("unexpected query")
		}
	}
	mockDB.BeginTxFn = func(ctx context.Context) (kwilsql.Tx, error) {
		return &utils.MockTx{ExecuteFn: mockDB.ExecuteFn}, nil
	}
	cacheDB := NewCacheDB(mockDB, log.New(log.WithWriter(io.Discard)))
	hasData, err := cacheDB.HasCachedIndexData(context.Background(), "p", "s", nil, 0, 0)
	require.NoError(t, err)
	require.True(t, hasData)
}

func TestCacheDB_HasCachedIndexData_Empty(t *testing.T) {
	mockDB := newTestDB()
	callCount := 0
	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		callCount++
		switch callCount {
		case 1:
			return &kwilsql.ResultSet{Columns: []string{"cache_refreshed_at_timestamp"}, Rows: [][]interface{}{{int64(100)}}}, nil
		case 2:
			return &kwilsql.ResultSet{Columns: []string{"exists"}, Rows: [][]interface{}{{false}}}, nil
		case 3:
			return &kwilsql.ResultSet{Columns: []string{"exists"}, Rows: [][]interface{}{{false}}}, nil
		default:
			return nil, fmt.Errorf("unexpected query")
		}
	}
	mockDB.BeginTxFn = func(ctx context.Context) (kwilsql.Tx, error) {
		return &utils.MockTx{ExecuteFn: mockDB.ExecuteFn}, nil
	}
	cacheDB := NewCacheDB(mockDB, log.New(log.WithWriter(io.Discard)))
	hasData, err := cacheDB.HasCachedIndexData(context.Background(), "p", "s", nil, 0, 0)
	require.NoError(t, err)
	require.False(t, hasData)
}

func TestCacheDB_UpdateStreamConfigsAtomic(t *testing.T) {
	// Create mock DB
	mockDB := newTestDB()

	newConfigs := []StreamCacheConfig{
		{
			DataProvider:              "provider1",
			StreamID:                  "stream1",
			FromTimestamp:             1234567890,
			CacheRefreshedAtTimestamp: 1672531200, // 2023-01-01T00:00:00Z
			CacheHeight:               100,
			CronSchedule:              "0 * * * *",
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
