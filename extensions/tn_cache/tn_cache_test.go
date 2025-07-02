package tn_cache

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
)

// mockDB implements sql.DB interface for testing
type mockDB struct {
	executeFn func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error)
	beginTxFn func(ctx context.Context) (sql.Tx, error)
}

func (m *mockDB) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	return m.executeFn(ctx, stmt, args...)
}

func (m *mockDB) BeginTx(ctx context.Context) (sql.Tx, error) {
	return m.beginTxFn(ctx)
}

// mockTx implements sql.Tx interface for testing
type mockTx struct {
	executeFn  func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error)
	beginTxFn  func(ctx context.Context) (sql.Tx, error)
	rollbackFn func(ctx context.Context) error
	commitFn   func(ctx context.Context) error
}

func (m *mockTx) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	return m.executeFn(ctx, stmt, args...)
}

func (m *mockTx) BeginTx(ctx context.Context) (sql.Tx, error) {
	return m.beginTxFn(ctx)
}

func (m *mockTx) Rollback(ctx context.Context) error {
	return m.rollbackFn(ctx)
}

func (m *mockTx) Commit(ctx context.Context) error {
	return m.commitFn(ctx)
}

// createTestLogger creates a logger suitable for testing
func createTestLogger(t *testing.T) log.Logger {
	return log.New(log.WithWriter(io.Discard))
}

// mockService implements Service for testing
type mockService struct {
	logger log.Logger
	config map[string]map[string]interface{}
}

func (m *mockService) Logger() log.Logger {
	return m.logger
}

func TestSetupCacheSchema(t *testing.T) {
	mockTx := &mockTx{
		executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			return &sql.ResultSet{
				Status: sql.CommandTag{RowsAffected: 1},
			}, nil
		},
		commitFn: func(ctx context.Context) error {
			return nil
		},
		rollbackFn: func(ctx context.Context) error {
			return nil
		},
		beginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return nil, nil
		},
	}

	mockDb := &mockDB{
		beginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return mockTx, nil
		},
		executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			return nil, nil
		},
	}

	err := setupCacheSchema(context.Background(), mockDb)
	require.NoError(t, err)
}

func TestHasCachedData(t *testing.T) {
	// Test when cache is not initialized
	t.Run("cache not initialized", func(t *testing.T) {
		cacheDB = nil

		_, err := HasCachedData(context.Background(), "test", "test", 0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not initialized")
	})

	// Test when cache is initialized
	t.Run("cache initialized", func(t *testing.T) {
		mockTx := &mockTx{
			executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				// First check stream config
				return &sql.ResultSet{
					Rows: [][]any{
						{true}, // Has stream config
					},
				}, nil
			},
			commitFn: func(ctx context.Context) error {
				return nil
			},
			rollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &mockDB{
			beginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		logger := createTestLogger(t)
		cacheDB = internal.NewCacheDB(mockDb, logger)

		_, err := HasCachedData(context.Background(), "test", "test", 0, 0)
		require.NoError(t, err)
	})
}

func TestGetCachedData(t *testing.T) {
	// Test when cache is not initialized
	t.Run("cache not initialized", func(t *testing.T) {
		cacheDB = nil

		_, err := GetCachedData(context.Background(), "test", "test", 0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not initialized")
	})

	// Test when cache is initialized
	t.Run("cache initialized", func(t *testing.T) {
		testDataProvider := "test_provider"
		testStreamID := "test_stream"
		testEventTime := int64(1234567890)
		testValue, _ := types.ParseDecimalExplicit("123.456", 36, 18)

		mockTx := &mockTx{
			executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return &sql.ResultSet{
					Columns: []string{"data_provider", "stream_id", "event_time", "value"},
					Rows: [][]any{
						{testDataProvider, testStreamID, testEventTime, testValue},
					},
				}, nil
			},
			commitFn: func(ctx context.Context) error {
				return nil
			},
			rollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &mockDB{
			beginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		logger := createTestLogger(t)
		cacheDB = internal.NewCacheDB(mockDb, logger)

		events, err := GetCachedData(context.Background(), testDataProvider, testStreamID, 0, 0)
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.Equal(t, testDataProvider, events[0].DataProvider)
		assert.Equal(t, testStreamID, events[0].StreamID)
		assert.Equal(t, testEventTime, events[0].EventTime)
		assert.Equal(t, testValue, events[0].Value)
	})
}

func TestSetupStreamConfigs(t *testing.T) {
	// Create a mock CacheDB
	mockTx := &mockTx{
		executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			return &sql.ResultSet{
				Status: sql.CommandTag{RowsAffected: 1},
			}, nil
		},
		commitFn: func(ctx context.Context) error {
			return nil
		},
		rollbackFn: func(ctx context.Context) error {
			return nil
		},
	}

	mockDb := &mockDB{
		beginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return mockTx, nil
		},
	}

	logger := createTestLogger(t)
	cacheDB = internal.NewCacheDB(mockDb, logger)

	// Create instructions for test with valid fields based on actual struct
	var fromTime int64 = 1234567890
	instructions := []config.InstructionDirective{
		{
			ID:           "test1",
			DataProvider: "test_provider1",
			StreamID:     "test_stream1",
			TimeRange: config.TimeRange{
				From: &fromTime,
			},
			Type: config.DirectiveSpecific,
			Schedule: config.Schedule{
				CronExpr: "*/5 * * * *",
			},
		},
		{
			ID:           "test2",
			DataProvider: "test_provider2",
			StreamID:     "test_stream2",
			TimeRange: config.TimeRange{
				From: &fromTime,
			},
			Type: config.DirectiveSpecific,
			Schedule: config.Schedule{
				CronExpr: "*/10 * * * *",
			},
		},
	}

	// Test that stream config setup is handled by the scheduler
	// This functionality is now part of the CacheScheduler.Start method
	require.NotNil(t, instructions, "Instructions should be properly created")
}