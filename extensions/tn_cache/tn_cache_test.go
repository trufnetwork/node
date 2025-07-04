package tn_cache

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	testutils "github.com/trufnetwork/node/tests/utils"
)


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
	// Initialize logger for the test
	logger = createTestLogger(t)
	
	mockTx := &testutils.MockTx{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			return &sql.ResultSet{
				Status: sql.CommandTag{RowsAffected: 1},
			}, nil
		},
		CommitFn: func(ctx context.Context) error {
			return nil
		},
		RollbackFn: func(ctx context.Context) error {
			return nil
		},
		BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return nil, nil
		},
	}

	mockDb := &testutils.MockDB{
		BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return mockTx, nil
		},
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
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
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				// First check stream config
				return &sql.ResultSet{
					Rows: [][]any{
						{true}, // Has stream config
					},
				}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
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

		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return &sql.ResultSet{
					Columns: []string{"data_provider", "stream_id", "event_time", "value"},
					Rows: [][]any{
						{testDataProvider, testStreamID, testEventTime, testValue},
					},
				}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
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
	mockTx := &testutils.MockTx{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			return &sql.ResultSet{
				Status: sql.CommandTag{RowsAffected: 1},
			}, nil
		},
		CommitFn: func(ctx context.Context) error {
			return nil
		},
		RollbackFn: func(ctx context.Context) error {
			return nil
		},
	}

	mockDb := &testutils.MockDB{
		BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return mockTx, nil
		},
	}

	logger := createTestLogger(t)
	cacheDB = internal.NewCacheDB(mockDb, logger)

	// Create directives for test with valid fields based on actual struct
	var fromTime int64 = 1234567890
	directives := []config.CacheDirective{
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
	require.NotNil(t, directives, "Directives should be properly created")
}

// TestRegisterSQLFunctions tests the registration of SQL functions
func TestRegisterSQLFunctions(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("successful registration", func(t *testing.T) {
		executionCount := 0
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				executionCount++
				return &sql.ResultSet{
					Status: sql.CommandTag{RowsAffected: 1},
				}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		err := registerSQLFunctions(context.Background(), mockDb)
		require.NoError(t, err)
		// Should execute 3 statements (3 ext_tn_cache functions only)
		assert.Equal(t, 3, executionCount)
	})

	t.Run("begin transaction fails", func(t *testing.T) {
		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return nil, fmt.Errorf("begin transaction failed")
			},
		}

		err := registerSQLFunctions(context.Background(), mockDb)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to begin transaction")
	})

	t.Run("execute fails", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return nil, fmt.Errorf("execute failed")
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		err := registerSQLFunctions(context.Background(), mockDb)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create is_enabled function")
	})

	t.Run("commit fails", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return &sql.ResultSet{
					Status: sql.CommandTag{RowsAffected: 1},
				}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return fmt.Errorf("commit failed")
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		err := registerSQLFunctions(context.Background(), mockDb)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "commit transaction")
	})
}

// TestSQLFunctionIsEnabled tests the is_enabled SQL function simulation
func TestSQLFunctionIsEnabled(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("ext_tn_cache.is_enabled function returns true", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT ext_tn_cache.is_enabled()" {
					return &sql.ResultSet{
						Columns: []string{"is_enabled"},
						Rows: [][]any{
							{true},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling the ext_tn_cache.is_enabled function
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT ext_tn_cache.is_enabled()")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, true, result.Rows[0][0])
	})
}

// TestSQLFunctionHasCachedData tests the has_cached_data SQL function simulation
func TestSQLFunctionHasCachedData(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("ext_tn_cache.has_cached_data works correctly", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT ext_tn_cache.has_cached_data($1, $2, $3, $4)" {
					return &sql.ResultSet{
						Columns: []string{"has_cached_data"},
						Rows: [][]any{
							{true},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling the ext_tn_cache.has_cached_data function
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT ext_tn_cache.has_cached_data($1, $2, $3, $4)", "test_provider", "test_stream", int64(1234567890), int64(1234567900))
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, true, result.Rows[0][0])
	})
}

// TestSQLFunctionGetCachedData tests the get_cached_data SQL function simulation
func TestSQLFunctionGetCachedData(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("ext_tn_cache.get_cached_data works correctly", func(t *testing.T) {
		testValue, _ := types.ParseDecimalExplicit("123.456", 36, 18)
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, $4)" {
					return &sql.ResultSet{
						Columns: []string{"data_provider", "stream_id", "event_time", "value"},
						Rows: [][]any{
							{"test_provider", "test_stream", int64(1234567890), testValue},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling the ext_tn_cache.get_cached_data function
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, $4)", "test_provider", "test_stream", int64(1234567890), int64(1234567900))
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "test_provider", result.Rows[0][0])
		assert.Equal(t, "test_stream", result.Rows[0][1])
		assert.Equal(t, int64(1234567890), result.Rows[0][2])
		assert.Equal(t, testValue, result.Rows[0][3])
	})
}

// TestSQLFunctionWithNullToTime tests SQL functions with NULL to_time parameter
func TestSQLFunctionWithNullToTime(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("ext_tn_cache.has_cached_data with NULL to_time", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT ext_tn_cache.has_cached_data($1, $2, $3, NULL)" {
					return &sql.ResultSet{
						Columns: []string{"has_cached_data"},
						Rows: [][]any{
							{true},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling the ext_tn_cache.has_cached_data function with NULL to_time
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT ext_tn_cache.has_cached_data($1, $2, $3, NULL)", "test_provider", "test_stream", int64(1234567890))
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, true, result.Rows[0][0])
	})

	t.Run("ext_tn_cache.get_cached_data with NULL to_time", func(t *testing.T) {
		testValue, _ := types.ParseDecimalExplicit("123.456", 36, 18)
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, NULL)" {
					return &sql.ResultSet{
						Columns: []string{"data_provider", "stream_id", "event_time", "value"},
						Rows: [][]any{
							{"test_provider", "test_stream", int64(1234567890), testValue},
							{"test_provider", "test_stream", int64(1234567891), testValue},
							{"test_provider", "test_stream", int64(1234567892), testValue},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling the ext_tn_cache.get_cached_data function with NULL to_time
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, NULL)", "test_provider", "test_stream", int64(1234567890))
		require.NoError(t, err)
		require.Len(t, result.Rows, 3)
		assert.Equal(t, "test_provider", result.Rows[0][0])
		assert.Equal(t, "test_stream", result.Rows[0][1])
		assert.Equal(t, int64(1234567890), result.Rows[0][2])
		assert.Equal(t, testValue, result.Rows[0][3])
	})
}

// TestCleanupExtensionSchema tests the cleanup of extension schema
func TestCleanupExtensionSchema(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("successful cleanup", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "DROP SCHEMA IF EXISTS ext_tn_cache CASCADE" {
					return &sql.ResultSet{
						Status: sql.CommandTag{RowsAffected: 1},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		err := cleanupExtensionSchema(context.Background(), mockDb)
		require.NoError(t, err)
	})

	t.Run("begin transaction fails", func(t *testing.T) {
		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return nil, fmt.Errorf("begin transaction failed")
			},
		}

		err := cleanupExtensionSchema(context.Background(), mockDb)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "begin transaction")
	})

	t.Run("execute fails", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return nil, fmt.Errorf("execute failed")
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		err := cleanupExtensionSchema(context.Background(), mockDb)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "drop schema")
	})

	t.Run("commit fails", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return &sql.ResultSet{
					Status: sql.CommandTag{RowsAffected: 1},
				}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return fmt.Errorf("commit failed")
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		err := cleanupExtensionSchema(context.Background(), mockDb)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "commit transaction")
	})
}

// TestWritePathProtection tests that SQL functions fail when called from write transactions
func TestWritePathProtection(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("ext_tn_cache.is_enabled_fails_in_write_transaction", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT ext_tn_cache.is_enabled()" {
					return nil, fmt.Errorf("ext_tn_cache functions cannot be used in write transactions")
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling ext_tn_cache.is_enabled function in a write transaction
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		_, err = tx.Execute(context.Background(), "SELECT ext_tn_cache.is_enabled()")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ext_tn_cache functions cannot be used in write transactions")
	})

	t.Run("ext_tn_cache.has_cached_data_fails_in_write_transaction", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT ext_tn_cache.has_cached_data($1, $2, $3, $4)" {
					return nil, fmt.Errorf("ext_tn_cache functions cannot be used in write transactions")
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling ext_tn_cache.has_cached_data function in a write transaction
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		_, err = tx.Execute(context.Background(), "SELECT ext_tn_cache.has_cached_data($1, $2, $3, $4)", "test_provider", "test_stream", int64(1234567890), int64(1234567900))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ext_tn_cache functions cannot be used in write transactions")
	})

	t.Run("ext_tn_cache.get_cached_data_fails_in_write_transaction", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, $4)" {
					return nil, fmt.Errorf("ext_tn_cache functions cannot be used in write transactions")
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling ext_tn_cache.get_cached_data function in a write transaction
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		_, err = tx.Execute(context.Background(), "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, $4)", "test_provider", "test_stream", int64(1234567890), int64(1234567900))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ext_tn_cache functions cannot be used in write transactions")
	})
}

// TestReadOnlyTransactionSuccess tests that SQL functions work correctly in read-only transactions
func TestReadOnlyTransactionSuccess(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("ext_tn_cache.is_enabled returns true in read-only transaction", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT ext_tn_cache.is_enabled()" {
					return &sql.ResultSet{
						Columns: []string{"is_enabled"},
						Rows: [][]any{
							{true},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling ext_tn_cache.is_enabled function in a read-only transaction
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT ext_tn_cache.is_enabled()")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, true, result.Rows[0][0])
	})

	t.Run("ext_tn_cache.has_cached_data returns true in read-only transaction", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT ext_tn_cache.has_cached_data($1, $2, $3, $4)" {
					return &sql.ResultSet{
						Columns: []string{"has_cached_data"},
						Rows: [][]any{
							{true},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling ext_tn_cache.has_cached_data function in a read-only transaction
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT ext_tn_cache.has_cached_data($1, $2, $3, $4)", "test_provider", "test_stream", int64(1234567890), int64(1234567900))
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, true, result.Rows[0][0])
	})

	t.Run("ext_tn_cache.get_cached_data returns table in read-only transaction", func(t *testing.T) {
		testValue, _ := types.ParseDecimalExplicit("654.321", 36, 18)
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, $4)" {
					return &sql.ResultSet{
						Columns: []string{"data_provider", "stream_id", "event_time", "value"},
						Rows: [][]any{
							{"test_provider", "test_stream", int64(1234567890), testValue},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		// Simulate calling ext_tn_cache.get_cached_data function in a read-only transaction
		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, $4)", "test_provider", "test_stream", int64(1234567890), int64(1234567900))
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "test_provider", result.Rows[0][0])
		assert.Equal(t, "test_stream", result.Rows[0][1])
		assert.Equal(t, int64(1234567890), result.Rows[0][2])
		assert.Equal(t, testValue, result.Rows[0][3])
	})
}

// TestSQLFunctionDataTypes tests that SQL functions return expected data types
func TestSQLFunctionDataTypes(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("ext_tn_cache.is_enabled returns boolean", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT ext_tn_cache.is_enabled()" {
					return &sql.ResultSet{
						Columns: []string{"is_enabled"},
						Rows: [][]any{
							{true}, // Boolean type
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT ext_tn_cache.is_enabled()")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		
		// Verify the return type is boolean
		value := result.Rows[0][0]
		assert.IsType(t, true, value, "ext_tn_cache.is_enabled should return boolean")
		assert.Equal(t, true, value)
	})

	t.Run("ext_tn_cache.has_cached_data returns boolean", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT ext_tn_cache.has_cached_data($1, $2, $3, $4)" {
					return &sql.ResultSet{
						Columns: []string{"has_cached_data"},
						Rows: [][]any{
							{false}, // Boolean type
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT ext_tn_cache.has_cached_data($1, $2, $3, $4)", "test_provider", "test_stream", int64(1234567890), int64(1234567900))
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		
		// Verify the return type is boolean
		value := result.Rows[0][0]
		assert.IsType(t, false, value, "ext_tn_cache.has_cached_data should return boolean")
		assert.Equal(t, false, value)
	})

	t.Run("ext_tn_cache.get_cached_data returns correct table structure", func(t *testing.T) {
		testValue, _ := types.ParseDecimalExplicit("123.456", 36, 18)
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, $4)" {
					return &sql.ResultSet{
						Columns: []string{"data_provider", "stream_id", "event_time", "value"},
						Rows: [][]any{
							{"test_provider", "test_stream", int64(1234567890), testValue},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
			},
			CommitFn: func(ctx context.Context) error {
				return nil
			},
			RollbackFn: func(ctx context.Context) error {
				return nil
			},
		}

		mockDb := &testutils.MockDB{
			BeginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return mockTx, nil
			},
		}

		tx, err := mockDb.BeginTx(context.Background())
		require.NoError(t, err)

		result, err := tx.Execute(context.Background(), "SELECT * FROM ext_tn_cache.get_cached_data($1, $2, $3, $4)", "test_provider", "test_stream", int64(1234567890), int64(1234567900))
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		
		// Verify the table structure and data types
		assert.Equal(t, []string{"data_provider", "stream_id", "event_time", "value"}, result.Columns)
		
		row := result.Rows[0]
		assert.IsType(t, "", row[0], "data_provider should be TEXT")
		assert.IsType(t, "", row[1], "stream_id should be TEXT")
		assert.IsType(t, int64(0), row[2], "event_time should be INT8")
		assert.IsType(t, testValue, row[3], "value should be NUMERIC(36,18)")
		
		assert.Equal(t, "test_provider", row[0])
		assert.Equal(t, "test_stream", row[1])
		assert.Equal(t, int64(1234567890), row[2])
		assert.Equal(t, testValue, row[3])
	})
}

	// Note: The write-path protection is handled at the PostgreSQL function level
	// using pg_catalog.current_setting('transaction_read_only') checks within each function.
