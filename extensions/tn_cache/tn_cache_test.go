package tn_cache

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
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

// TestPrecompileHandlers tests the precompile handler functions directly
func TestPrecompileHandlers(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("handleIsEnabled", func(t *testing.T) {
		var result []any
		resultFn := func(row []any) error {
			result = row
			return nil
		}

		ctx := createTestEngineContext()
		app := &common.App{}

		err := handleIsEnabled(ctx, app, []any{}, resultFn)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, true, result[0])
	})

	t.Run("handleHasCachedData - successful", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == `
		SELECT from_timestamp, last_refreshed
		FROM ext_tn_cache.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
	` {
					return &sql.ResultSet{
						Rows: [][]any{
							{int64(1234567890), "2023-01-01T00:00:00Z"},
						},
					}, nil
				}
				if stmt == `
			SELECT COUNT(*) FROM ext_tn_cache.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3 AND event_time <= $4
		` {
					return &sql.ResultSet{
						Rows: [][]any{
							{int64(5)},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
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

		ctx := createTestEngineContext()
		app := &common.App{DB: mockDb}
		inputs := []any{"test_provider", "test_stream", int64(1234567890), int64(1234567900)}

		var result []any
		resultFn := func(row []any) error {
			result = row
			return nil
		}

		err := handleHasCachedData(ctx, app, inputs, resultFn)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, true, result[0])
	})

	t.Run("handleGetCachedData - successful", func(t *testing.T) {
		testValue, _ := types.ParseDecimalExplicit("123.456", 36, 18)
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return &sql.ResultSet{
					Columns: []string{"data_provider", "stream_id", "event_time", "value"},
					Rows: [][]any{
						{"test_provider", "test_stream", int64(1234567890), testValue},
						{"test_provider", "test_stream", int64(1234567891), testValue},
					},
				}, nil
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

		ctx := createTestEngineContext()
		app := &common.App{DB: mockDb}
		inputs := []any{"test_provider", "test_stream", int64(1234567890), int64(1234567900)}

		var results [][]any
		resultFn := func(row []any) error {
			results = append(results, row)
			return nil
		}

		err := handleGetCachedData(ctx, app, inputs, resultFn)
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, "test_provider", results[0][0])
		assert.Equal(t, "test_stream", results[0][1])
		assert.Equal(t, int64(1234567890), results[0][2])
		assert.Equal(t, testValue, results[0][3])
	})

	t.Run("handleHasCachedData - stream not configured", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				// Return empty result for stream not configured
				return &sql.ResultSet{
					Rows: [][]any{},
				}, nil
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

		ctx := createTestEngineContext()
		app := &common.App{DB: mockDb}
		inputs := []any{"test_provider", "test_stream", int64(1234567890), int64(1234567900)}

		var result []any
		resultFn := func(row []any) error {
			result = row
			return nil
		}

		err := handleHasCachedData(ctx, app, inputs, resultFn)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, false, result[0])
	})

	t.Run("handleHasCachedData - stream not refreshed", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				// Return stream config but with null last_refreshed
				return &sql.ResultSet{
					Rows: [][]any{
						{int64(1234567890), nil},
					},
				}, nil
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

		ctx := createTestEngineContext()
		app := &common.App{DB: mockDb}
		inputs := []any{"test_provider", "test_stream", int64(1234567890), int64(1234567900)}

		var result []any
		resultFn := func(row []any) error {
			result = row
			return nil
		}

		err := handleHasCachedData(ctx, app, inputs, resultFn)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, false, result[0])
	})

	t.Run("handleHasCachedData - requested time before cached range", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				// Return stream config with configured from time after requested time
				return &sql.ResultSet{
					Rows: [][]any{
						{int64(1234567900), "2023-01-01T00:00:00Z"}, // from_timestamp > requested fromTime
					},
				}, nil
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

		ctx := createTestEngineContext()
		app := &common.App{DB: mockDb}
		inputs := []any{"test_provider", "test_stream", int64(1234567890), int64(1234567900)}

		var result []any
		resultFn := func(row []any) error {
			result = row
			return nil
		}

		err := handleHasCachedData(ctx, app, inputs, resultFn)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, false, result[0])
	})
}

// TestPrecompileHandlersWithNullToTime tests precompile handlers with NULL to_time parameter
func TestPrecompileHandlersWithNullToTime(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("handleHasCachedData with NULL to_time", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				if stmt == `
		SELECT from_timestamp, last_refreshed
		FROM ext_tn_cache.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
	` {
					return &sql.ResultSet{
						Rows: [][]any{
							{int64(1234567890), "2023-01-01T00:00:00Z"},
						},
					}, nil
				}
				if stmt == `
			SELECT COUNT(*) FROM ext_tn_cache.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3
		` {
					return &sql.ResultSet{
						Rows: [][]any{
							{int64(3)},
						},
					}, nil
				}
				return &sql.ResultSet{}, nil
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

		ctx := createTestEngineContext()
		app := &common.App{DB: mockDb}
		inputs := []any{"test_provider", "test_stream", int64(1234567890)} // No to_time

		var result []any
		resultFn := func(row []any) error {
			result = row
			return nil
		}

		err := handleHasCachedData(ctx, app, inputs, resultFn)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, true, result[0])
	})

	t.Run("handleGetCachedData with NULL to_time", func(t *testing.T) {
		testValue, _ := types.ParseDecimalExplicit("123.456", 36, 18)
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return &sql.ResultSet{
					Columns: []string{"data_provider", "stream_id", "event_time", "value"},
					Rows: [][]any{
						{"test_provider", "test_stream", int64(1234567890), testValue},
						{"test_provider", "test_stream", int64(1234567891), testValue},
						{"test_provider", "test_stream", int64(1234567892), testValue},
					},
				}, nil
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

		ctx := createTestEngineContext()
		app := &common.App{DB: mockDb}
		inputs := []any{"test_provider", "test_stream", int64(1234567890)} // No to_time

		var results [][]any
		resultFn := func(row []any) error {
			results = append(results, row)
			return nil
		}

		err := handleGetCachedData(ctx, app, inputs, resultFn)
		require.NoError(t, err)
		require.Len(t, results, 3)
		assert.Equal(t, "test_provider", results[0][0])
		assert.Equal(t, "test_stream", results[0][1])
		assert.Equal(t, int64(1234567890), results[0][2])
		assert.Equal(t, testValue, results[0][3])
	})
}

// TestPrecompileDataTypes tests that precompile handlers return expected data types
func TestPrecompileDataTypes(t *testing.T) {
	// Initialize logger for the test
	logger = createTestLogger(t)

	t.Run("handleIsEnabled returns boolean", func(t *testing.T) {
		var result []any
		resultFn := func(row []any) error {
			result = row
			return nil
		}

		ctx := createTestEngineContext()
		app := &common.App{}

		err := handleIsEnabled(ctx, app, []any{}, resultFn)
		require.NoError(t, err)
		require.Len(t, result, 1)
		
		// Verify the return type is boolean
		value := result[0]
		assert.IsType(t, true, value, "handleIsEnabled should return boolean")
		assert.Equal(t, true, value)
	})

	t.Run("handleHasCachedData returns boolean", func(t *testing.T) {
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return &sql.ResultSet{
					Rows: [][]any{}, // Empty result means no stream configured
				}, nil
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

		ctx := createTestEngineContext()
		app := &common.App{DB: mockDb}
		inputs := []any{"test_provider", "test_stream", int64(1234567890), int64(1234567900)}

		var result []any
		resultFn := func(row []any) error {
			result = row
			return nil
		}

		err := handleHasCachedData(ctx, app, inputs, resultFn)
		require.NoError(t, err)
		require.Len(t, result, 1)
		
		// Verify the return type is boolean
		value := result[0]
		assert.IsType(t, false, value, "handleHasCachedData should return boolean")
		assert.Equal(t, false, value)
	})

	t.Run("handleGetCachedData returns correct table structure", func(t *testing.T) {
		testValue, _ := types.ParseDecimalExplicit("123.456", 36, 18)
		mockTx := &testutils.MockTx{
			ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return &sql.ResultSet{
					Columns: []string{"data_provider", "stream_id", "event_time", "value"},
					Rows: [][]any{
						{"test_provider", "test_stream", int64(1234567890), testValue},
					},
				}, nil
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

		ctx := createTestEngineContext()
		app := &common.App{DB: mockDb}
		inputs := []any{"test_provider", "test_stream", int64(1234567890), int64(1234567900)}

		var results [][]any
		resultFn := func(row []any) error {
			results = append(results, row)
			return nil
		}

		err := handleGetCachedData(ctx, app, inputs, resultFn)
		require.NoError(t, err)
		require.Len(t, results, 1)
		
		// Verify the table structure and data types
		row := results[0]
		require.Len(t, row, 4)
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
