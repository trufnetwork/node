package internal

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
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
	executeFn   func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error)
	beginTxFn   func(ctx context.Context) (sql.Tx, error)
	rollbackFn  func(ctx context.Context) error
	commitFn    func(ctx context.Context) error
	statements  []string
	commitCalls int
}

func (m *mockTx) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	m.statements = append(m.statements, stmt)
	return m.executeFn(ctx, stmt, args...)
}

func (m *mockTx) BeginTx(ctx context.Context) (sql.Tx, error) {
	return m.beginTxFn(ctx)
}

func (m *mockTx) Rollback(ctx context.Context) error {
	return m.rollbackFn(ctx)
}

func (m *mockTx) Commit(ctx context.Context) error {
	m.commitCalls++
	return m.commitFn(ctx)
}

// createTestLogger creates a logger suitable for testing
func createTestLogger(t *testing.T) log.Logger {
	return log.New(log.WithWriter(io.Discard))
}

func TestCacheDB_AddStreamConfig(t *testing.T) {
	// Setup mock transaction
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
			return nil, fmt.Errorf("should not be called")
		},
	}

	// Setup mock DB
	mockDb := &mockDB{
		beginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return mockTx, nil
		},
		executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			return nil, fmt.Errorf("should not be called")
		},
	}

	// Create CacheDB with mock DB
	logger := createTestLogger(t)
	cacheDB := NewCacheDB(mockDb, logger)

	// Test AddStreamConfig
	config := StreamCacheConfig{
		DataProvider:  "test_provider",
		StreamID:      "test_stream",
		FromTimestamp: 1234567890,
		LastRefreshed: time.Now().UTC().Format(time.RFC3339),
		CronSchedule:  "*/5 * * * *",
	}

	err := cacheDB.AddStreamConfig(context.Background(), config)
	require.NoError(t, err)
	assert.Equal(t, 1, mockTx.commitCalls)
	assert.Contains(t, mockTx.statements[0], "INSERT INTO ext_tn_cache.cached_streams")
}

func TestCacheDB_GetStreamConfig(t *testing.T) {
	// Define test data
	testDataProvider := "test_provider"
	testStreamID := "test_stream"
	testFromTimestamp := int64(1234567890)
	testLastRefreshed := time.Now().UTC().Format(time.RFC3339)
	testCronSchedule := "*/5 * * * *"

	// Setup mock transaction
	mockTx := &mockTx{
		executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			if len(args) >= 2 && args[0] == testDataProvider && args[1] == testStreamID {
				return &sql.ResultSet{
					Columns: []string{"data_provider", "stream_id", "from_timestamp", "last_refreshed", "cron_schedule"},
					Rows: [][]any{
						{testDataProvider, testStreamID, testFromTimestamp, testLastRefreshed, testCronSchedule},
					},
				}, nil
			}
			return &sql.ResultSet{Rows: [][]any{}}, nil
		},
		commitFn: func(ctx context.Context) error {
			return nil
		},
		rollbackFn: func(ctx context.Context) error {
			return nil
		},
		beginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return nil, fmt.Errorf("should not be called")
		},
	}

	// Setup mock DB
	mockDb := &mockDB{
		beginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return mockTx, nil
		},
		executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			return nil, fmt.Errorf("should not be called")
		},
	}

	// Create CacheDB with mock DB
	logger := createTestLogger(t)
	cacheDB := NewCacheDB(mockDb, logger)

	// Test GetStreamConfig
	config, err := cacheDB.GetStreamConfig(context.Background(), testDataProvider, testStreamID)
	require.NoError(t, err)
	require.NotNil(t, config)
	assert.Equal(t, testDataProvider, config.DataProvider)
	assert.Equal(t, testStreamID, config.StreamID)
	assert.Equal(t, testFromTimestamp, config.FromTimestamp)
	assert.Equal(t, testLastRefreshed, config.LastRefreshed)
	assert.Equal(t, testCronSchedule, config.CronSchedule)
	assert.Equal(t, 1, mockTx.commitCalls)
}

func TestCacheDB_CacheEvents(t *testing.T) {
	// Setup mock transaction
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
			return nil, fmt.Errorf("should not be called")
		},
	}

	// Setup mock DB
	mockDb := &mockDB{
		beginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return mockTx, nil
		},
		executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			return nil, fmt.Errorf("should not be called")
		},
	}

	// Create CacheDB with mock DB
	logger := createTestLogger(t)
	cacheDB := NewCacheDB(mockDb, logger)

	// Test CacheEvents
	testValue1, _ := types.ParseDecimalExplicit("123.456", 36, 18)
	testValue2, _ := types.ParseDecimalExplicit("456.789", 36, 18)
	events := []CachedEvent{
		{
			DataProvider: "test_provider",
			StreamID:     "test_stream",
			EventTime:    1234567890,
			Value:        testValue1,
		},
		{
			DataProvider: "test_provider",
			StreamID:     "test_stream",
			EventTime:    1234567891,
			Value:        testValue2,
		},
	}

	err := cacheDB.CacheEvents(context.Background(), events)
	require.NoError(t, err)
	assert.Equal(t, 1, mockTx.commitCalls)
	assert.Len(t, mockTx.statements, 2) // Insert events + update last_refreshed
}

func TestCacheDB_GetEvents(t *testing.T) {
	// Define test data
	testDataProvider := "test_provider"
	testStreamID := "test_stream"
	testEventTime1 := int64(1234567890)
	testEventTime2 := int64(1234567891)
	testValue1, _ := types.ParseDecimalExplicit("123.456", 36, 18)
	testValue2, _ := types.ParseDecimalExplicit("456.789", 36, 18)

	// Setup mock transaction
	mockTx := &mockTx{
		executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			if len(args) >= 2 && args[0] == testDataProvider && args[1] == testStreamID {
				return &sql.ResultSet{
					Columns: []string{"data_provider", "stream_id", "event_time", "value"},
					Rows: [][]any{
						{testDataProvider, testStreamID, testEventTime1, testValue1},
						{testDataProvider, testStreamID, testEventTime2, testValue2},
					},
				}, nil
			}
			return &sql.ResultSet{Rows: [][]any{}}, nil
		},
		commitFn: func(ctx context.Context) error {
			return nil
		},
		rollbackFn: func(ctx context.Context) error {
			return nil
		},
		beginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return nil, fmt.Errorf("should not be called")
		},
	}

	// Setup mock DB
	mockDb := &mockDB{
		beginTxFn: func(ctx context.Context) (sql.Tx, error) {
			return mockTx, nil
		},
		executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
			return nil, fmt.Errorf("should not be called")
		},
	}

	// Create CacheDB with mock DB
	logger := createTestLogger(t)
	cacheDB := NewCacheDB(mockDb, logger)

	// Test GetEvents
	events, err := cacheDB.GetEvents(context.Background(), testDataProvider, testStreamID, 0, 0)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, testDataProvider, events[0].DataProvider)
	assert.Equal(t, testStreamID, events[0].StreamID)
	assert.Equal(t, testEventTime1, events[0].EventTime)
	assert.Equal(t, testValue1, events[0].Value)
	assert.Equal(t, testEventTime2, events[1].EventTime)
	assert.Equal(t, testValue2, events[1].Value)
	assert.Equal(t, 1, mockTx.commitCalls)
}

func TestCacheDB_HasCachedData(t *testing.T) {
	// Define test data
	testDataProvider := "test_provider"
	testStreamID := "test_stream"
	fromTime := int64(1234567890)

	tests := []struct {
		name            string
		hasStreamConfig bool
		hasEvents       bool
		expectedResult  bool
	}{
		{
			name:            "has stream config and events",
			hasStreamConfig: true,
			hasEvents:       true,
			expectedResult:  true,
		},
		{
			name:            "has stream config but no events",
			hasStreamConfig: true,
			hasEvents:       false,
			expectedResult:  false,
		},
		{
			name:            "no stream config",
			hasStreamConfig: false,
			hasEvents:       false,
			expectedResult:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mock transaction
			streamConfigIdx := 0
			eventsIdx := 0
			mockTx := &mockTx{
				executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
					if streamConfigIdx == 0 {
						streamConfigIdx++
						return &sql.ResultSet{
							Rows: [][]any{
								{tc.hasStreamConfig},
							},
						}, nil
					}
					if eventsIdx == 0 && tc.hasStreamConfig {
						eventsIdx++
						return &sql.ResultSet{
							Rows: [][]any{
								{tc.hasEvents},
							},
						}, nil
					}
					return &sql.ResultSet{Rows: [][]any{}}, nil
				},
				commitFn: func(ctx context.Context) error {
					return nil
				},
				rollbackFn: func(ctx context.Context) error {
					return nil
				},
				beginTxFn: func(ctx context.Context) (sql.Tx, error) {
					return nil, fmt.Errorf("should not be called")
				},
			}

			// Setup mock DB
			mockDb := &mockDB{
				beginTxFn: func(ctx context.Context) (sql.Tx, error) {
					return mockTx, nil
				},
				executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
					return nil, fmt.Errorf("should not be called")
				},
			}

			// Create CacheDB with mock DB
			logger := createTestLogger(t)
			cacheDB := NewCacheDB(mockDb, logger)

			// Test HasCachedData
			result, err := cacheDB.HasCachedData(context.Background(), testDataProvider, testStreamID, fromTime, 0)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedResult, result)
			assert.Equal(t, 1, mockTx.commitCalls)
		})
	}
}

func TestCacheDB_ErrorHandling(t *testing.T) {
	// Test transaction begin error
	t.Run("transaction begin error", func(t *testing.T) {
		mockDb := &mockDB{
			beginTxFn: func(ctx context.Context) (sql.Tx, error) {
				return nil, fmt.Errorf("transaction error")
			},
		}

		logger := createTestLogger(t)
		cacheDB := NewCacheDB(mockDb, logger)

		_, err := cacheDB.GetStreamConfig(context.Background(), "provider", "stream")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "begin transaction")
	})

	// Test transaction execute error
	t.Run("execute error", func(t *testing.T) {
		mockTx := &mockTx{
			executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				return nil, fmt.Errorf("execute error")
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
		cacheDB := NewCacheDB(mockDb, logger)

		_, err := cacheDB.GetStreamConfig(context.Background(), "provider", "stream")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "query stream config")
	})

	// Test fatal DB error
	t.Run("fatal DB error", func(t *testing.T) {
		mockTx := &mockTx{
			executeFn: func(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
				pgErr := &pgconn.PgError{
					Code: "XX000", // Internal error
				}
				return nil, pgErr
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
		cacheDB := NewCacheDB(mockDb, logger)

		_, err := cacheDB.GetStreamConfig(context.Background(), "provider", "stream")
		require.Error(t, err)
	})
}
