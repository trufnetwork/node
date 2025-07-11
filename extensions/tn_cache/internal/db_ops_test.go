package internal

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
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


func TestCacheDB_GetStreamConfig(t *testing.T) {
	// Define test data
	testDataProvider := "test_provider"
	testStreamID := "test_stream"
	testFromTimestamp := int64(1234567890)
	testLastRefreshed := time.Now().Unix()
	testCronSchedule := "0 */5 * * * *"

	// Create mock DB
	mockDB := newTestDB()

	// Set up expected result
	resultSet := &kwilsql.ResultSet{
		Columns: []string{"data_provider", "stream_id", "from_timestamp", "last_refreshed", "cron_schedule"},
		Rows: [][]interface{}{
			{testDataProvider, testStreamID, testFromTimestamp, testLastRefreshed, testCronSchedule},
		},
	}
	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		return resultSet, nil
	}

	// Create CacheDB with mock DB
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	// Test GetStreamConfig
	config, err := cacheDB.GetStreamConfig(context.Background(), testDataProvider, testStreamID)
	require.NoError(t, err)
	require.NotNil(t, config)
	assert.Equal(t, testDataProvider, config.DataProvider)
	assert.Equal(t, testStreamID, config.StreamID)
	assert.Equal(t, testFromTimestamp, config.FromTimestamp)
	assert.Equal(t, testLastRefreshed, config.LastRefreshed)
	assert.Equal(t, testCronSchedule, config.CronSchedule)
}


func TestCacheDB_CacheEvents(t *testing.T) {
	// Create mock DB
	mockDB := newTestDB()

	// Test data
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

	// Create CacheDB with mock DB
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	// Test CacheEvents
	err := cacheDB.CacheEvents(context.Background(), events)
	require.NoError(t, err)
}

func TestCacheDB_GetEvents(t *testing.T) {
	// Define test data
	testDataProvider := "test_provider"
	testStreamID := "test_stream"
	testValue1, _ := types.ParseDecimalExplicit("123.456", 36, 18)
	testValue2, _ := types.ParseDecimalExplicit("456.789", 36, 18)

	// Create mock DB
	mockDB := newTestDB()

	// Set up expected result
	resultSet := &kwilsql.ResultSet{
		Columns: []string{"data_provider", "stream_id", "event_time", "value"},
		Rows: [][]interface{}{
			{testDataProvider, testStreamID, int64(1234567890), testValue1},
			{testDataProvider, testStreamID, int64(1234567891), testValue2},
		},
	}
	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		return resultSet, nil
	}

	// Create CacheDB with mock DB
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	// Test GetEvents
	events, err := cacheDB.GetCachedEvents(context.Background(), testDataProvider, testStreamID, 1234567890, 1234567900)
	require.NoError(t, err)
	require.Len(t, events, 2)

	assert.Equal(t, testDataProvider, events[0].DataProvider)
	assert.Equal(t, testStreamID, events[0].StreamID)
	assert.Equal(t, int64(1234567890), events[0].EventTime)
	assert.Equal(t, testValue1.String(), events[0].Value.String())

	assert.Equal(t, testDataProvider, events[1].DataProvider)
	assert.Equal(t, testStreamID, events[1].StreamID)
	assert.Equal(t, int64(1234567891), events[1].EventTime)
	assert.Equal(t, testValue2.String(), events[1].Value.String())
}

func TestCacheDB_ListStreamConfigs(t *testing.T) {
	// Create mock DB
	mockDB := newTestDB()

	// Set up expected result
	resultSet := &kwilsql.ResultSet{
		Columns: []string{"data_provider", "stream_id", "from_timestamp", "last_refreshed", "cron_schedule"},
		Rows: [][]interface{}{
			{"provider1", "stream1", int64(1234567890), int64(1672531200), "0 0 * * * *"},
			{"provider2", "stream2", int64(1234567891), int64(1672617600), "0 */5 * * * *"},
		},
	}
	mockDB.ExecuteFn = func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		return resultSet, nil
	}

	// Create CacheDB with mock DB
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockDB, logger)

	// Test ListStreamConfigs
	configs, err := cacheDB.ListStreamConfigs(context.Background())
	require.NoError(t, err)
	require.Len(t, configs, 2)

	assert.Equal(t, "provider1", configs[0].DataProvider)
	assert.Equal(t, "stream1", configs[0].StreamID)
	assert.Equal(t, int64(1234567890), configs[0].FromTimestamp)

	assert.Equal(t, "provider2", configs[1].DataProvider)
	assert.Equal(t, "stream2", configs[1].StreamID)
	assert.Equal(t, int64(1234567891), configs[1].FromTimestamp)
}

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
