package internal

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
)

func TestCacheDB_AddStreamConfig(t *testing.T) {
	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Set up expectations
	mockPool.ExpectBegin()
	mockPool.ExpectExec(`INSERT INTO ext_tn_cache\.cached_streams`).
		WithArgs("test_provider", "test_stream", int64(1234567890), pgxmock.AnyArg(), "0 */5 * * * *").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mockPool.ExpectCommit()

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

	// Test AddStreamConfig
	config := StreamCacheConfig{
		DataProvider:  "test_provider",
		StreamID:      "test_stream",
		FromTimestamp: 1234567890,
		LastRefreshed: time.Now().UTC().Format(time.RFC3339),
		CronSchedule:  "0 */5 * * * *",
	}

	err = cacheDB.AddStreamConfig(context.Background(), config)
	require.NoError(t, err)

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestCacheDB_GetStreamConfig(t *testing.T) {
	// Define test data
	testDataProvider := "test_provider"
	testStreamID := "test_stream"
	testFromTimestamp := int64(1234567890)
	testLastRefreshed := time.Now().UTC().Format(time.RFC3339)
	testCronSchedule := "0 */5 * * * *"

	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Set up expectations for QueryRow
	rows := pgxmock.NewRows([]string{"data_provider", "stream_id", "from_timestamp", "last_refreshed", "cron_schedule"}).
		AddRow(testDataProvider, testStreamID, testFromTimestamp, testLastRefreshed, testCronSchedule)
	
	mockPool.ExpectQuery(`SELECT data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule`).
		WithArgs(testDataProvider, testStreamID).
		WillReturnRows(rows)

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

	// Test GetStreamConfig
	config, err := cacheDB.GetStreamConfig(context.Background(), testDataProvider, testStreamID)
	require.NoError(t, err)
	require.NotNil(t, config)
	assert.Equal(t, testDataProvider, config.DataProvider)
	assert.Equal(t, testStreamID, config.StreamID)
	assert.Equal(t, testFromTimestamp, config.FromTimestamp)
	assert.Equal(t, testLastRefreshed, config.LastRefreshed)
	assert.Equal(t, testCronSchedule, config.CronSchedule)

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestCacheDB_GetStreamConfig_NotFound(t *testing.T) {
	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Set up expectations for QueryRow with no results
	mockPool.ExpectQuery(`SELECT data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule`).
		WithArgs("unknown_provider", "unknown_stream").
		WillReturnError(sql.ErrNoRows)

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

	// Test GetStreamConfig with non-existent stream
	config, err := cacheDB.GetStreamConfig(context.Background(), "unknown_provider", "unknown_stream")
	assert.Error(t, err)
	assert.Nil(t, config)
	assert.Contains(t, err.Error(), "sql: no rows in result set")

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestCacheDB_CacheEvents(t *testing.T) {
	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

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

	// Set up expectations
	mockPool.ExpectBegin()
	mockPool.ExpectExec(`INSERT INTO ext_tn_cache\.cached_events`).
		WithArgs(
			[]string{"test_provider", "test_provider"},
			[]string{"test_stream", "test_stream"},
			[]int64{1234567890, 1234567891},
			[]*types.Decimal{testValue1, testValue2},
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 2))
	mockPool.ExpectExec(`UPDATE ext_tn_cache\.cached_streams`).
		WithArgs("test_provider", "test_stream", pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mockPool.ExpectCommit()

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

	// Test CacheEvents
	err = cacheDB.CacheEvents(context.Background(), events)
	require.NoError(t, err)

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestCacheDB_GetEvents(t *testing.T) {
	// Define test data
	testDataProvider := "test_provider"
	testStreamID := "test_stream"
	testValue1, _ := types.ParseDecimalExplicit("123.456", 36, 18)
	testValue2, _ := types.ParseDecimalExplicit("456.789", 36, 18)

	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Set up expectations
	rows := pgxmock.NewRows([]string{"data_provider", "stream_id", "event_time", "value"}).
		AddRow(testDataProvider, testStreamID, int64(1234567890), testValue1).
		AddRow(testDataProvider, testStreamID, int64(1234567891), testValue2)

	mockPool.ExpectQuery(`SELECT data_provider, stream_id, event_time, value`).
		WithArgs(testDataProvider, testStreamID, int64(1234567890), int64(1234567900)).
		WillReturnRows(rows)

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

	// Test GetEvents
	events, err := cacheDB.GetEvents(context.Background(), testDataProvider, testStreamID, 1234567890, 1234567900)
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

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestCacheDB_ListStreamConfigs(t *testing.T) {
	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Set up expectations
	rows := pgxmock.NewRows([]string{"data_provider", "stream_id", "from_timestamp", "last_refreshed", "cron_schedule"}).
		AddRow("provider1", "stream1", int64(1234567890), "2023-01-01T00:00:00Z", "0 0 * * * *").
		AddRow("provider2", "stream2", int64(1234567891), "2023-01-02T00:00:00Z", "0 */5 * * * *")

	mockPool.ExpectQuery(`SELECT data_provider, stream_id, from_timestamp, last_refreshed, cron_schedule`).
		WillReturnRows(rows)

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

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

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestCacheDB_HasCachedData(t *testing.T) {
	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Set up expectations
	mockPool.ExpectBegin()
	
	// First query checks if stream is configured
	mockPool.ExpectQuery(`SELECT COUNT\(\*\) > 0`).
		WithArgs("test_provider", "test_stream", int64(1234567890)).
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	
	// Second query checks if events exist
	mockPool.ExpectQuery(`SELECT COUNT\(\*\) > 0`).
		WithArgs("test_provider", "test_stream", int64(1234567890), int64(1234567900)).
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	
	mockPool.ExpectCommit()

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

	// Test HasCachedData
	hasData, err := cacheDB.HasCachedData(context.Background(), "test_provider", "test_stream", 1234567890, 1234567900)
	require.NoError(t, err)
	assert.True(t, hasData)

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestCacheDB_DeleteStreamData(t *testing.T) {
	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Set up expectations
	mockPool.ExpectBegin()
	mockPool.ExpectExec(`DELETE FROM ext_tn_cache\.cached_events`).
		WithArgs("test_provider", "test_stream").
		WillReturnResult(pgxmock.NewResult("DELETE", 10))
	mockPool.ExpectExec(`DELETE FROM ext_tn_cache\.cached_streams`).
		WithArgs("test_provider", "test_stream").
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mockPool.ExpectCommit()

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

	// Test DeleteStreamData
	err = cacheDB.DeleteStreamData(context.Background(), "test_provider", "test_stream")
	require.NoError(t, err)

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestCacheDB_UpdateStreamConfigsAtomic(t *testing.T) {
	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	newConfigs := []StreamCacheConfig{
		{
			DataProvider:  "provider1",
			StreamID:      "stream1",
			FromTimestamp: 1234567890,
			LastRefreshed: "2023-01-01T00:00:00Z",
			CronSchedule:  "0 0 * * * *",
		},
	}

	toDelete := []StreamCacheConfig{
		{
			DataProvider: "provider2",
			StreamID:     "stream2",
		},
	}

	// Set up expectations
	mockPool.ExpectBegin()
	
	// Delete query
	mockPool.ExpectExec(`DELETE FROM ext_tn_cache\.cached_streams`).
		WithArgs([]string{"provider2"}, []string{"stream2"}).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	
	// Insert/Update query
	mockPool.ExpectExec(`INSERT INTO ext_tn_cache\.cached_streams`).
		WithArgs(
			[]string{"provider1"},
			[]string{"stream1"},
			[]int64{1234567890},
			[]string{"2023-01-01T00:00:00Z"},
			[]string{"0 0 * * * *"},
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	
	mockPool.ExpectCommit()

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

	// Test UpdateStreamConfigsAtomic
	err = cacheDB.UpdateStreamConfigsAtomic(context.Background(), newConfigs, toDelete)
	require.NoError(t, err)

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestCacheDB_TransactionRollback tests that transactions are properly rolled back on error
func TestCacheDB_TransactionRollback(t *testing.T) {
	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Set up expectations
	mockPool.ExpectBegin()
	mockPool.ExpectExec(`INSERT INTO ext_tn_cache\.cached_streams`).
		WithArgs("test_provider", "test_stream", int64(1234567890), pgxmock.AnyArg(), "0 */5 * * * *").
		WillReturnError(fmt.Errorf("constraint violation"))
	mockPool.ExpectRollback()

	// Create CacheDB with mock pool
	logger := log.New(log.WithWriter(io.Discard))
	cacheDB := NewCacheDB(mockPool, logger)

	// Test AddStreamConfig with error
	config := StreamCacheConfig{
		DataProvider:  "test_provider",
		StreamID:      "test_stream",
		FromTimestamp: 1234567890,
		LastRefreshed: time.Now().UTC().Format(time.RFC3339),
		CronSchedule:  "0 */5 * * * *",
	}

	err = cacheDB.AddStreamConfig(context.Background(), config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "constraint violation")

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}