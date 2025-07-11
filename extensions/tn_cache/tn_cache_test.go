package tn_cache

import (
	"context"
	"io"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
	"github.com/trufnetwork/node/extensions/tn_cache/scheduler"
)

// createTestLogger creates a logger suitable for testing
func createTestLogger(t *testing.T) log.Logger {
	return log.New(log.WithWriter(io.Discard))
}

// mockService implements a minimal Service for testing
type mockService struct {
	logger log.Logger
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

func TestHasCachedData(t *testing.T) {
	// Test when cache is not initialized
	t.Run("cache not initialized", func(t *testing.T) {
		// Save current extension and restore after test
		oldExt := GetExtension()
		defer func() { SetExtension(oldExt) }()

		SetExtension(nil)

		_, err := GetExtension().CacheDB().HasCachedData(context.Background(), "test", "test", 0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not initialized")
	})

	// Test when cache is initialized
	t.Run("cache initialized", func(t *testing.T) {
		// Save current extension and restore after test
		oldExt := GetExtension()
		defer func() { SetExtension(oldExt) }()

		// Create mock pool
		mockPool, err := pgxmock.NewPool()
		require.NoError(t, err)
		defer mockPool.Close()

		// Set up expectations
		mockPool.ExpectBegin()
		mockPool.ExpectQuery(`SELECT COUNT\(\*\) > 0`).
			WithArgs("test_provider", "test_stream", int64(1000)).
			WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
		mockPool.ExpectQuery(`SELECT COUNT\(\*\) FROM ext_tn_cache\.cached_events`).
			WithArgs("test_provider", "test_stream", int64(1000), int64(2000)).
			WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(5)))
		mockPool.ExpectCommit()

		logger := createTestLogger(t)
		cacheDB := internal.NewCacheDB(mockPool, logger)

		// Create extension with the mock DB
		ext := NewExtension(logger, cacheDB, nil, nil, metrics.NewNoOpMetrics(), nil, mockPool, true)
		SetExtension(ext)

		// Test with cache initialized
		hasData, err := ext.CacheDB().HasCachedData(context.Background(), "test_provider", "test_stream", 1000, 2000)
		require.NoError(t, err)
		assert.True(t, hasData)

		// Verify all expectations were met
		err = mockPool.ExpectationsWereMet()
		require.NoError(t, err)
	})
}

func TestGetCachedData(t *testing.T) {
	// Save current extension and restore after test
	oldExt := GetExtension()
	defer func() { SetExtension(oldExt) }()

	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Test data
	testValue1, _ := types.ParseDecimalExplicit("123.456", 36, 18)
	testValue2, _ := types.ParseDecimalExplicit("456.789", 36, 18)

	// Set up expectations - GetEvents query
	rows := pgxmock.NewRows([]string{"data_provider", "stream_id", "event_time", "value"}).
		AddRow("test_provider", "test_stream", int64(1000), testValue1).
		AddRow("test_provider", "test_stream", int64(1500), testValue2)

	mockPool.ExpectQuery(`SELECT data_provider, stream_id, event_time, value`).
		WithArgs("test_provider", "test_stream", int64(1000), int64(2000)).
		WillReturnRows(rows)

	logger := createTestLogger(t)
	cacheDB := internal.NewCacheDB(mockPool, logger)

	// Create extension with the mock DB
	ext := NewExtension(logger, cacheDB, nil, nil, metrics.NewNoOpMetrics(), nil, mockPool, true)
	SetExtension(ext)

	// Test GetCachedData
	ctx := context.Background()
	events, err := ext.CacheDB().GetCachedEvents(ctx, "test_provider", "test_stream", 1000, 2000)
	require.NoError(t, err)
	assert.Len(t, events, 2)

	// Verify results
	assert.Equal(t, "test_provider", events[0].DataProvider)
	assert.Equal(t, "test_stream", events[0].StreamID)
	assert.Equal(t, int64(1000), events[0].EventTime)
	assert.Equal(t, testValue1.String(), events[0].Value.String())

	assert.Equal(t, "test_provider", events[1].DataProvider)
	assert.Equal(t, "test_stream", events[1].StreamID)
	assert.Equal(t, int64(1500), events[1].EventTime)
	assert.Equal(t, testValue2.String(), events[1].Value.String())

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestGetCachedData_NoData(t *testing.T) {
	// Save current cacheDB and restore after test
	oldExt := GetExtension()
	defer func() { SetExtension(oldExt) }()

	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Set up expectations - GetEvents returns empty result
	mockPool.ExpectQuery(`SELECT data_provider, stream_id, event_time, value`).
		WithArgs("test_provider", "test_stream", int64(1000), int64(2000)).
		WillReturnRows(pgxmock.NewRows([]string{"data_provider", "stream_id", "event_time", "value"}))

	logger := createTestLogger(t)
	cacheDB := internal.NewCacheDB(mockPool, logger)
	ext := NewExtension(logger, cacheDB, nil, nil, metrics.NewNoOpMetrics(), nil, mockPool, true)
	SetExtension(ext)

	// Test GetCachedData with no data
	ctx := context.Background()
	events, err := ext.CacheDB().GetCachedEvents(ctx, "test_provider", "test_stream", 1000, 2000)
	require.NoError(t, err)
	assert.Len(t, events, 0)

	// Verify all expectations were met
	err = mockPool.ExpectationsWereMet()
	require.NoError(t, err)
}

// Test the extension initialization
// TODO: This test needs to be updated to match the current extension initialization
// func TestInitializeExtension(t *testing.T) {
// }

// Test configuration loading
func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name        string
		rawConfig   map[string]string
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid inline config",
			rawConfig: map[string]string{
				"enabled": "true",
				"streams_inline": `[{
					"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
					"stream_id": "st123456789012345678901234567890",
					"cron_schedule": "0 0 * * * *"
				}]`,
			},
			expectError: false,
		},
		{
			name: "invalid cron schedule",
			rawConfig: map[string]string{
				"enabled": "true",
				"streams_inline": `[{
					"data_provider": "0x1234567890abcdef1234567890abcdef12345678",
					"stream_id": "st123456789012345678901234567890",
					"cron_schedule": "invalid"
				}]`,
			},
			expectError: true,
			errorMsg:    "invalid cron schedule",
		},
		{
			name: "missing required fields",
			rawConfig: map[string]string{
				"enabled": "true",
				"streams_inline": `[{
					"data_provider": "0x1234567890abcdef1234567890abcdef12345678"
				}]`,
			},
			expectError: true,
			errorMsg:    "validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loader := config.NewLoader()
			processedConfig, err := loader.LoadAndProcess(context.Background(), config.RawConfig{
				Enabled:            tt.rawConfig["enabled"],
				StreamsInline:      tt.rawConfig["streams_inline"],
				StreamsCSVFile:     tt.rawConfig["streams_csv_file"],
				ResolutionSchedule: tt.rawConfig["resolution_schedule"],
			})

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				require.NoError(t, err)
				assert.True(t, processedConfig.Enabled)
				assert.Greater(t, len(processedConfig.Directives), 0)
			}
		})
	}
}

// Test scheduler creation - simplified test that just verifies construction
func TestSchedulerCreation(t *testing.T) {
	// Create a simple mock app with proper Service
	service := &common.Service{
		Logger: createTestLogger(t),
	}
	app := &common.App{
		Service: service,
	}

	// Create mock pool
	mockPool, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mockPool.Close()

	// Create cache DB
	logger := createTestLogger(t)
	cacheDB := internal.NewCacheDB(mockPool, logger)

	// Create scheduler
	scheduler := scheduler.NewCacheScheduler(scheduler.NewCacheSchedulerParams{
		App:             app,
		CacheDB:         cacheDB,
		EngineOps:       nil,
		Logger:          logger,
		MetricsRecorder: metrics.NewNoOpMetrics(),
	})
	require.NotNil(t, scheduler, "Scheduler should be created")

}
