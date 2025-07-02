package tn_cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
)

// mockEngine implements common.Engine for testing
type mockEngine struct{}

func (m *mockEngine) Call(ctx *common.EngineContext, db sql.DB, namespace, action string, args []any, resultFn func(*common.Row) error) (*common.CallResult, error) {
	return &common.CallResult{}, nil
}

func (m *mockEngine) CallWithoutEngineCtx(ctx context.Context, db sql.DB, namespace, action string, args []any, resultFn func(*common.Row) error) (*common.CallResult, error) {
	// Mock a successful call with sample data
	if resultFn != nil {
		// Simulate returning some sample events with decimal(36,18) value
		testDecimal, _ := types.ParseDecimalExplicit("123.45", 36, 18)
		row := &common.Row{
			ColumnNames: []string{"event_time", "value"},
			Values:      []any{int64(1640995200), testDecimal}, // Sample timestamp and decimal value
		}
		if err := resultFn(row); err != nil {
			return nil, err
		}
	}
	return &common.CallResult{
		Logs: []string{"cache_miss"},
	}, nil
}

func (m *mockEngine) Execute(ctx *common.EngineContext, db sql.DB, statement string, params map[string]any, fn func(*common.Row) error) error {
	return nil
}

func (m *mockEngine) ExecuteWithoutEngineCtx(ctx context.Context, db sql.DB, statement string, params map[string]any, fn func(*common.Row) error) error {
	return nil
}

func TestCacheScheduler_New(t *testing.T) {
	// Create mock app and dependencies
	mockApp := &common.App{
		Service: &common.Service{},
		Engine:  &mockEngine{},
	}

	// Create test logger
	logger := log.New(log.WithWriter(nil)) // Discard logs during tests

	// Create mock CacheDB (nil for this test)
	var cacheDB *internal.CacheDB

	// Test creating scheduler
	scheduler := NewCacheScheduler(mockApp, cacheDB, logger)
	
	require.NotNil(t, scheduler, "Scheduler should be created")
	assert.Equal(t, mockApp, scheduler.app, "App should be set correctly")
	assert.NotNil(t, scheduler.cron, "Cron scheduler should be initialized")
	assert.NotNil(t, scheduler.jobs, "Jobs map should be initialized")
	assert.NotNil(t, scheduler.breakers, "Circuit breakers map should be initialized")
	assert.Equal(t, "truf_db", scheduler.namespace, "Default namespace should be set")
}

func TestCacheScheduler_WithCustomNamespace(t *testing.T) {
	// Create mock app and dependencies
	mockApp := &common.App{
		Service: &common.Service{},
		Engine:  &mockEngine{},
	}

	// Create test logger
	logger := log.New(log.WithWriter(nil))

	// Create mock CacheDB
	var cacheDB *internal.CacheDB

	// Test creating scheduler with custom namespace
	scheduler := NewCacheSchedulerWithNamespace(mockApp, cacheDB, logger, "custom_db")
	
	require.NotNil(t, scheduler, "Scheduler should be created")
	assert.Equal(t, "custom_db", scheduler.namespace, "Custom namespace should be set")
}

func TestCacheScheduler_GroupBySchedule(t *testing.T) {
	// Create scheduler for testing utility methods
	mockApp := &common.App{}
	logger := log.New(log.WithWriter(nil))
	cacheDB := &internal.CacheDB{}
	scheduler := NewCacheScheduler(mockApp, cacheDB, logger)

	// Create test instructions with different schedules
	from := int64(1640995200)
	instructions := []config.InstructionDirective{
		{
			ID:           "test1",
			DataProvider: "provider1",
			StreamID:     "stream1",
			Schedule:     config.Schedule{CronExpr: "0 * * * *"}, // Hourly
			TimeRange:    config.TimeRange{From: &from},
		},
		{
			ID:           "test2",
			DataProvider: "provider2",
			StreamID:     "stream2",
			Schedule:     config.Schedule{CronExpr: "0 * * * *"}, // Hourly (same as test1)
			TimeRange:    config.TimeRange{From: &from},
		},
		{
			ID:           "test3",
			DataProvider: "provider3",
			StreamID:     "stream3",
			Schedule:     config.Schedule{CronExpr: "0 0 * * *"}, // Daily
			TimeRange:    config.TimeRange{From: &from},
		},
	}

	// Test grouping by schedule
	groups := scheduler.groupBySchedule(instructions)

	// Verify grouping
	require.Len(t, groups, 2, "Should have 2 different schedules")
	
	hourlyGroup := groups["0 * * * *"]
	require.Len(t, hourlyGroup, 2, "Hourly group should have 2 instructions")
	assert.Equal(t, "test1", hourlyGroup[0].ID)
	assert.Equal(t, "test2", hourlyGroup[1].ID)

	dailyGroup := groups["0 0 * * *"]
	require.Len(t, dailyGroup, 1, "Daily group should have 1 instruction")
	assert.Equal(t, "test3", dailyGroup[0].ID)
}

func TestCacheScheduler_CreateJobFunc(t *testing.T) {
	// Create mock dependencies
	mockApp := &common.App{
		Engine: &mockEngine{},
	}
	logger := log.New(log.WithWriter(nil))
	cacheDB := &internal.CacheDB{}
	
	scheduler := NewCacheScheduler(mockApp, cacheDB, logger)
	scheduler.ctx, scheduler.cancel = context.WithCancel(context.Background())
	defer scheduler.cancel()

	// Create test instruction
	from := int64(1640995200)
	instructions := []config.InstructionDirective{
		{
			ID:           "test1",
			Type:         config.DirectiveSpecific,
			DataProvider: "provider1",
			StreamID:     "stream1",
			Schedule:     config.Schedule{CronExpr: "0 * * * *"},
			TimeRange:    config.TimeRange{From: &from},
		},
	}

	// Create job function
	jobFunc := scheduler.createJobFunc(instructions)

	// Test that job function can be called without panicking
	// Note: This will likely fail due to missing database setup, but shouldn't panic
	require.NotNil(t, jobFunc, "Job function should not be nil")
	
	// In a real test, we would set up proper mocks and verify the behavior
	// For now, we just ensure the function is created successfully
}

func TestCacheScheduler_CircuitBreaker(t *testing.T) {
	// Create mock dependencies
	mockApp := &common.App{
		Engine: &mockEngine{},
	}
	logger := log.New(log.WithWriter(nil))
	cacheDB := &internal.CacheDB{}
	
	scheduler := NewCacheScheduler(mockApp, cacheDB, logger)

	// Test getting circuit breaker for a stream
	streamKey := "provider1/stream1"
	cb1 := scheduler.getCircuitBreaker(streamKey)
	require.NotNil(t, cb1, "Circuit breaker should be created")

	// Test getting the same circuit breaker again
	cb2 := scheduler.getCircuitBreaker(streamKey)
	assert.Equal(t, cb1, cb2, "Should return the same circuit breaker instance")

	// Test getting circuit breaker for different stream
	cb3 := scheduler.getCircuitBreaker("provider2/stream2")
	require.NotNil(t, cb3, "Circuit breaker should be created for new stream")
	assert.NotEqual(t, cb1, cb3, "Different streams should have different circuit breakers")
}