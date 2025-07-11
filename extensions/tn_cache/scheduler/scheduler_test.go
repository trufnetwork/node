package scheduler

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
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
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
	// Create test logger
	logger := log.New(log.WithWriter(nil)) // Discard logs during tests

	// Create mock CacheDB (nil for this test)
	var cacheDB *internal.CacheDB

	// Test creating scheduler
	scheduler := NewCacheScheduler(NewCacheSchedulerParams{
		Service:         &common.Service{},
		CacheDB:         cacheDB,
		EngineOps:       nil,
		Logger:          logger,
		MetricsRecorder: metrics.NewNoOpMetrics(),
		Namespace:       "",
	})

	require.NotNil(t, scheduler, "Scheduler should be created")
	assert.NotNil(t, scheduler.cron, "Cron scheduler should be initialized")
	assert.NotNil(t, scheduler.jobs, "Jobs map should be initialized")
	assert.Equal(t, "", scheduler.namespace, "Default namespace should be empty")
}

func TestCacheScheduler_WithCustomNamespace(t *testing.T) {
	// Create test logger
	logger := log.New(log.WithWriter(nil))

	// Create mock CacheDB
	var cacheDB *internal.CacheDB

	// Test creating scheduler with custom namespace
	scheduler := NewCacheScheduler(NewCacheSchedulerParams{
		Service:         &common.Service{},
		CacheDB:         cacheDB,
		EngineOps:       nil,
		Logger:          logger,
		MetricsRecorder: metrics.NewNoOpMetrics(),
		Namespace:       "custom_db",
	})

	require.NotNil(t, scheduler, "Scheduler should be created")
	assert.Equal(t, "custom_db", scheduler.namespace, "Custom namespace should be set")
}

func TestCacheScheduler_GroupBySchedule(t *testing.T) {
	logger := log.New(log.WithWriter(nil))
	cacheDB := &internal.CacheDB{}
	scheduler := NewCacheScheduler(NewCacheSchedulerParams{
		Service:         &common.Service{},
		CacheDB:         cacheDB,
		EngineOps:       nil,
		Logger:          logger,
		MetricsRecorder: metrics.NewNoOpMetrics(),
		Namespace:       "",
	})

	// Create test directives with different schedules
	from := int64(1640995200)
	directives := []config.CacheDirective{
		{
			ID:           "test1",
			DataProvider: "provider1",
			StreamID:     "stream1",
			Schedule:     config.Schedule{CronExpr: "0 0 * * * *"}, // Hourly
			TimeRange:    config.TimeRange{From: &from},
		},
		{
			ID:           "test2",
			DataProvider: "provider2",
			StreamID:     "stream2",
			Schedule:     config.Schedule{CronExpr: "0 0 * * * *"}, // Hourly (same as test1)
			TimeRange:    config.TimeRange{From: &from},
		},
		{
			ID:           "test3",
			DataProvider: "provider3",
			StreamID:     "stream3",
			Schedule:     config.Schedule{CronExpr: "0 0 0 * * *"}, // Daily
			TimeRange:    config.TimeRange{From: &from},
		},
	}

	// Test grouping by schedule
	groups := scheduler.groupBySchedule(directives)

	// Verify grouping
	require.Len(t, groups, 2, "Should have 2 different schedules")

	hourlyGroup := groups["0 0 * * * *"]
	require.Len(t, hourlyGroup, 2, "Hourly group should have 2 directives")
	assert.Equal(t, "test1", hourlyGroup[0].ID)
	assert.Equal(t, "test2", hourlyGroup[1].ID)

	dailyGroup := groups["0 0 0 * * *"]
	require.Len(t, dailyGroup, 1, "Daily group should have 1 directive")
	assert.Equal(t, "test3", dailyGroup[0].ID)
}

func TestCacheScheduler_ResolutionFlow(t *testing.T) {
	logger := log.New(log.WithWriter(nil))
	cacheDB := &internal.CacheDB{}

	scheduler := NewCacheScheduler(NewCacheSchedulerParams{
		Service:         &common.Service{},
		CacheDB:         cacheDB,
		EngineOps:       nil,
		Logger:          logger,
		MetricsRecorder: metrics.NewNoOpMetrics(),
		Namespace:       "",
	})
	scheduler.ctx, scheduler.cancel = context.WithCancel(context.Background())
	defer scheduler.cancel()

	// Create test directives with wildcard
	from := int64(1640995200)
	originalDirectives := []config.CacheDirective{
		{
			ID:           "test1",
			Type:         config.DirectiveProviderWildcard,
			DataProvider: "provider1",
			StreamID:     "*",
			Schedule:     config.Schedule{CronExpr: "0 0 * * * *"},
			TimeRange:    config.TimeRange{From: &from},
		},
	}

	// Set original directives
	scheduler.originalDirectives = originalDirectives

	// Verify resolution flow stores original directives
	assert.Len(t, scheduler.originalDirectives, 1)
	assert.Equal(t, config.DirectiveProviderWildcard, scheduler.originalDirectives[0].Type)

	// Test getDirectivesForSchedule
	scheduler.resolvedDirectives = []config.CacheDirective{
		{
			ID:           "resolved1",
			Type:         config.DirectiveSpecific,
			DataProvider: "provider1",
			StreamID:     "stream1",
			Schedule:     config.Schedule{CronExpr: "0 0 * * * *"},
			TimeRange:    config.TimeRange{From: &from},
		},
	}

	directives := scheduler.getDirectivesForSchedule("0 0 * * * *")
	assert.Len(t, directives, 1)
	assert.Equal(t, "stream1", directives[0].StreamID)
}
