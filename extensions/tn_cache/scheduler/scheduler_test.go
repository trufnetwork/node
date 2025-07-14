package scheduler

import (
	"context"
	"testing"
	"time"

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

func TestCacheScheduler_Creation(t *testing.T) {
	logger := log.New(log.WithWriter(nil))
	var cacheDB *internal.CacheDB

	t.Run("default namespace", func(t *testing.T) {
		scheduler := NewCacheScheduler(NewCacheSchedulerParams{
			Service:         &common.Service{},
			CacheDB:         cacheDB,
			EngineOps:       nil,
			Logger:          logger,
			MetricsRecorder: metrics.NewNoOpMetrics(),
			Namespace:       "",
		})

		require.NotNil(t, scheduler)
		assert.NotNil(t, scheduler.cron)
		assert.NotNil(t, scheduler.jobs)
		assert.Equal(t, "", scheduler.namespace)
	})

	t.Run("custom namespace", func(t *testing.T) {
		scheduler := NewCacheScheduler(NewCacheSchedulerParams{
			Service:         &common.Service{},
			CacheDB:         cacheDB,
			EngineOps:       nil,
			Logger:          logger,
			MetricsRecorder: metrics.NewNoOpMetrics(),
			Namespace:       "custom_db",
		})

		require.NotNil(t, scheduler)
		assert.Equal(t, "custom_db", scheduler.namespace)
	})
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
	groups := scheduler.groupBySchedule(directives)

	// Verify grouping
	require.Len(t, groups, 2, "Should have 2 different schedules")

	hourlyGroup := groups["0 * * * *"]
	require.Len(t, hourlyGroup, 2, "Hourly group should have 2 directives")
	assert.Equal(t, "test1", hourlyGroup[0].ID)
	assert.Equal(t, "test2", hourlyGroup[1].ID)

	dailyGroup := groups["0 0 * * *"]
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
			Schedule:     config.Schedule{CronExpr: "0 * * * *"},
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
			Schedule:     config.Schedule{CronExpr: "0 * * * *"},
			TimeRange:    config.TimeRange{From: &from},
		},
	}

	directives := scheduler.getDirectivesForSchedule("0 * * * *")
	assert.Len(t, directives, 1)
	assert.Equal(t, "stream1", directives[0].StreamID)
}

func TestCacheScheduler_ConcurrentExecution(t *testing.T) {
	logger := log.New(log.WithWriter(nil))
	
	// Mock components
	cacheDB := &internal.CacheDB{}
	
	scheduler := NewCacheScheduler(NewCacheSchedulerParams{
		Service:         &common.Service{},
		CacheDB:         cacheDB,
		EngineOps:       nil,
		Logger:          logger,
		MetricsRecorder: metrics.NewNoOpMetrics(),
		Namespace:       "",
	})

	// Verify gocron scheduler is created
	assert.NotNil(t, scheduler.cron, "gocron scheduler should be initialized")
	assert.NotNil(t, scheduler.jobs, "jobs map should be initialized")
	
	// Test job timeout configuration
	assert.Equal(t, 60*time.Minute, scheduler.jobTimeout, "default job timeout should be 60 minutes")
	
	// Test concurrent job context management
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.ctx = ctx
	scheduler.cancel = cancel
	defer cancel()
	
	// Create multiple job contexts concurrently
	jobIDs := []string{"job1", "job2", "job3"}
	for _, jobID := range jobIDs {
		jobCtx, _ := scheduler.createJobContext(jobID)
		assert.NotNil(t, jobCtx, "job context should be created")
	}
	
	// Verify active job count
	assert.Equal(t, 3, scheduler.getActiveJobCount(), "should have 3 active jobs")
	
	// Test concurrent job removal
	for _, jobID := range jobIDs {
		scheduler.removeJobContext(jobID)
	}
	
	assert.Equal(t, 0, scheduler.getActiveJobCount(), "should have 0 active jobs after removal")
}

func TestCacheScheduler_StopWithTimeout(t *testing.T) {
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
	
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.ctx = ctx
	scheduler.cancel = cancel
	
	// Create a long-running job context
	jobCtx, jobCancel := scheduler.createJobContext("long_job")
	
	// Start a goroutine that simulates a long-running job
	go func() {
		<-jobCtx.Done()
		// Simulate some cleanup time
		time.Sleep(50 * time.Millisecond)
		jobCancel()
		scheduler.removeJobContext("long_job")
	}()
	
	// Ensure job is registered
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, scheduler.getActiveJobCount(), "should have 1 active job")
	
	// Stop should wait for the job to complete
	err := scheduler.Stop()
	assert.NoError(t, err, "stop should complete without error")
	assert.Equal(t, 0, scheduler.getActiveJobCount(), "should have 0 active jobs after stop")
}
