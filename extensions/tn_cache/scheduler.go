package tn_cache

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/sony/gobreaker"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"golang.org/x/sync/errgroup"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/validation"
)

// CacheScheduler manages the scheduled refresh of cache data
type CacheScheduler struct {
	app       *common.App
	cacheDB   *internal.CacheDB
	logger    log.Logger
	cron      *cron.Cron
	jobs      map[string]cron.EntryID // schedule -> job ID
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	namespace string                               // configurable database namespace
	breakers  map[string]*gobreaker.CircuitBreaker // circuit breakers per stream
	breakerMu sync.RWMutex

	// Dynamic resolution fields
	originalDirectives []config.CacheDirective // Keep original with wildcards/includeChildren
	resolvedDirectives []config.CacheDirective // Current resolved state
	resolutionJob      cron.EntryID            // Cron job for resolution
	resolutionMu       sync.RWMutex            // Protect resolved state
	lastResolution     time.Time               // Track last resolution time
	resolutionStatus   ResolutionStatus        // Current resolution status
	resolutionErr      error                   // Last resolution error if any
}

// NewCacheScheduler creates a new cache scheduler instance
func NewCacheScheduler(app *common.App, cacheDB *internal.CacheDB, logger log.Logger) *CacheScheduler {
	return NewCacheSchedulerWithNamespace(app, cacheDB, logger, "")
}

// NewCacheSchedulerWithNamespace creates a new cache scheduler instance with configurable namespace
func NewCacheSchedulerWithNamespace(app *common.App, cacheDB *internal.CacheDB, logger log.Logger, namespace string) *CacheScheduler {
	if namespace == "" {
		namespace = "main" // Default namespace
	}

	return &CacheScheduler{
		app:              app,
		cacheDB:          cacheDB,
		logger:           logger.New("scheduler"),
		cron:             cron.New(cron.WithSeconds()),
		jobs:             make(map[string]cron.EntryID),
		namespace:        namespace,
		breakers:         make(map[string]*gobreaker.CircuitBreaker),
		resolutionStatus: ResolutionStatusPending,
	}
}

// Start initializes and starts the cache scheduler
func (s *CacheScheduler) Start(ctx context.Context, processedConfig *config.ProcessedConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("starting cache scheduler", "directives", len(processedConfig.Directives))

	// Create cancellable context
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Store original directives for future re-resolution
	s.originalDirectives = processedConfig.Directives

	// Perform initial resolution
	resolvedStreamSpecs, err := s.resolveStreamSpecs(s.ctx, s.originalDirectives)
	if err != nil {
		return fmt.Errorf("resolve stream specs: %w", err)
	}

	s.logger.Info("resolved stream specifications",
		"original_count", len(s.originalDirectives),
		"resolved_count", len(resolvedStreamSpecs))

	// Store resolved directives
	s.resolvedDirectives = resolvedStreamSpecs
	s.lastResolution = time.Now()

	// Store stream configurations in the database
	if err := s.storeStreamConfigs(s.ctx, resolvedStreamSpecs); err != nil {
		return fmt.Errorf("store stream configs: %w", err)
	}

	// Register resolution cron job if schedule is provided
	if processedConfig.ResolutionSchedule != "" {
		if err := s.registerResolutionJob(processedConfig.ResolutionSchedule); err != nil {
			return fmt.Errorf("register resolution job: %w", err)
		}
	}

	// Group resolved specifications by schedule for efficient batch processing
	scheduleGroups := s.groupBySchedule(resolvedStreamSpecs)

	// Register cron jobs for each schedule group
	for schedule, _ := range scheduleGroups {
		if err := s.registerRefreshJob(schedule); err != nil {
			return fmt.Errorf("register refresh job for schedule %s: %w", schedule, err)
		}
	}

	// Start the cron scheduler
	s.cron.Start()
	s.logger.Info("cache scheduler started", "jobs", len(s.jobs))

	// Run initial refresh for all resolved streams asynchronously
	go s.runInitialRefresh(resolvedStreamSpecs)

	return nil
}

// Stop gracefully shuts down the cache scheduler
func (s *CacheScheduler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("stopping cache scheduler")

	// Stop accepting new jobs and get a context that's done when all jobs complete
	cronCtx := s.cron.Stop()

	// Cancel context to signal running jobs to stop
	if s.cancel != nil {
		s.cancel()
	}

	// Wait for running jobs to complete with timeout
	select {
	case <-cronCtx.Done():
		s.logger.Info("all scheduled jobs completed")
	case <-time.After(30 * time.Second):
		s.logger.Warn("timeout waiting for jobs to complete")
	}

	s.logger.Info("cache scheduler stopped")
	return nil
}

// storeStreamConfigs persists the stream configurations to the database using batch operations
func (s *CacheScheduler) storeStreamConfigs(ctx context.Context, directives []config.CacheDirective) error {
	if len(directives) == 0 {
		return nil
	}

	// Convert directives to stream configs
	var configs []internal.StreamCacheConfig
	for _, directive := range directives {
		// Get the from timestamp, defaulting to 0 if not set
		var fromTimestamp int64
		if directive.TimeRange.From != nil {
			fromTimestamp = *directive.TimeRange.From
		}

		streamConfig := internal.StreamCacheConfig{
			DataProvider:  directive.DataProvider,
			StreamID:      directive.StreamID,
			FromTimestamp: fromTimestamp,
			LastRefreshed: "", // Will be set on first refresh
			CronSchedule:  directive.Schedule.CronExpr,
		}
		configs = append(configs, streamConfig)
	}

	// Use batch insert for efficiency
	if err := s.cacheDB.AddStreamConfigs(ctx, configs); err != nil {
		return fmt.Errorf("failed to add stream configs: %w", err)
	}

	return nil
}

// groupBySchedule groups directives by their cron schedule for batch processing
func (s *CacheScheduler) groupBySchedule(directives []config.CacheDirective) map[string][]config.CacheDirective {
	scheduleGroups := make(map[string][]config.CacheDirective)
	for _, directive := range directives {
		schedule := directive.Schedule.CronExpr
		scheduleGroups[schedule] = append(scheduleGroups[schedule], directive)
	}
	return scheduleGroups
}

// runInitialRefresh performs an initial refresh of all streams with concurrency control
func (s *CacheScheduler) runInitialRefresh(directives []config.CacheDirective) {
	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in initial refresh",
				"panic", r,
				"stack", string(debug.Stack()))
		}
	}()

	s.logger.Info("starting initial refresh", "streams", len(directives))

	// Use errgroup for structured concurrency
	g, ctx := errgroup.WithContext(s.ctx)
	g.SetLimit(5) // Maximum 5 concurrent operations

	for _, directive := range directives {
		dir := directive // Capture loop variable

		g.Go(func() error {
			// Panic recovery is handled by errgroup
			if err := s.refreshStreamDataWithCircuitBreaker(ctx, dir); err != nil {
				// Log error but don't fail the entire group
				s.logger.Error("failed to perform initial refresh of stream data",
					"provider", dir.DataProvider,
					"stream", dir.StreamID,
					"error", err)
			}
			return nil // Return nil to continue with other streams
		})
	}

	if err := g.Wait(); err != nil {
		s.logger.Error("initial refresh group error", "error", err)
	}

	s.logger.Info("initial refresh completed")
}

// registerRefreshJob registers a cron job for refreshing streams on a schedule
func (s *CacheScheduler) registerRefreshJob(schedule string) error {
	// Validate the cron schedule
	if err := validation.ValidateCronSchedule(schedule); err != nil {
		return fmt.Errorf("invalid refresh schedule %s: %w", schedule, err)
	}

	// Create job function that reads current resolved directives
	jobFunc := func() {
		// Add panic recovery
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("panic in refresh job",
					"panic", r,
					"schedule", schedule,
					"stack", string(debug.Stack()))
			}
		}()

		// Get current resolved directives for this schedule
		directives := s.getDirectivesForSchedule(schedule)
		if len(directives) == 0 {
			s.logger.Debug("no directives for schedule", "schedule", schedule)
			return
		}

		s.logger.Debug("executing scheduled refresh", "schedule", schedule, "streams", len(directives))

		// Use errgroup for better error handling
		g, ctx := errgroup.WithContext(s.ctx)
		g.SetLimit(5) // Limit concurrent refreshes

		for _, directive := range directives {
			dir := directive // Capture loop variable

			g.Go(func() error {
				if err := s.refreshStreamDataWithCircuitBreaker(ctx, dir); err != nil {
					s.logger.Error("failed to refresh stream data",
						"provider", dir.DataProvider,
						"stream", dir.StreamID,
						"type", dir.Type,
						"error", err)
				}
				return nil // Continue with other streams
			})
		}

		if err := g.Wait(); err != nil {
			s.logger.Error("scheduled refresh group error", "error", err)
		}
	}

	// Register the cron job
	entryID, err := s.cron.AddFunc(schedule, jobFunc)
	if err != nil {
		return fmt.Errorf("add refresh cron job: %w", err)
	}

	s.jobs[schedule] = entryID
	s.logger.Info("registered refresh cron job",
		"schedule", schedule)

	return nil
}