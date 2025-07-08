package tn_cache

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
	"github.com/trufnetwork/node/extensions/tn_cache/validation"
)

// ResolutionJobKey avoids collision with cron expression keys in jobs map
const ResolutionJobKey = "__resolution__"

// CacheScheduler orchestrates periodic data fetching from external providers.
// It resolves wildcards/patterns to concrete streams and manages their refresh schedules.
type CacheScheduler struct {
	app       *common.App
	cacheDB   *internal.CacheDB
	logger    log.Logger
	cron      *cron.Cron
	jobs      map[string]cron.EntryID // schedule -> job ID
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	namespace string                  // database namespace to query (default: "main")
	metrics   metrics.MetricsRecorder

	// Resolution state - wildcards expand to concrete streams dynamically
	originalDirectives []config.CacheDirective // Preserved for re-resolution when new streams appear
	resolvedDirectives []config.CacheDirective // Concrete streams that jobs operate on
	resolutionMu       sync.RWMutex            // Guards resolvedDirectives during re-resolution

	// Job lifecycle - prevents runaway jobs and enables graceful shutdown
	jobContexts   map[string]context.CancelFunc // Active job cancellation registry
	jobContextsMu sync.RWMutex
	jobTimeout    time.Duration // Safety limit per job (default: 60m)
}


// NewCacheScheduler creates a scheduler with default "main" namespace
func NewCacheScheduler(app *common.App, cacheDB *internal.CacheDB, logger log.Logger, metricsRecorder metrics.MetricsRecorder) *CacheScheduler {
	return NewCacheSchedulerWithNamespace(app, cacheDB, logger, "", metricsRecorder)
}

// NewCacheSchedulerWithNamespace allows targeting specific database namespaces
func NewCacheSchedulerWithNamespace(app *common.App, cacheDB *internal.CacheDB, logger log.Logger, namespace string, metricsRecorder metrics.MetricsRecorder) *CacheScheduler {
	if namespace == "" {
		namespace = "main" // Default namespace
	}

	return &CacheScheduler{
		app:         app,
		cacheDB:     cacheDB,
		logger:      logger.New("scheduler"),
		cron:        cron.New(cron.WithSeconds()),
		jobs:        make(map[string]cron.EntryID),
		namespace:   namespace,
		metrics:     metricsRecorder,
		jobContexts: make(map[string]context.CancelFunc),
		jobTimeout:  60 * time.Minute, // Default job timeout
	}
}

// createJobContext ensures jobs can't run forever and are cancellable on shutdown
func (s *CacheScheduler) createJobContext(jobID string) (context.Context, context.CancelFunc) {
	s.jobContextsMu.Lock()
	defer s.jobContextsMu.Unlock()

	// Inherits cancellation from scheduler shutdown
	ctx, cancel := context.WithTimeout(s.ctx, s.jobTimeout)

	s.jobContexts[jobID] = cancel

	return ctx, cancel
}

// removeJobContext cleans up completed jobs to prevent memory leaks
func (s *CacheScheduler) removeJobContext(jobID string) {
	s.jobContextsMu.Lock()
	defer s.jobContextsMu.Unlock()

	if cancel, exists := s.jobContexts[jobID]; exists {
		// Defensive: job might have self-cancelled
		cancel()
		delete(s.jobContexts, jobID)
	}
}

// cancelAllJobContexts triggers graceful shutdown of running jobs
func (s *CacheScheduler) cancelAllJobContexts() {
	s.jobContextsMu.Lock()
	defer s.jobContextsMu.Unlock()

	for jobID, cancel := range s.jobContexts {
		cancel()
		s.logger.Info("cancelled job context", "job_id", jobID)
	}

	s.jobContexts = make(map[string]context.CancelFunc)
}

// getActiveJobCount helps monitor job backlog during shutdown
func (s *CacheScheduler) getActiveJobCount() int {
	s.jobContextsMu.RLock()
	defer s.jobContextsMu.RUnlock()
	return len(s.jobContexts)
}

// shouldSkipInitialRefresh checks if we should skip refresh on startup based on last refresh time
// Returns true if the stream was already refreshed within the current cron period
func (s *CacheScheduler) shouldSkipInitialRefresh(lastRefreshed string, cronSchedule string) bool {
	if lastRefreshed == "" {
		// Never refreshed before, don't skip
		return false
	}

	lastTime, err := time.Parse(time.RFC3339, lastRefreshed)
	if err != nil {
		s.logger.Warn("invalid last refresh time", "time", lastRefreshed, "error", err)
		return false
	}

	// Parse cron expression using the same parser as the scheduler
	parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(cronSchedule)
	if err != nil {
		s.logger.Warn("invalid cron expression", "schedule", cronSchedule, "error", err)
		return false
	}

	now := time.Now()
	
	// Calculate when the next refresh would be based on the cron schedule
	nextScheduled := schedule.Next(lastTime)
	
	// If the next scheduled time is still in the future (or equal), we're within the current period
	if !nextScheduled.Before(now) {
		s.logger.Debug("skipping initial refresh - already refreshed in current period",
			"last_refresh", lastTime.Format(time.RFC3339),
			"next_scheduled", nextScheduled.Format(time.RFC3339),
			"now", now.Format(time.RFC3339))
		return true
	}

	return false
}

// createExtensionEngineContext enables the extension to query databases
// as a privileged system agent, bypassing normal authorization
func (s *CacheScheduler) createExtensionEngineContext(ctx context.Context) *common.EngineContext {
	// Create a valid block context
	// In a real implementation, you might want to get this from the chain
	blockCtx := &common.BlockContext{
		Height:    -1, // System operations don't need a specific height
		Timestamp: time.Now().Unix(),
		ChainContext: &common.ChainContext{
			ChainID:           s.app.Service.GenesisConfig.ChainID,
			NetworkParameters: &common.NetworkParameters{},
			MigrationParams:   &common.MigrationContext{},
		},
	}

	// Create the engine context with the extension as the caller
	return &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx:           ctx,
			BlockContext:  blockCtx,
			TxID:          fmt.Sprintf("tn_cache_%d", time.Now().UnixNano()),
			Signer:        []byte(internal.ExtensionAgentName),
			Caller:        internal.ExtensionAgentName, // Required for transaction context validity
			Authenticator: "system",
		},
		OverrideAuthz: true,  // System agent privileges
		InvalidTxCtx:  false, // Enables access to block height, timestamps, etc.
	}
}

// Start begins periodic cache refresh. Must be called before Stop.
func (s *CacheScheduler) Start(ctx context.Context, processedConfig *config.ProcessedConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("starting cache scheduler", "directives", len(processedConfig.Directives))

	s.ctx, s.cancel = context.WithCancel(ctx)

	// Preserved to detect new streams matching wildcards later
	s.originalDirectives = processedConfig.Directives

	// Expand wildcards/patterns to actual stream IDs
	resolvedStreamSpecs, err := s.resolveStreamSpecs(s.ctx, s.originalDirectives)
	if err != nil {
		return fmt.Errorf("resolve stream specs: %w", err)
	}

	s.logger.Info("resolved stream specifications",
		"original_count", len(s.originalDirectives),
		"resolved_count", len(resolvedStreamSpecs))

	s.resolvedDirectives = resolvedStreamSpecs

	// Database tracks what we're caching and when last refreshed
	if err := s.storeStreamConfigs(s.ctx, resolvedStreamSpecs); err != nil {
		return fmt.Errorf("store stream configs: %w", err)
	}

	// Re-resolution detects new streams matching patterns
	if processedConfig.ResolutionSchedule != "" {
		if err := s.registerResolutionJob(processedConfig.ResolutionSchedule); err != nil {
			return fmt.Errorf("register resolution job: %w", err)
		}
	}

	// Batch streams with same schedule into single cron job
	scheduleGroups := s.groupBySchedule(resolvedStreamSpecs)

	for schedule := range scheduleGroups {
		if err := s.registerRefreshJob(schedule); err != nil {
			return fmt.Errorf("register refresh job for schedule %s: %w", schedule, err)
		}
	}

	s.cron.Start()
	s.logger.Info("cache scheduler started", "jobs", len(s.jobs))

	// Populate cache immediately instead of waiting for first cron trigger
	go s.runInitialRefresh(resolvedStreamSpecs)

	return nil
}

// Stop waits for active jobs to complete (up to 30s timeout)
func (s *CacheScheduler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("stopping cache scheduler",
		"active_jobs", s.getActiveJobCount())

	// Cancel all active job contexts first
	s.cancelAllJobContexts()

	// Stop accepting new jobs and get a context that's done when all jobs complete
	cronCtx := s.cron.Stop()

	if s.cancel != nil {
		s.cancel()
	}

	select {
	case <-cronCtx.Done():
		s.logger.Info("all scheduled jobs completed")
	case <-time.After(30 * time.Second):
		s.logger.Warn("timeout waiting for jobs to complete",
			"remaining_jobs", s.getActiveJobCount())
	}

	s.logger.Info("cache scheduler stopped")
	return nil
}

// storeStreamConfigs persists the stream configurations to the database using batch operations
func (s *CacheScheduler) storeStreamConfigs(ctx context.Context, directives []config.CacheDirective) error {
	if len(directives) == 0 {
		return nil
	}

	var configs []internal.StreamCacheConfig
	// Build configuration objects for each directive
	for _, directive := range directives {
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

// runInitialRefresh performs initial refresh only for streams that haven't been refreshed
// in the current cron period, preventing duplicate refreshes after restarts
func (s *CacheScheduler) runInitialRefresh(directives []config.CacheDirective) {
	// Generate unique job ID for initial refresh
	jobID := fmt.Sprintf("initial_refresh_%d", time.Now().UnixNano())

	// Create job-specific context for initial refresh
	jobCtx, jobCancel := s.createJobContext(jobID)

	defer func() {
		// Always remove job context when done
		s.removeJobContext(jobID)

		if r := recover(); r != nil {
			s.logger.Error("panic in initial refresh",
				"panic", r,
				"job_id", jobID,
				"stack", string(debug.Stack()))
		}
	}()

	// First, get all stream configs to check last refresh times
	streamConfigs, err := s.cacheDB.ListStreamConfigs(jobCtx)
	if err != nil {
		s.logger.Error("failed to get stream configs for initial refresh check",
			"error", err,
			"job_id", jobID)
		// Continue anyway - better to refresh than to skip
		streamConfigs = []internal.StreamCacheConfig{}
	}

	// Build a map for quick lookup
	configMap := make(map[string]internal.StreamCacheConfig)
	for _, config := range streamConfigs {
		key := fmt.Sprintf("%s:%s", config.DataProvider, config.StreamID)
		configMap[key] = config
	}

	// Filter directives to only those that need refresh
	var needsRefresh []config.CacheDirective
	var skipped int
	
	for _, directive := range directives {
		key := fmt.Sprintf("%s:%s", directive.DataProvider, directive.StreamID)
		if config, exists := configMap[key]; exists {
			if s.shouldSkipInitialRefresh(config.LastRefreshed, directive.Schedule.CronExpr) {
				skipped++
				continue
			}
		}
		needsRefresh = append(needsRefresh, directive)
	}

	s.logger.Info("starting initial refresh",
		"total_streams", len(directives),
		"needs_refresh", len(needsRefresh),
		"skipped", skipped,
		"job_id", jobID)

	if len(needsRefresh) == 0 {
		s.logger.Info("no streams need initial refresh", "job_id", jobID)
		jobCancel()
		return
	}

	g, ctx := errgroup.WithContext(jobCtx)
	g.SetLimit(5) // Maximum 5 concurrent operations

	for _, directive := range needsRefresh {
		dir := directive // Capture loop variable to avoid closure issues

		g.Go(func() error {
			// Check context before starting work
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err := s.refreshStreamDataWithRetry(ctx, dir, 3); err != nil {
				// Log error but don't fail the entire group
				s.logger.Error("failed to perform initial refresh of stream data",
					"provider", dir.DataProvider,
					"stream", dir.StreamID,
					"job_id", jobID,
					"error", err)
			}
			return nil // Return nil to continue with other streams
		})
	}

	if err := g.Wait(); err != nil {
		s.logger.Error("initial refresh group error",
			"error", err,
			"job_id", jobID)
	}

	// Cancel job context explicitly when done
	jobCancel()

	s.logger.Info("initial refresh completed",
		"job_id", jobID)
}

// registerRefreshJob registers a cron job for refreshing streams on a schedule
func (s *CacheScheduler) registerRefreshJob(schedule string) error {
	if err := validation.ValidateCronSchedule(schedule); err != nil {
		return fmt.Errorf("invalid refresh schedule %s: %w", schedule, err)
	}

	jobFunc := func() {
		// Generate unique job ID for this execution
		jobID := fmt.Sprintf("%s_%d", schedule, time.Now().UnixNano())

		// Create job-specific context with timeout
		jobCtx, jobCancel := s.createJobContext(jobID)
		defer func() {
			// Always remove job context when done
			s.removeJobContext(jobID)
		}()

		// Add tracing for scheduled job execution
		ctx, end := tracing.SchedulerOperation(jobCtx, tracing.OpSchedulerJob,
			attribute.String("schedule", schedule),
			attribute.String("job_id", jobID))
		defer func() {
			end(nil) // Job errors are logged separately, not returned
			if r := recover(); r != nil {
				s.logger.Error("panic in refresh job",
					"panic", r,
					"schedule", schedule,
					"job_id", jobID,
					"stack", string(debug.Stack()))
			}
		}()

		// Check if context is already cancelled
		select {
		case <-jobCtx.Done():
			s.logger.Warn("job context cancelled before execution",
				"job_id", jobID,
				"schedule", schedule)
			return
		default:
		}

		directives := s.getDirectivesForSchedule(schedule)
		if len(directives) == 0 {
			s.logger.Debug("no directives for schedule", "schedule", schedule)
			return
		}

		s.logger.Debug("executing scheduled refresh",
			"schedule", schedule,
			"streams", len(directives),
			"job_id", jobID,
			"active_jobs", s.getActiveJobCount())

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(5) // Limit concurrent refreshes

		for _, directive := range directives {
			dir := directive // Capture loop variable

			g.Go(func() error {
				// Check context before starting work
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				default:
				}

				if err := s.refreshStreamDataWithRetry(gCtx, dir, 3); err != nil {
					s.logger.Error("failed to refresh stream data",
						"provider", dir.DataProvider,
						"stream", dir.StreamID,
						"type", dir.Type,
						"job_id", jobID,
						"error", err)
				}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			s.logger.Error("scheduled refresh group error",
				"error", err,
				"job_id", jobID)
		}

		// Cancel job context explicitly when done
		jobCancel()
	}

	entryID, err := s.cron.AddFunc(schedule, jobFunc)
	if err != nil {
		return fmt.Errorf("add refresh cron job: %w", err)
	}

	s.jobs[schedule] = entryID
	s.logger.Info("registered refresh cron job",
		"schedule", schedule)

	return nil
}

// updateGaugeMetrics updates the gauge metrics for active streams and total cached events
func (s *CacheScheduler) updateGaugeMetrics(ctx context.Context) {
	// Query active streams and event counts
	activeStreams := 0
	totalEvents := int64(0)
	
	rows, err := s.cacheDB.QueryCachedStreamsWithCounts(ctx)
	if err != nil {
		s.logger.Warn("failed to query cached streams for metrics", "error", err)
		return
	}
	
	// Clear previous event counts
	// Record new counts per stream
	for _, row := range rows {
		if row.EventCount > 0 {
			activeStreams++
			totalEvents += row.EventCount
			// Update per-stream event count
			s.metrics.RecordCacheSize(ctx, row.DataProvider, row.StreamID, row.EventCount)
		}
	}
	
	// Update active streams gauge
	s.metrics.RecordStreamActive(ctx, activeStreams)
	
	s.logger.Debug("updated gauge metrics",
		"active_streams", activeStreams,
		"total_events", totalEvents)
}
