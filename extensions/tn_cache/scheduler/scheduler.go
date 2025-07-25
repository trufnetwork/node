package scheduler

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/robfig/cron/v3"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/node/types/sql"
	"go.opentelemetry.io/otel/attribute"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
	"github.com/trufnetwork/node/extensions/tn_cache/syncschecker"
	"github.com/trufnetwork/node/extensions/tn_cache/validation"
)

// ResolutionJobKey avoids collision with cron expression keys in jobs map
const ResolutionJobKey = "__resolution__"

// CacheScheduler orchestrates periodic data fetching from external providers.
// It resolves wildcards/patterns to concrete streams and manages their refresh schedules.
type CacheScheduler struct {
	kwilService      *common.Service
	cacheDB          *internal.CacheDB
	engineOperations *internal.EngineOperations
	logger           log.Logger
	cron             *gocron.Scheduler
	jobs             map[string]*gocron.Job
	mu               sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
	namespace        string // database namespace to query (default: "main")
	metrics          metrics.MetricsRecorder

	// Resolution state - wildcards expand to concrete streams dynamically
	originalDirectives []config.CacheDirective // Preserved for re-resolution when new streams appear
	resolvedDirectives []config.CacheDirective // Concrete streams that jobs operate on
	resolutionMu       sync.RWMutex            // Guards resolvedDirectives during re-resolution

	// Job lifecycle - prevents runaway jobs and enables graceful shutdown
	jobContexts   map[string]context.CancelFunc // Active job cancellation registry
	jobContextsMu sync.RWMutex
	jobTimeout    time.Duration // Safety limit per job (default: 60m)

	// Sync-aware caching
	syncChecker *syncschecker.SyncChecker // Monitors node sync status
}

// GetResolvedDirectives returns the current resolved directives
// This is used in tests to verify resolution results
func (s *CacheScheduler) GetResolvedDirectives() []config.CacheDirective {
	s.resolutionMu.RLock()
	defer s.resolutionMu.RUnlock()

	// Return a copy to avoid data races
	result := make([]config.CacheDirective, len(s.resolvedDirectives))
	copy(result, s.resolvedDirectives)
	return result
}

// canRefresh checks if a refresh operation should proceed
func (s *CacheScheduler) canRefresh(provider, streamID string) bool {
	if s.syncChecker == nil {
		return true
	}

	canExecute, reason := s.syncChecker.CanExecute()
	if !canExecute {
		s.logger.Debug("skipping refresh due to sync status",
			"provider", provider,
			"stream", streamID,
			"reason", reason)
		// Record the skip metric
		s.metrics.RecordRefreshSkipped(s.ctx, provider, streamID, reason)
	}
	return canExecute
}

type NewCacheSchedulerParams struct {
	Service         *common.Service
	CacheDB         *internal.CacheDB
	EngineOps       *internal.EngineOperations
	Logger          log.Logger
	MetricsRecorder metrics.MetricsRecorder
	Namespace       string
	SyncChecker     *syncschecker.SyncChecker
}

// NewCacheSchedulerWithNamespace allows targeting specific database namespaces
func NewCacheScheduler(params NewCacheSchedulerParams) *CacheScheduler {
	// Keep namespace as-is, including empty string for global namespace
	if params.Namespace == "" {
		params.Logger.New("scheduler").Debug("using global namespace (empty string)")
	} else {
		params.Logger.New("scheduler").Debug("using namespace", "namespace", params.Namespace)
	}

	return &CacheScheduler{
		kwilService:      params.Service,
		cacheDB:          params.CacheDB,
		engineOperations: params.EngineOps,
		logger:           params.Logger.New("scheduler"),
		cron:             gocron.NewScheduler(time.UTC),
		jobs:             make(map[string]*gocron.Job),
		namespace:        params.Namespace,
		metrics:          params.MetricsRecorder,
		jobContexts:      make(map[string]context.CancelFunc),
		jobTimeout:       60 * time.Minute, // Default job timeout
		syncChecker:      params.SyncChecker,
	}
}

// createJobContext ensures jobs can't run forever and are cancellable on shutdown
func (s *CacheScheduler) createJobContext(jobID string) (context.Context, context.CancelFunc) {
	s.jobContextsMu.Lock()
	defer s.jobContextsMu.Unlock()

	// Inherits cancellation from scheduler shutdown
	ctx, cancel := context.WithTimeout(s.ctx, s.jobTimeout)

	// Monitor for timeout in a separate goroutine
	go func() {
		<-ctx.Done()
		// Only log if it was actually a timeout, not a cancellation
		if ctx.Err() == context.DeadlineExceeded {
			s.logger.Warn("job timeout exceeded",
				"job_id", jobID,
				"timeout", s.jobTimeout)
		}
		// Exit goroutine on any context completion
	}()

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

	jobCount := len(s.jobContexts)
	for _, cancel := range s.jobContexts {
		cancel()
	}

	if jobCount > 0 {
		s.logger.Info("cancelled all job contexts", "count", jobCount)
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
func (s *CacheScheduler) shouldSkipInitialRefresh(lastRefreshed int64, cronSchedule string) bool {
	if lastRefreshed == 0 {
		// Never refreshed before, don't skip
		return false
	}

	lastTime := time.Unix(lastRefreshed, 0)

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
		// In test environments, initial resolution might fail if actions aren't available yet
		// This is OK - resolution will be retried when TriggerStreamResolution is called
		s.logger.Warn("initial stream resolution failed - will retry later", "error", err)
		resolvedStreamSpecs = []config.CacheDirective{}
	}

	s.logger.Info("resolved stream specifications",
		"original_count", len(s.originalDirectives),
		"resolved_count", len(resolvedStreamSpecs))

	s.resolvedDirectives = resolvedStreamSpecs

	// Database tracks what we're caching and when last refreshed
	if err := s.storeStreamConfigs(s.ctx, resolvedStreamSpecs); err != nil {
		return fmt.Errorf("store stream configs: %w", err)
	}

	// Start asynchronous initialization once node is synced
	go s.waitUntilSyncedAndStart(processedConfig, resolvedStreamSpecs)

	s.logger.Info("scheduler initialization deferred until node is synced")

	return nil
}

// Stop waits for active jobs to complete (up to 30s timeout)
func (s *CacheScheduler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("stopping cache scheduler",
		"active_jobs", s.getActiveJobCount())

	// Stop accepting new jobs
	s.cron.Stop()

	// Cancel scheduler context to signal shutdown
	if s.cancel != nil {
		s.cancel()
	}

	// Wait for active jobs with timeout
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer waitCancel()

	// Poll for active jobs to complete
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			// Timeout - force cancel remaining jobs
			activeCount := s.getActiveJobCount()
			if activeCount > 0 {
				s.logger.Warn("timeout waiting for jobs to complete, force cancelling",
					"remaining_jobs", activeCount)
				s.cancelAllJobContexts()
			}
			s.logger.Info("cache scheduler stopped (timeout)")
			return nil
		case <-ticker.C:
			if s.getActiveJobCount() == 0 {
				s.logger.Info("all jobs completed, cache scheduler stopped")
				return nil
			}
		}
	}
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
			DataProvider:              directive.DataProvider,
			StreamID:                  directive.StreamID,
			FromTimestamp:             fromTimestamp,
			CacheRefreshedAtTimestamp: 0, // Will be set on first refresh
			CacheHeight:               0, // Will be set on first refresh
			CronSchedule:              directive.Schedule.CronExpr,
		}
		configs = append(configs, streamConfig)
	}

	// Use batch insert for efficiency
	if err := s.cacheDB.AddStreamConfigs(ctx, configs); err != nil {
		return fmt.Errorf("failed to add stream configs: %w", err)
	}

	return nil
}

// TriggerResolution manually triggers the global resolution process
// This is useful for tests or when streams need to be re-discovered immediately
func (s *CacheScheduler) TriggerResolution(ctx context.Context) error {
	return s.performGlobalResolution(ctx)
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
		s.logger.Warn("failed to get stream configs for initial refresh check",
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
			if s.shouldSkipInitialRefresh(config.CacheRefreshedAtTimestamp, directive.Schedule.CronExpr) {
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

	// Track refresh results
	var (
		successCount  int32
		failureCount  int32
		skippedCount  int32
		failedStreams []string
		mu            sync.Mutex
	)

	// Use worker pool pattern for concurrent refresh
	const workerCount = 5
	workChan := make(chan config.CacheDirective, len(needsRefresh))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for directive := range workChan {
				// Check context before starting work
				select {
				case <-jobCtx.Done():
					return
				default:
				}

				// Check sync status
				if !s.canRefresh(directive.DataProvider, directive.StreamID) {
					atomic.AddInt32(&skippedCount, 1)
					s.logger.Debug("skipping refresh - sync check failed",
						"provider", directive.DataProvider,
						"stream", directive.StreamID,
						"worker_id", workerID)
					continue
				}

				s.logger.Debug("starting refresh for stream",
					"provider", directive.DataProvider,
					"stream", directive.StreamID,
					"job_id", jobID,
					"worker_id", workerID)

				if err := s.refreshStreamDataWithRetry(jobCtx, directive, 3); err != nil {
					atomic.AddInt32(&failureCount, 1)
					mu.Lock()
					failedStreams = append(failedStreams, fmt.Sprintf("%s:%s", directive.DataProvider, directive.StreamID))
					mu.Unlock()
					// Log error but don't fail the entire group
					s.logger.Error("failed to perform initial refresh of stream data",
						"provider", directive.DataProvider,
						"stream", directive.StreamID,
						"job_id", jobID,
						"worker_id", workerID,
						"error", err)
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}(i)
	}

	// Queue work items
	for _, directive := range needsRefresh {
		workChan <- directive
	}
	close(workChan)

	// Wait for all workers to complete
	wg.Wait()

	// Cancel job context explicitly when done
	jobCancel()

	// Log summary of refresh results
	s.logger.Info("initial refresh completed",
		"job_id", jobID,
		"success", successCount,
		"failed", failureCount,
		"skipped", skippedCount)

	if failureCount > 0 {
		s.logger.Warn("some streams failed to refresh",
			"failed_streams", failedStreams,
			"count", failureCount)
	}
}

// RunFullRefreshSync performs a synchronous full refresh: resolution + data refresh for all streams
// Returns the total number of records cached across all streams after refresh
func (s *CacheScheduler) RunFullRefreshSync(ctx context.Context) (int, error) {
	s.resolutionMu.RLock()
	directives := make([]config.CacheDirective, len(s.resolvedDirectives))
	copy(directives, s.resolvedDirectives)
	s.resolutionMu.RUnlock()

	// First, perform resolution to pick up any new streams
	if err := s.performGlobalResolution(ctx); err != nil {
		return 0, fmt.Errorf("resolution failed: %w", err)
	}

	// Generate unique job ID
	jobID := fmt.Sprintf("full_refresh_%d", time.Now().UnixNano())

	// Create job context
	jobCtx, jobCancel := s.createJobContext(jobID)
	defer s.removeJobContext(jobID)

	// Get current configs for skip checks
	streamConfigs, err := s.cacheDB.ListStreamConfigs(jobCtx)
	if err != nil {
		s.logger.Warn("failed to get stream configs for full refresh", "error", err)
		streamConfigs = []internal.StreamCacheConfig{}
	}

	configMap := make(map[string]internal.StreamCacheConfig)
	for _, config := range streamConfigs {
		key := fmt.Sprintf("%s:%s", config.DataProvider, config.StreamID)
		configMap[key] = config
	}

	var needsRefresh []config.CacheDirective
	for _, dir := range directives {
		key := fmt.Sprintf("%s:%s", dir.DataProvider, dir.StreamID)
		if conf, exists := configMap[key]; exists {
			if s.shouldSkipInitialRefresh(conf.CacheRefreshedAtTimestamp, dir.Schedule.CronExpr) {
				continue
			}
		}
		needsRefresh = append(needsRefresh, dir)
	}

	if len(needsRefresh) == 0 {
		s.logger.Info("no streams need refresh")
		jobCancel()
		return s.getTotalCachedRecords(jobCtx)
	}

	// Use worker pool for concurrent refresh
	const workerCount = 5
	workChan := make(chan config.CacheDirective, len(needsRefresh))
	errChan := make(chan error, len(needsRefresh))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range workChan {
				if !s.canRefresh(dir.DataProvider, dir.StreamID) {
					continue
				}
				if err := s.refreshStreamDataWithRetry(jobCtx, dir, 3); err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	// Queue work items
	for _, dir := range needsRefresh {
		workChan <- dir
	}
	close(workChan)

	// Wait for all workers to complete
	wg.Wait()
	close(errChan)

	// Check for errors
	if err := <-errChan; err != nil {
		jobCancel()
		return 0, err
	}

	jobCancel()
	return s.getTotalCachedRecords(jobCtx)
}

// Helper to get total cached records
func (s *CacheScheduler) getTotalCachedRecords(ctx context.Context) (int, error) {
	infos, err := s.cacheDB.QueryCachedStreamsWithCounts(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, info := range infos {
		total += int(info.EventCount)
	}
	return total, nil
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

		// Use middleware for tracing
		_, _ = tracing.TracedSchedulerOperation(jobCtx, tracing.OpSchedulerJob,
			func(traceCtx context.Context) (any, error) {
				defer func() {
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
				case <-traceCtx.Done():
					s.logger.Warn("job context cancelled before execution",
						"job_id", jobID,
						"schedule", schedule)
					return nil, nil
				default:
				}

				directives := s.getDirectivesForSchedule(schedule)
				if len(directives) == 0 {
					s.logger.Debug("no directives for schedule", "schedule", schedule)
					return nil, nil
				}

				s.logger.Debug("executing scheduled refresh",
					"schedule", schedule,
					"streams", len(directives),
					"job_id", jobID,
					"active_jobs", s.getActiveJobCount())

				// Use worker pool for concurrent refresh in scheduled jobs too
				const workerCount = 5
				workChan := make(chan config.CacheDirective, len(directives))
				var wg sync.WaitGroup

				// Start workers
				for i := 0; i < workerCount; i++ {
					wg.Add(1)
					go func(workerID int) {
						defer wg.Done()
						for directive := range workChan {
							// Check context before starting work
							select {
							case <-jobCtx.Done():
								return
							default:
							}

							// Check sync status
							if !s.canRefresh(directive.DataProvider, directive.StreamID) {
								continue
							}

							if err := s.refreshStreamDataWithRetry(jobCtx, directive, 3); err != nil {
								s.logger.Error("failed to refresh stream data",
									"provider", directive.DataProvider,
									"stream", directive.StreamID,
									"type", directive.Type,
									"job_id", jobID,
									"worker_id", workerID,
									"error", err)
							}
						}
					}(i)
				}

				// Queue work items
				for _, directive := range directives {
					workChan <- directive
				}
				close(workChan)

				// Wait for all workers to complete
				wg.Wait()

				// Cancel job context explicitly when done
				jobCancel()
				return nil, nil
			}, attribute.String("schedule", schedule), attribute.String("job_id", jobID))
	}

	job, err := s.cron.Cron(schedule).Do(jobFunc)
	if err != nil {
		return fmt.Errorf("failed to register refresh job: %w", err)
	}

	s.jobs[schedule] = job
	s.logger.Info("registered refresh cron job",
		"schedule", schedule)

	return nil
}

// UpdateGaugeMetrics updates the gauge metrics for active streams and total cached events
func (s *CacheScheduler) UpdateGaugeMetrics(ctx context.Context) {
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
}

func (s *CacheScheduler) SetTx(tx sql.DB) {
	s.cacheDB.SetTx(tx)
}

// GetCurrentDirectives returns the current directives
func (s *CacheScheduler) GetCurrentDirectives() []config.CacheDirective {
	s.resolutionMu.RLock()
	defer s.resolutionMu.RUnlock()
	return s.resolvedDirectives
}

// waitUntilSyncedAndStart waits until SyncChecker (or absence thereof) allows execution, then performs initial refresh,
// registers cron jobs, and starts the scheduler.
func (s *CacheScheduler) waitUntilSyncedAndStart(procCfg *config.ProcessedConfig, directives []config.CacheDirective) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.syncChecker == nil {
				// Sync checking disabled – proceed immediately
				s.startJobs(procCfg, directives)
				return
			}
			s.syncChecker.Start(s.ctx)
			if ok, _ := s.syncChecker.CanExecute(); ok {
				s.startJobs(procCfg, directives)
				return
			}
		}
	}
}

// startJobs runs initial refresh, registers resolution & refresh cron jobs, then starts gocron.
func (s *CacheScheduler) startJobs(procCfg *config.ProcessedConfig, directives []config.CacheDirective) {
	s.logger.Info("node synced – starting cache scheduler jobs")

	// Run initial refresh (respecting skip logic)
	s.runInitialRefresh(directives)

	// Register resolution job if configured
	if procCfg.ResolutionSchedule != "" {
		if err := s.registerResolutionJob(procCfg.ResolutionSchedule); err != nil {
			s.logger.Warn("failed to register resolution job", "error", err)
		}
	}

	// Register refresh cron jobs per schedule
	scheduleGroups := s.groupBySchedule(directives)
	for schedule := range scheduleGroups {
		if err := s.registerRefreshJob(schedule); err != nil {
			s.logger.Warn("failed to register refresh job", "schedule", schedule, "error", err)
		}
	}

	s.cron.StartAsync()
	s.logger.Info("cache scheduler started", "jobs", len(s.jobs))
}
