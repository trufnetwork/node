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
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
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
	metrics   metrics.MetricsRecorder // metrics recorder

	// Dynamic resolution fields
	originalDirectives []config.CacheDirective // Keep original with wildcards/includeChildren
	resolvedDirectives []config.CacheDirective // Current resolved state
	resolutionJob      cron.EntryID            // Cron job for resolution
	resolutionMu       sync.RWMutex            // Protect resolved state
	lastResolution     time.Time               // Track last resolution time
	resolutionStatus   ResolutionStatus        // Current resolution status
	resolutionErr      error                   // Last resolution error if any

	// Job-specific context management
	jobContexts   map[string]*jobContext // Job ID -> context info
	jobContextsMu sync.RWMutex
	jobTimeout    time.Duration // Timeout for individual jobs
}

// jobContext holds context information for individual scheduled jobs
type jobContext struct {
	ctx        context.Context
	cancel     context.CancelFunc
	startTime  time.Time
	schedule   string
	streamInfo string // For debugging and monitoring
}

// NewCacheScheduler creates a new cache scheduler instance
func NewCacheScheduler(app *common.App, cacheDB *internal.CacheDB, logger log.Logger, metricsRecorder metrics.MetricsRecorder) *CacheScheduler {
	return NewCacheSchedulerWithNamespace(app, cacheDB, logger, "", metricsRecorder)
}

// NewCacheSchedulerWithNamespace creates a new cache scheduler instance with configurable namespace
func NewCacheSchedulerWithNamespace(app *common.App, cacheDB *internal.CacheDB, logger log.Logger, namespace string, metricsRecorder metrics.MetricsRecorder) *CacheScheduler {
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
		metrics:          metricsRecorder,
		jobContexts:      make(map[string]*jobContext),
		jobTimeout:       60 * time.Minute, // Default job timeout
	}
}

// createJobContext creates a new job-specific context with timeout
func (s *CacheScheduler) createJobContext(jobID, schedule, streamInfo string) (context.Context, context.CancelFunc) {
	s.jobContextsMu.Lock()
	defer s.jobContextsMu.Unlock()

	// Create context with timeout derived from scheduler context
	ctx, cancel := context.WithTimeout(s.ctx, s.jobTimeout)

	// Store job context info
	s.jobContexts[jobID] = &jobContext{
		ctx:        ctx,
		cancel:     cancel,
		startTime:  time.Now(),
		schedule:   schedule,
		streamInfo: streamInfo,
	}

	return ctx, cancel
}

// removeJobContext removes a job context after completion
func (s *CacheScheduler) removeJobContext(jobID string) {
	s.jobContextsMu.Lock()
	defer s.jobContextsMu.Unlock()

	if jc, exists := s.jobContexts[jobID]; exists {
		// Cancel if not already done
		jc.cancel()
		delete(s.jobContexts, jobID)
		s.logger.Debug("removed job context",
			"job_id", jobID,
			"duration", time.Since(jc.startTime))
	}
}

// cancelAllJobContexts cancels all active job contexts
func (s *CacheScheduler) cancelAllJobContexts() {
	s.jobContextsMu.Lock()
	defer s.jobContextsMu.Unlock()

	for jobID, jc := range s.jobContexts {
		jc.cancel()
		s.logger.Info("cancelled job context",
			"job_id", jobID,
			"schedule", jc.schedule,
			"running_time", time.Since(jc.startTime))
	}

	// Clear the map
	s.jobContexts = make(map[string]*jobContext)
}

// getActiveJobCount returns the number of currently active jobs
func (s *CacheScheduler) getActiveJobCount() int {
	s.jobContextsMu.RLock()
	defer s.jobContextsMu.RUnlock()
	return len(s.jobContexts)
}

// createExtensionEngineContext creates a proper EngineContext for the extension
// to call actions as a system agent with valid transaction context
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
			Caller:        internal.ExtensionAgentName, // Extension identifies itself as caller
			Authenticator: "system",
		},
		OverrideAuthz: true,  // Override authorization for system operations
		InvalidTxCtx:  false, // Context is valid for accessing system variables
	}
}

// Start initializes and starts the cache scheduler
func (s *CacheScheduler) Start(ctx context.Context, processedConfig *config.ProcessedConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("starting cache scheduler", "directives", len(processedConfig.Directives))

	s.ctx, s.cancel = context.WithCancel(ctx)

	// Store original directives for future re-resolution
	s.originalDirectives = processedConfig.Directives

	// First, resolve wildcard and IncludeChildren directives to concrete stream specifications
	resolvedStreamSpecs, err := s.resolveStreamSpecs(s.ctx, s.originalDirectives)
	if err != nil {
		return fmt.Errorf("resolve stream specs: %w", err)
	}

	s.logger.Info("resolved stream specifications",
		"original_count", len(s.originalDirectives),
		"resolved_count", len(resolvedStreamSpecs))

	// Store the resolved state for use by refresh jobs
	s.resolvedDirectives = resolvedStreamSpecs
	s.lastResolution = time.Now()

	// Then, persist stream configurations to the database
	if err := s.storeStreamConfigs(s.ctx, resolvedStreamSpecs); err != nil {
		return fmt.Errorf("store stream configs: %w", err)
	}

	// Set up periodic re-resolution if configured
	if processedConfig.ResolutionSchedule != "" {
		if err := s.registerResolutionJob(processedConfig.ResolutionSchedule); err != nil {
			return fmt.Errorf("register resolution job: %w", err)
		}
	}

	// Group resolved specifications by schedule for efficient batch processing
	scheduleGroups := s.groupBySchedule(resolvedStreamSpecs)

	// Register cron jobs for each unique schedule
	for schedule := range scheduleGroups {
		if err := s.registerRefreshJob(schedule); err != nil {
			return fmt.Errorf("register refresh job for schedule %s: %w", schedule, err)
		}
	}

	// Start the cron scheduler
	s.cron.Start()
	s.logger.Info("cache scheduler started", "jobs", len(s.jobs))

	// Run initial refresh asynchronously without delay
	// Now that we have:
	// 1. Independent connection pool (no more "tx is closed" errors)
	// 2. Proper EngineContext (no more "invalid transaction context" errors)
	// We can run the initial refresh immediately
	go s.runInitialRefresh(resolvedStreamSpecs)

	return nil
}

// Stop gracefully shuts down the cache scheduler
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

// runInitialRefresh performs an initial refresh of all streams with concurrency control
func (s *CacheScheduler) runInitialRefresh(directives []config.CacheDirective) {
	// Generate unique job ID for initial refresh
	jobID := fmt.Sprintf("initial_refresh_%d", time.Now().UnixNano())

	// Create job-specific context for initial refresh
	jobCtx, jobCancel := s.createJobContext(jobID, "initial_refresh", fmt.Sprintf("streams=%d", len(directives)))

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

	s.logger.Info("starting initial refresh",
		"streams", len(directives),
		"job_id", jobID)

	g, ctx := errgroup.WithContext(jobCtx)
	g.SetLimit(5) // Maximum 5 concurrent operations

	for _, directive := range directives {
		dir := directive // Capture loop variable to avoid closure issues

		g.Go(func() error {
			// Check context before starting work
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err := s.refreshStreamDataWithCircuitBreaker(ctx, dir); err != nil {
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
		jobCtx, jobCancel := s.createJobContext(jobID, schedule, fmt.Sprintf("schedule=%s", schedule))
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

				if err := s.refreshStreamDataWithCircuitBreaker(gCtx, dir); err != nil {
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
