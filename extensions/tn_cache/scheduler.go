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
	namespace string // configurable database namespace
	breakers  map[string]*gobreaker.CircuitBreaker // circuit breakers per stream
	breakerMu sync.RWMutex
}

// NewCacheScheduler creates a new cache scheduler instance
func NewCacheScheduler(app *common.App, cacheDB *internal.CacheDB, logger log.Logger) *CacheScheduler {
	return NewCacheSchedulerWithNamespace(app, cacheDB, logger, "")
}

// NewCacheSchedulerWithNamespace creates a new cache scheduler instance with configurable namespace
func NewCacheSchedulerWithNamespace(app *common.App, cacheDB *internal.CacheDB, logger log.Logger, namespace string) *CacheScheduler {
	if namespace == "" {
		namespace = "truf_db" // Default namespace
	}
	
	return &CacheScheduler{
		app:       app,
		cacheDB:   cacheDB,
		logger:    logger.New("scheduler"),
		cron:      cron.New(cron.WithSeconds()),
		jobs:      make(map[string]cron.EntryID),
		namespace: namespace,
		breakers:  make(map[string]*gobreaker.CircuitBreaker),
	}
}

// Start initializes and starts the cache scheduler
func (s *CacheScheduler) Start(ctx context.Context, instructions []config.InstructionDirective) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("starting cache scheduler", "instructions", len(instructions))

	// Create cancellable context
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Store stream configurations in the database
	if err := s.storeStreamConfigs(s.ctx, instructions); err != nil {
		return fmt.Errorf("store stream configs: %w", err)
	}

	// Group instructions by schedule for efficient batch processing
	scheduleGroups := s.groupBySchedule(instructions)

	// Register cron jobs for each schedule group
	for schedule, groupInstructions := range scheduleGroups {
		if err := s.registerScheduledJob(schedule, groupInstructions); err != nil {
			return fmt.Errorf("register job for schedule %s: %w", schedule, err)
		}
	}

	// Start the cron scheduler
	s.cron.Start()
	s.logger.Info("cache scheduler started", "jobs", len(s.jobs))

	// Run initial refresh for all streams asynchronously
	go s.runInitialRefresh(instructions)

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

// storeStreamConfigs persists the stream configurations to the database
func (s *CacheScheduler) storeStreamConfigs(ctx context.Context, instructions []config.InstructionDirective) error {
	for _, instruction := range instructions {
		// Get the from timestamp, defaulting to 0 if not set
		var fromTimestamp int64
		if instruction.TimeRange.From != nil {
			fromTimestamp = *instruction.TimeRange.From
		}

		streamConfig := internal.StreamCacheConfig{
			DataProvider:  instruction.DataProvider,
			StreamID:      instruction.StreamID,
			FromTimestamp: fromTimestamp,
			LastRefreshed: "", // Will be set on first refresh
			CronSchedule:  instruction.Schedule.CronExpr,
		}

		if err := s.cacheDB.AddStreamConfig(ctx, streamConfig); err != nil {
			return fmt.Errorf("failed to add stream config for %s/%s: %w",
				instruction.DataProvider, instruction.StreamID, err)
		}
	}
	return nil
}

// groupBySchedule groups instructions by their cron schedule for batch processing
func (s *CacheScheduler) groupBySchedule(instructions []config.InstructionDirective) map[string][]config.InstructionDirective {
	scheduleGroups := make(map[string][]config.InstructionDirective)
	for _, instruction := range instructions {
		schedule := instruction.Schedule.CronExpr
		scheduleGroups[schedule] = append(scheduleGroups[schedule], instruction)
	}
	return scheduleGroups
}

// registerScheduledJob registers a cron job for a specific schedule
func (s *CacheScheduler) registerScheduledJob(schedule string, instructions []config.InstructionDirective) error {
	// Validate the cron schedule
	if err := validation.ValidateCronSchedule(schedule); err != nil {
		return fmt.Errorf("invalid cron schedule %s: %w", schedule, err)
	}

	// Create the job function
	jobFunc := s.createJobFunc(instructions)

	// Register the cron job
	entryID, err := s.cron.AddFunc(schedule, jobFunc)
	if err != nil {
		return fmt.Errorf("add cron job: %w", err)
	}

	s.jobs[schedule] = entryID
	s.logger.Info("registered cron job", 
		"schedule", schedule, 
		"streams", len(instructions))

	return nil
}

// createJobFunc creates a function that will be executed by the cron scheduler
func (s *CacheScheduler) createJobFunc(instructions []config.InstructionDirective) func() {
	return func() {
		// Add panic recovery to prevent node crashes
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("panic in cache refresh job", 
					"panic", r,
					"stack", string(debug.Stack()))
			}
		}()
		
		s.logger.Debug("executing scheduled refresh", "streams", len(instructions))
		
		// Use errgroup for better error handling
		g, ctx := errgroup.WithContext(s.ctx)
		g.SetLimit(5) // Limit concurrent refreshes
		
		for _, instruction := range instructions {
			inst := instruction // Capture loop variable
			
			g.Go(func() error {
				if err := s.refreshStreamWithCircuitBreaker(ctx, inst); err != nil {
					s.logger.Error("failed to refresh stream",
						"provider", inst.DataProvider,
						"stream", inst.StreamID,
						"type", inst.Type,
						"error", err)
				}
				return nil // Continue with other streams
			})
		}
		
		if err := g.Wait(); err != nil {
			s.logger.Error("scheduled refresh group error", "error", err)
		}
	}
}

// getCircuitBreaker returns or creates a circuit breaker for a stream
func (s *CacheScheduler) getCircuitBreaker(streamKey string) *gobreaker.CircuitBreaker {
	s.breakerMu.RLock()
	cb, exists := s.breakers[streamKey]
	s.breakerMu.RUnlock()
	
	if exists {
		return cb
	}
	
	// Create new circuit breaker
	s.breakerMu.Lock()
	defer s.breakerMu.Unlock()
	
	// Double-check after acquiring write lock
	if cb, exists := s.breakers[streamKey]; exists {
		return cb
	}
	
	cb = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        streamKey,
		MaxRequests: 3,
		Interval:    10 * time.Second,
		Timeout:     60 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.6
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			s.logger.Info("circuit breaker state changed",
				"stream", name,
				"from", from.String(),
				"to", to.String())
		},
	})
	
	s.breakers[streamKey] = cb
	return cb
}

// refreshStreamWithCircuitBreaker wraps stream refresh with circuit breaker
func (s *CacheScheduler) refreshStreamWithCircuitBreaker(ctx context.Context, instruction config.InstructionDirective) error {
	streamKey := fmt.Sprintf("%s/%s", instruction.DataProvider, instruction.StreamID)
	cb := s.getCircuitBreaker(streamKey)
	
	_, err := cb.Execute(func() (interface{}, error) {
		return nil, s.refreshStreamWithRetry(ctx, instruction, 3)
	})
	
	return err
}

// runInitialRefresh performs an initial refresh of all streams with concurrency control
func (s *CacheScheduler) runInitialRefresh(instructions []config.InstructionDirective) {
	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in initial refresh", 
				"panic", r,
				"stack", string(debug.Stack()))
		}
	}()
	
	s.logger.Info("starting initial refresh", "streams", len(instructions))
	
	// Use errgroup for structured concurrency
	g, ctx := errgroup.WithContext(s.ctx)
	g.SetLimit(5) // Maximum 5 concurrent operations
	
	for _, instruction := range instructions {
		inst := instruction // Capture loop variable
		
		g.Go(func() error {
			// Panic recovery is handled by errgroup
			if err := s.refreshStreamWithCircuitBreaker(ctx, inst); err != nil {
				// Log error but don't fail the entire group
				s.logger.Error("failed to perform initial refresh",
					"provider", inst.DataProvider,
					"stream", inst.StreamID,
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