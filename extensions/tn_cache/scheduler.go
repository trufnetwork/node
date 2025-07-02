package tn_cache

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
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

	// Resolve wildcard and IncludeChildren directives to concrete stream specifications
	resolvedStreamSpecs, err := s.resolveStreamSpecs(s.ctx, instructions)
	if err != nil {
		return fmt.Errorf("resolve stream specs: %w", err)
	}

	s.logger.Info("resolved stream specifications",
		"original_count", len(instructions),
		"resolved_count", len(resolvedStreamSpecs))

	// Store stream configurations in the database
	if err := s.storeStreamConfigs(s.ctx, resolvedStreamSpecs); err != nil {
		return fmt.Errorf("store stream configs: %w", err)
	}

	// Group resolved specifications by schedule for efficient batch processing
	scheduleGroups := s.groupBySchedule(resolvedStreamSpecs)

	// Register cron jobs for each schedule group
	for schedule, groupInstructions := range scheduleGroups {
		if err := s.registerScheduledJob(schedule, groupInstructions); err != nil {
			return fmt.Errorf("register job for schedule %s: %w", schedule, err)
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

// resolveStreamSpecs resolves wildcard and IncludeChildren directives to concrete stream specifications
func (s *CacheScheduler) resolveStreamSpecs(ctx context.Context, instructions []config.InstructionDirective) ([]config.InstructionDirective, error) {
	var resolvedSpecs []config.InstructionDirective

	for _, instruction := range instructions {
		switch instruction.Type {
		case config.DirectiveSpecific:
			// Handle IncludeChildren for specific composed streams
			if instruction.IncludeChildren {
				// Expand to include child streams using get_category_streams
				s.logger.Debug("expanding specific instruction with IncludeChildren",
					"provider", instruction.DataProvider,
					"stream", instruction.StreamID)

				childStreams, err := s.getChildStreamsForComposed(ctx, instruction.DataProvider, instruction.StreamID, instruction.TimeRange.From)
				if err != nil {
					return nil, fmt.Errorf("expand children for %s/%s: %w", instruction.DataProvider, instruction.StreamID, err)
				}

				// Add the original stream first
				resolvedSpecs = append(resolvedSpecs, instruction)

				// Add each child stream as a separate specification
				for _, childKey := range childStreams {
					// Parse the composite key (provider:streamID)
					parts := strings.Split(childKey, ":")
					if len(parts) == 2 {
						childProvider, childStreamID := parts[0], parts[1]
						childSpec := config.InstructionDirective{
							ID:           fmt.Sprintf("%s_%s_%s", childProvider, childStreamID, "child_resolved"),
							Type:         config.DirectiveSpecific,
							DataProvider: childProvider,
							StreamID:     childStreamID,
							Schedule:     instruction.Schedule,
							TimeRange:    instruction.TimeRange,
							IncludeChildren: false, // Avoid recursive resolution
						}
						resolvedSpecs = append(resolvedSpecs, childSpec)
					}
				}

				s.logger.Info("resolved specific instruction with children",
					"provider", instruction.DataProvider,
					"stream", instruction.StreamID,
					"child_count", len(childStreams))
			} else {
				// Keep specific instructions as-is when IncludeChildren is false
				resolvedSpecs = append(resolvedSpecs, instruction)
			}

		case config.DirectiveProviderWildcard:
			// Check for mutual exclusivity with IncludeChildren
			if instruction.IncludeChildren {
				s.logger.Warn("both provider wildcard and IncludeChildren are set - using wildcard behavior only",
					"provider", instruction.DataProvider,
					"wildcard", instruction.StreamID)
			}

			// Resolve wildcard to all composed streams for the provider
			s.logger.Debug("resolving wildcard instruction",
				"provider", instruction.DataProvider,
				"wildcard", instruction.StreamID)

			composedStreams, err := s.getComposedStreamsForProvider(ctx, instruction.DataProvider)
			if err != nil {
				return nil, fmt.Errorf("resolve wildcard for provider %s: %w", instruction.DataProvider, err)
			}

			// Create individual specifications for each composed stream
			for _, streamID := range composedStreams {
				streamSpec := config.InstructionDirective{
					ID:           fmt.Sprintf("%s_%s_%s", instruction.DataProvider, streamID, "resolved"),
					Type:         config.DirectiveSpecific, // Convert to specific
					DataProvider: instruction.DataProvider,
					StreamID:     streamID,
					Schedule:     instruction.Schedule,
					TimeRange:    instruction.TimeRange,
				}
				resolvedSpecs = append(resolvedSpecs, streamSpec)
			}

			s.logger.Info("resolved wildcard instruction",
				"provider", instruction.DataProvider,
				"resolved_streams", len(composedStreams))
		}
	}

	// Deduplicate resolved specifications, keeping ones with earliest 'from' timestamp
	return s.deduplicateResolvedSpecs(resolvedSpecs), nil
}

// storeStreamConfigs persists the stream configurations to the database using batch operations
func (s *CacheScheduler) storeStreamConfigs(ctx context.Context, instructions []config.InstructionDirective) error {
	if len(instructions) == 0 {
		return nil
	}

	// Convert instructions to stream configs
	var configs []internal.StreamCacheConfig
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
		configs = append(configs, streamConfig)
	}

	// Use batch insert for efficiency
	if err := s.cacheDB.AddStreamConfigs(ctx, configs); err != nil {
		return fmt.Errorf("failed to add stream configs: %w", err)
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

// getComposedStreamsForProvider queries all composed streams for a given provider using list_streams action
func (s *CacheScheduler) getComposedStreamsForProvider(ctx context.Context, provider string) ([]string, error) {
	var composedStreams []string

	// Use the list_streams action to get all streams for the provider
	result, err := s.app.Engine.CallWithoutEngineCtx(
		ctx,
		s.app.DB,
		s.namespace,
		"list_streams",
		[]any{
			provider,  // data_provider
			5000,      // limit (maximum allowed)
			0,         // offset
			"stream_id", // order_by
			nil,       // block_height (current)
		},
		func(row *common.Row) error {
			if len(row.Values) >= 3 {
				// row.Values: [data_provider, stream_id, stream_type, created_at]
				if streamID, ok := row.Values[1].(string); ok {
					if streamType, ok := row.Values[2].(string); ok && streamType == "composed" {
						composedStreams = append(composedStreams, streamID)
					}
				}
			}
			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("query composed streams for provider %s: %w", provider, err)
	}

	s.logger.Debug("found composed streams",
		"provider", provider,
		"count", len(composedStreams),
		"streams", composedStreams)

	// Log any notices from the query execution
	if len(result.Logs) > 0 {
		s.logger.Debug("query logs", "logs", result.FormatLogs())
	}

	return composedStreams, nil
}

// deduplicateResolvedSpecs removes duplicate stream specifications, keeping the one with earliest 'from' timestamp
func (s *CacheScheduler) deduplicateResolvedSpecs(specs []config.InstructionDirective) []config.InstructionDirective {
	// Map to track seen streams: key is "provider:streamID"
	streamMap := make(map[string]config.InstructionDirective)
	
	for _, spec := range specs {
		key := fmt.Sprintf("%s:%s", spec.DataProvider, spec.StreamID)
		
		if existing, exists := streamMap[key]; exists {
			// Compare 'from' timestamps - keep the one with earlier timestamp
			var existingFrom, newFrom int64
			
			if existing.TimeRange.From != nil {
				existingFrom = *existing.TimeRange.From
			}
			if spec.TimeRange.From != nil {
				newFrom = *spec.TimeRange.From
			}
			
			// If new spec has earlier 'from' (or same), check schedules
			if newFrom <= existingFrom {
				// If timestamps are equal, also log schedule info for debugging
				if newFrom == existingFrom {
					s.logger.Warn("duplicate stream with same 'from' timestamp - keeping first occurrence",
						"provider", spec.DataProvider,
						"stream", spec.StreamID,
						"kept_schedule", existing.Schedule.CronExpr,
						"discarded_schedule", spec.Schedule.CronExpr,
						"from_timestamp", existingFrom)
				} else {
					s.logger.Warn("duplicate stream found - keeping earlier 'from' timestamp",
						"provider", spec.DataProvider,
						"stream", spec.StreamID,
						"kept_from", newFrom,
						"kept_schedule", spec.Schedule.CronExpr,
						"discarded_from", existingFrom,
						"discarded_schedule", existing.Schedule.CronExpr)
					streamMap[key] = spec
				}
			} else {
				s.logger.Warn("duplicate stream found - keeping earlier 'from' timestamp",
					"provider", spec.DataProvider,
					"stream", spec.StreamID,
					"kept_from", existingFrom,
					"kept_schedule", existing.Schedule.CronExpr,
					"discarded_from", newFrom,
					"discarded_schedule", spec.Schedule.CronExpr)
			}
		} else {
			streamMap[key] = spec
		}
	}
	
	// Convert map back to slice
	deduplicated := make([]config.InstructionDirective, 0, len(streamMap))
	for _, spec := range streamMap {
		deduplicated = append(deduplicated, spec)
	}
	
	// Log summary if deduplication occurred
	if len(deduplicated) < len(specs) {
		s.logger.Info("deduplication completed",
			"original_count", len(specs),
			"deduplicated_count", len(deduplicated),
			"removed", len(specs)-len(deduplicated))
	}
	
	return deduplicated
}

// getChildStreamsForComposed queries all child streams for a composed stream using get_category_streams action
func (s *CacheScheduler) getChildStreamsForComposed(ctx context.Context, dataProvider, streamID string, fromTime *int64) ([]string, error) {
	var childStreams []string

	// Use current time if fromTime is not specified
	activeFrom := int64(0)
	if fromTime != nil {
		activeFrom = *fromTime
	}

	// Use the get_category_streams action to get all child streams
	result, err := s.app.Engine.CallWithoutEngineCtx(
		ctx,
		s.app.DB,
		s.namespace,
		"get_category_streams",
		[]any{
			dataProvider, // data_provider
			streamID,     // stream_id
			activeFrom,   // active_from
			nil,          // active_to (get all)
		},
		func(row *common.Row) error {
			if len(row.Values) >= 2 {
				// row.Values: [data_provider, stream_id]
				if childProvider, ok := row.Values[0].(string); ok {
					if childStreamID, ok := row.Values[1].(string); ok {
						// Create a composite key for the child stream
						childKey := fmt.Sprintf("%s:%s", childProvider, childStreamID)
						childStreams = append(childStreams, childKey)
					}
				}
			}
			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("query child streams for %s/%s: %w", dataProvider, streamID, err)
	}

	s.logger.Debug("found child streams",
		"provider", dataProvider,
		"stream", streamID,
		"count", len(childStreams),
		"children", childStreams)

	// Log any notices from the query execution
	if len(result.Logs) > 0 {
		s.logger.Debug("query logs", "logs", result.FormatLogs())
	}

	return childStreams, nil
}
