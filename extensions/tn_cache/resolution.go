package tn_cache

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/common"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/errors"
	"github.com/trufnetwork/node/extensions/tn_cache/validation"
)

// Resolution solves the "stale wildcard" problem:
// When config says "cache all streams from provider X", new streams
// created after startup are automatically detected and cached.
// Database updates are atomic to prevent data gaps during re-resolution.

// resolveStreamSpecs expands patterns to actual stream IDs by querying the database
func (s *CacheScheduler) resolveStreamSpecs(ctx context.Context, directives []config.CacheDirective) ([]config.CacheDirective, error) {
	var resolvedSpecs []config.CacheDirective

	for _, directive := range directives {
		switch directive.Type {
		case config.DirectiveSpecific:
			if directive.IncludeChildren {
				// Query child streams from composed (category) stream
				s.logger.Debug("expanding specific directive with IncludeChildren",
					"provider", directive.DataProvider,
					"stream", directive.StreamID)

				childStreams, err := s.getChildStreamsForComposed(ctx, directive.DataProvider, directive.StreamID, directive.TimeRange.From)
				if err != nil {
					// Non-fatal: parent might not have children yet
					s.logger.Error("failed to expand children for composed stream",
						"provider", directive.DataProvider,
						"stream", directive.StreamID,
						"error", err)
					// Always cache parent even if children lookup fails
					resolvedSpecs = append(resolvedSpecs, directive)
					continue
				}

				resolvedSpecs = append(resolvedSpecs, directive)

				for _, childKey := range childStreams {
					// Child key format: "provider:streamID"
					parts := strings.Split(childKey, ":")
					if len(parts) == 2 {
						childProvider, childStreamID := parts[0], parts[1]
						childSpec := config.CacheDirective{
							ID:              fmt.Sprintf("%s_%s_%s", childProvider, childStreamID, "child_resolved"),
							Type:            config.DirectiveSpecific,
							DataProvider:    childProvider,
							StreamID:        childStreamID,
							Schedule:        directive.Schedule,
							TimeRange:       directive.TimeRange,
							IncludeChildren: false, // Avoid recursive resolution
						}
						resolvedSpecs = append(resolvedSpecs, childSpec)
					}
				}

				s.logger.Info("resolved specific directive with children",
					"provider", directive.DataProvider,
					"stream", directive.StreamID,
					"child_count", len(childStreams))
			} else {
				// Keep specific directives as-is when IncludeChildren is false
				resolvedSpecs = append(resolvedSpecs, directive)
			}

		case config.DirectiveProviderWildcard:
			if directive.IncludeChildren {
				s.logger.Warn("both provider wildcard and IncludeChildren are set - using wildcard behavior only",
					"provider", directive.DataProvider,
					"wildcard", directive.StreamID)
			}

			s.logger.Debug("resolving wildcard directive",
				"provider", directive.DataProvider,
				"wildcard", directive.StreamID)

			composedStreams, err := s.getComposedStreamsForProvider(ctx, directive.DataProvider)
			if err != nil {
				return nil, fmt.Errorf("resolve wildcard for provider %s: %w", directive.DataProvider, err)
			}

			// Convert each found stream to a concrete directive
			for _, streamID := range composedStreams {
				streamSpec := config.CacheDirective{
					ID:           fmt.Sprintf("%s_%s_%s", directive.DataProvider, streamID, "resolved"),
					Type:         config.DirectiveSpecific, // Convert to specific
					DataProvider: directive.DataProvider,
					StreamID:     streamID,
					Schedule:     directive.Schedule,
					TimeRange:    directive.TimeRange,
				}
				resolvedSpecs = append(resolvedSpecs, streamSpec)
			}

			s.logger.Info("resolved wildcard directive",
				"provider", directive.DataProvider,
				"resolved_streams", len(composedStreams))
		}
	}

	// Multiple directives might resolve to same stream - keep earliest 'from'
	return s.deduplicateResolvedSpecs(resolvedSpecs), nil
}

// deduplicateResolvedSpecs prevents redundant caching when patterns overlap
func (s *CacheScheduler) deduplicateResolvedSpecs(specs []config.CacheDirective) []config.CacheDirective {
	// Track seen streams by composite key
	streamMap := make(map[string]config.CacheDirective)

	for _, spec := range specs {
		key := fmt.Sprintf("%s:%s", spec.DataProvider, spec.StreamID)

		if existing, exists := streamMap[key]; exists {
			// Earlier 'from' = more historical data to cache
			var existingFrom, newFrom int64

			if existing.TimeRange.From != nil {
				existingFrom = *existing.TimeRange.From
			}
			if spec.TimeRange.From != nil {
				newFrom = *spec.TimeRange.From
			}

			if newFrom <= existingFrom {
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

	deduplicated := make([]config.CacheDirective, 0, len(streamMap))
	for _, spec := range streamMap {
		deduplicated = append(deduplicated, spec)
	}

	if len(deduplicated) < len(specs) {
		s.logger.Info("deduplication completed",
			"original_count", len(specs),
			"deduplicated_count", len(deduplicated),
			"removed", len(specs)-len(deduplicated))
	}

	return deduplicated
}

// getComposedStreamsForProvider finds streams that can expand (have children)
func (s *CacheScheduler) getComposedStreamsForProvider(ctx context.Context, provider string) ([]string, error) {
	var composedStreams []string

	// Create a proper engine context with extension agent as the caller
	engineCtx := s.createExtensionEngineContext(ctx)

	// Query all streams for the provider using list_streams action
	// Use Engine.Call with proper context to provide @caller
	// Use wrapped independent connection pool to avoid "tx is closed" errors
	result, err := s.app.Engine.Call(
		engineCtx,
		s.getWrappedDB(), // Use wrapped independent connection pool
		s.namespace,
		"list_streams",
		[]any{
			provider,    // data_provider
			5000,        // limit (maximum allowed)
			0,           // offset
			"stream_id", // order_by
			nil,         // block_height (current)
		},
		func(row *common.Row) error {
			if len(row.Values) >= 3 {
				// Extract stream_id and check if it's a composed stream
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
		// Check if this is a "provider not found" type error
		if errors.IsNotFoundError(err) {
			s.logger.Warn("provider not found",
				"provider", provider,
				"error", err)
			return []string{}, nil // Return empty list, not an error
		}
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

// getChildStreamsForComposed queries all child streams for a composed stream using get_category_streams action
func (s *CacheScheduler) getChildStreamsForComposed(ctx context.Context, dataProvider, streamID string, fromTime *int64) ([]string, error) {
	var childStreams []string

	// Use current time if fromTime is not specified
	activeFrom := int64(0)
	if fromTime != nil {
		activeFrom = *fromTime
	}

	// Create a proper engine context with extension agent as the caller
	engineCtx := s.createExtensionEngineContext(ctx)

	// Query child streams using get_category_streams action
	// Use Engine.Call with proper context to provide @caller
	// Use wrapped independent connection pool to avoid "tx is closed" errors
	result, err := s.app.Engine.Call(
		engineCtx,
		s.getWrappedDB(), // Use wrapped independent connection pool
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
				// Extract provider and stream_id for each child
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
		// Check if this is a "stream not found" or "not a composed stream" error
		if errors.IsNotFoundError(err) {
			s.logger.Warn("stream not found or not a composed stream",
				"provider", dataProvider,
				"stream", streamID,
				"error", err)
			return []string{}, nil // Return empty list, not an error
		}
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

// registerResolutionJob registers the cron job for periodic re-resolution
func (s *CacheScheduler) registerResolutionJob(schedule string) error {
	// Validate the cron schedule
	if err := validation.ValidateCronSchedule(schedule); err != nil {
		return fmt.Errorf("invalid resolution schedule %s: %w", schedule, err)
	}

	// Create resolution job function
	jobFunc := func() {
		// Add panic recovery
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("panic in resolution job",
					"panic", r,
					"stack", string(debug.Stack()))
			}
		}()

		if err := s.performGlobalResolution(s.ctx); err != nil {
			s.logger.Error("global resolution failed", "error", err)
		}
	}

	// Register the cron job
	entryID, err := s.cron.AddFunc(schedule, jobFunc)
	if err != nil {
		return fmt.Errorf("add resolution cron job: %w", err)
	}

	s.jobs[ResolutionJobKey] = entryID
	s.logger.Info("registered resolution cron job", "schedule", schedule)

	return nil
}

// performGlobalResolution discovers new streams matching patterns
func (s *CacheScheduler) performGlobalResolution(ctx context.Context) error {

	s.logger.Info("starting global resolution")
	startTime := time.Now()

	// Re-expand patterns to catch newly created streams
	newResolvedSpecs, err := s.resolveStreamSpecs(ctx, s.originalDirectives)
	if err != nil {
		return fmt.Errorf("resolve stream specs: %w", err)
	}

	oldSet := make(map[string]bool)
	newSet := make(map[string]bool)

	s.resolutionMu.RLock()
	// Safety: prevent accidental cache wipe from resolution bugs
	if len(s.resolvedDirectives) > 0 && len(newResolvedSpecs) == 0 {
		s.resolutionMu.RUnlock()
		err := fmt.Errorf("resolution would clear all streams (had %d, now 0) - aborting", len(s.resolvedDirectives))
		s.logger.Error("dangerous resolution detected", "error", err)
		return err
	}

	for _, dir := range s.resolvedDirectives {
		key := fmt.Sprintf("%s:%s", dir.DataProvider, dir.StreamID)
		oldSet[key] = true
	}
	s.resolutionMu.RUnlock()

	for _, dir := range newResolvedSpecs {
		key := fmt.Sprintf("%s:%s", dir.DataProvider, dir.StreamID)
		newSet[key] = true
	}

	// Diff to log what changed
	var added, removed []string
	for key := range newSet {
		if !oldSet[key] {
			added = append(added, key)
		}
	}
	for key := range oldSet {
		if !newSet[key] {
			removed = append(removed, key)
		}
	}

	// Atomic update prevents half-cached state
	if err := s.updateCachedStreamsTable(ctx, newResolvedSpecs); err != nil {
		return fmt.Errorf("update cached_streams table: %w", err)
	}

	s.resolutionMu.Lock()
	s.resolvedDirectives = newResolvedSpecs
	s.resolutionMu.Unlock()

	s.logger.Info("global resolution completed",
		"duration", time.Since(startTime),
		"original_count", len(s.originalDirectives),
		"resolved_count", len(newResolvedSpecs),
		"added_streams", len(added),
		"removed_streams", len(removed))

	if len(added) > 0 {
		s.logger.Debug("streams added", "streams", added)
	}
	if len(removed) > 0 {
		s.logger.Debug("streams removed", "streams", removed)
	}

	return nil
}

// updateCachedStreamsTable atomically updates the cached_streams table
func (s *CacheScheduler) updateCachedStreamsTable(ctx context.Context, resolvedSpecs []config.CacheDirective) error {
	// First, get current streams from database
	currentConfigs, err := s.cacheDB.ListStreamConfigs(ctx)
	if err != nil {
		return fmt.Errorf("list current stream configs: %w", err)
	}

	// Build sets for comparison
	currentSet := make(map[string]internal.StreamCacheConfig)
	for _, config := range currentConfigs {
		key := fmt.Sprintf("%s:%s", config.DataProvider, config.StreamID)
		currentSet[key] = config
	}

	// Then, build new configurations
	newConfigs := make([]internal.StreamCacheConfig, 0, len(resolvedSpecs))
	newSet := make(map[string]bool)

	for _, spec := range resolvedSpecs {
		key := fmt.Sprintf("%s:%s", spec.DataProvider, spec.StreamID)
		newSet[key] = true

		// Preserve last_refreshed if stream already exists
		var lastRefreshed int64
		if existing, exists := currentSet[key]; exists {
			lastRefreshed = existing.LastRefreshed
		}

		var fromTimestamp int64
		if spec.TimeRange.From != nil {
			fromTimestamp = *spec.TimeRange.From
		}

		newConfigs = append(newConfigs, internal.StreamCacheConfig{
			DataProvider:  spec.DataProvider,
			StreamID:      spec.StreamID,
			FromTimestamp: fromTimestamp,
			LastRefreshed: lastRefreshed,
			CronSchedule:  spec.Schedule.CronExpr,
		})
	}

	// Find streams to delete
	var toDelete []internal.StreamCacheConfig
	for key, config := range currentSet {
		if !newSet[key] {
			toDelete = append(toDelete, config)
		}
	}

	// Finally, apply all changes atomically
	if err := s.cacheDB.UpdateStreamConfigsAtomic(ctx, newConfigs, toDelete); err != nil {
		return fmt.Errorf("atomic update failed: %w", err)
	}

	return nil
}

// getDirectivesForSchedule returns resolved directives for a specific schedule
func (s *CacheScheduler) getDirectivesForSchedule(schedule string) []config.CacheDirective {
	s.resolutionMu.RLock()
	defer s.resolutionMu.RUnlock()

	var directives []config.CacheDirective
	for _, directive := range s.resolvedDirectives {
		if directive.Schedule.CronExpr == schedule {
			directives = append(directives, directive)
		}
	}

	return directives
}
