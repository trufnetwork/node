// Package tn_cache implements stream data caching with robust error handling.
// See internal/errors/doc.go for the complete error handling philosophy.
package tn_cache

import (
	"context"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/trufnetwork/kwil-db/common"
	"go.opentelemetry.io/otel/attribute"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/errors"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/parsing"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
)

// refreshStreamDataWithRetry refreshes stream data with exponential backoff retry logic using retry-go
func (s *CacheScheduler) refreshStreamDataWithRetry(ctx context.Context, directive config.CacheDirective, maxRetries int) error {
	return retry.Do(
		func() error {
			return s.refreshStreamData(ctx, directive)
		},
		retry.Attempts(uint(maxRetries+1)),
		retry.Delay(1*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.MaxDelay(30*time.Second),
		retry.OnRetry(func(n uint, err error) {
			s.logger.Warn("refresh failed, retrying",
				"provider", directive.DataProvider,
				"stream", directive.StreamID,
				"attempt", n,
				"error", err)
		}),
		retry.RetryIf(func(err error) bool {
			// Don't retry non-retryable errors
			if errors.IsNonRetryableError(err) {
				s.logger.Debug("non-retryable error, not retrying",
					"provider", directive.DataProvider,
					"stream", directive.StreamID,
					"error", err)
				return false
			}
			return true
		}),
		retry.Context(ctx),
		retry.LastErrorOnly(true),
	)
}

// refreshStreamData refreshes the cached data for a single cache directive
func (s *CacheScheduler) refreshStreamData(ctx context.Context, directive config.CacheDirective) (err error) {
	// Add tracing
	ctx, end := tracing.StreamOperation(ctx, tracing.OpRefreshStream, directive.DataProvider, directive.StreamID,
		attribute.String("type", string(directive.Type)))
	defer func() {
		end(err)
	}()

	// Start timing the refresh operation
	startTime := time.Now()

	s.logger.Debug("refreshing stream",
		"provider", directive.DataProvider,
		"stream", directive.StreamID,
		"type", directive.Type)

	// Record refresh start
	s.metrics.RecordRefreshStart(ctx, directive.DataProvider, directive.StreamID)

	// Always fetch from configured start time - data is mutable
	fromTime := directive.TimeRange.From

	// Wildcards already resolved at startup - this should never happen
	if directive.Type != config.DirectiveSpecific {
		s.metrics.RecordRefreshError(ctx, directive.DataProvider, directive.StreamID, "invalid_directive")
		return fmt.Errorf("unexpected directive type in refresh: %s (should be specific after resolution)", directive.Type)
	}

	events, err := s.fetchSpecificStream(ctx, directive, fromTime)

	if err != nil {
		s.metrics.RecordRefreshError(ctx, directive.DataProvider, directive.StreamID, metrics.ClassifyError(err))
		return fmt.Errorf("fetch stream data: %w", err)
	}

	// Database handles duplicates via primary key constraint
	if len(events) > 0 {
		if err := s.cacheDB.CacheEvents(ctx, events); err != nil {
			s.metrics.RecordRefreshError(ctx, directive.DataProvider, directive.StreamID, "storage_error")
			return fmt.Errorf("cache events: %w", err)
		}
		s.logger.Info("cached events",
			"count", len(events),
			"provider", directive.DataProvider,
			"stream", directive.StreamID)
	} else {
		s.logger.Debug("no new events to cache",
			"provider", directive.DataProvider,
			"stream", directive.StreamID)
	}

	s.metrics.RecordRefreshComplete(ctx, directive.DataProvider, directive.StreamID, time.Since(startTime), len(events))

	// Update gauge metrics after successful refresh
	s.updateGaugeMetrics(ctx)

	return nil
}

// fetchSpecificStream calls get_record_composed action with proper authorization
func (s *CacheScheduler) fetchSpecificStream(ctx context.Context, directive config.CacheDirective, fromTime *int64) ([]internal.CachedEvent, error) {
	// For now, we'll use get_record_composed as the primary action
	action := "get_record_composed"

	// if from is nil, we should set to 0, meaning it's all available
	if fromTime == nil {
		fromTime = new(int64)
		*fromTime = 0
	}

	// Build arguments for the action call
	args := []any{
		directive.DataProvider,
		directive.StreamID,
		fromTime, // from timestamp
		nil,      // to timestamp (fetch all available)
		nil,      // frozen_at (not applicable for cache refresh)
		false,    // don't use cache to get new data
	}

	var events []internal.CachedEvent

	// Create a proper engine context with extension agent as the caller
	// This provides @caller = "extension_agent" which has special permissions
	engineCtx := s.createExtensionEngineContext(ctx)

	s.logger.Debug("calling engine action",
		"action", action,
		"provider", directive.DataProvider,
		"stream", directive.StreamID,
		"namespace", s.namespace,
		"caller", internal.ExtensionAgentName)

	// Execute the action with proper engine context
	// This provides @caller for authorization checks in get_record_composed
	// Use the wrapped independent connection pool instead of app.DB to avoid "tx is closed" errors
	result, err := s.app.Engine.Call(
		engineCtx,
		s.getWrappedDB(), // Use wrapped independent connection pool
		s.namespace,      // configurable database namespace
		action,
		args,
		func(row *common.Row) error {
			// Parse each row into a CachedEvent
			if len(row.Values) >= 2 {
				// Parse event time using utility function
				eventTime, err := parsing.ParseEventTime(row.Values[0])
				if err != nil {
					return fmt.Errorf("parse event_time: %w", err)
				}

				// Then parse the value using utility function
				value, err := parsing.ParseEventValue(row.Values[1])
				if err != nil {
					return fmt.Errorf("parse value: %w", err)
				}

				event := internal.CachedEvent{
					DataProvider: directive.DataProvider,
					StreamID:     directive.StreamID,
					EventTime:    eventTime,
					Value:        value,
				}
				events = append(events, event)
			}
			return nil
		},
	)

	if err != nil {
		// Check if this is a "stream not found" type error
		if errors.IsNotFoundError(err) {
			s.logger.Warn("stream not found or has no data",
				"action", action,
				"provider", directive.DataProvider,
				"stream", directive.StreamID,
				"error", err)
			return []internal.CachedEvent{}, nil // Return empty events, not an error
		}

		return nil, fmt.Errorf("call action %s: %w", action, err)
	}

	s.logger.Debug("fetched stream data",
		"action", action,
		"events", len(events),
		"provider", directive.DataProvider,
		"stream", directive.StreamID)

	// Log any notices from the action execution
	if len(result.Logs) > 0 {
		s.logger.Debug("action logs", "logs", result.FormatLogs())
	}

	return events, nil
}
