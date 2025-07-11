// Package tn_cache implements stream data caching with robust error handling.
// See internal/errors/doc.go for the complete error handling philosophy.
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"go.opentelemetry.io/otel/attribute"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/errors"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
)

// refreshStreamDataWithRetry refreshes stream data with exponential backoff retry logic using retry-go
func (s *CacheScheduler) refreshStreamDataWithRetry(ctx context.Context, directive config.CacheDirective, maxRetries int) error {
	return retry.Do(
		func() error {
			return s.RefreshStreamData(ctx, directive)
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

// RefreshStreamData refreshes the cached data for a single cache directive
func (s *CacheScheduler) RefreshStreamData(ctx context.Context, directive config.CacheDirective) (err error) {
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

	// Fetch both regular and index events concurrently
	var events []internal.CachedEvent
	var indexEvents []internal.CachedEvent
	var fetchErr error
	var indexFetchErr error

	// Create a wait group to fetch both types concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	// Fetch regular events
	go func() {
		defer wg.Done()
		events, fetchErr = s.fetchSpecificStream(ctx, directive, fromTime)
	}()

	// Fetch index events
	go func() {
		defer wg.Done()
		indexEvents, indexFetchErr = s.fetchSpecificIndexStream(ctx, directive, fromTime)
	}()

	// Wait for both fetches to complete
	wg.Wait()

	// Check for errors
	if fetchErr != nil {
		s.metrics.RecordRefreshError(ctx, directive.DataProvider, directive.StreamID, metrics.ClassifyError(fetchErr))
		return fmt.Errorf("fetch stream data: %w", fetchErr)
	}
	if indexFetchErr != nil {
		s.metrics.RecordRefreshError(ctx, directive.DataProvider, directive.StreamID, metrics.ClassifyError(indexFetchErr))
		return fmt.Errorf("fetch index stream data: %w", indexFetchErr)
	}

	// Cache both types of events atomically
	if len(events) > 0 || len(indexEvents) > 0 {
		if err := s.cacheDB.CacheEventsWithIndex(ctx, events, indexEvents); err != nil {
			s.metrics.RecordRefreshError(ctx, directive.DataProvider, directive.StreamID, "storage_error")
			return fmt.Errorf("cache events with index: %w", err)
		}
		s.logger.Info("cached events",
			"raw_count", len(events),
			"index_count", len(indexEvents),
			"provider", directive.DataProvider,
			"stream", directive.StreamID)
	} else {
		s.logger.Debug("no new events to cache",
			"provider", directive.DataProvider,
			"stream", directive.StreamID)
	}

	s.metrics.RecordRefreshComplete(ctx, directive.DataProvider, directive.StreamID, time.Since(startTime), len(events))

	// Update gauge metrics after successful refresh
	s.UpdateGaugeMetrics(ctx)

	return nil
}

// fetchSpecificStream calls get_record_composed action with proper authorization
func (s *CacheScheduler) fetchSpecificStream(ctx context.Context, directive config.CacheDirective, fromTime *int64) ([]internal.CachedEvent, error) {
	// Retrieve composed records through engine operations
	records, err := s.engineOperations.GetRecordComposed(ctx, directive.DataProvider, directive.StreamID, fromTime, nil)
	if err != nil {
		return nil, fmt.Errorf("get record composed: %w", err)
	}

	// Convert composed records to cached events for storage
	events := make([]internal.CachedEvent, len(records))
	for i, r := range records {
		events[i] = internal.CachedEvent{
			DataProvider: directive.DataProvider,
			StreamID:     directive.StreamID,
			EventTime:    r.EventTime,
			Value:        r.Value,
		}
	}

	return events, nil
}

// fetchSpecificIndexStream retrieves index events via engine operations
func (s *CacheScheduler) fetchSpecificIndexStream(ctx context.Context, directive config.CacheDirective, fromTime *int64) ([]internal.CachedEvent, error) {
	// Ensure fromTime is not nil (engine expects int64 pointer)
	if fromTime == nil {
		zero := int64(0)
		fromTime = &zero
	}

	records, err := s.engineOperations.GetIndexComposed(ctx, directive.DataProvider, directive.StreamID, fromTime, nil)
	if err != nil {
		// Handle not-found errors gracefully
		if errors.IsNotFoundError(err) {
			s.logger.Warn("stream not found or has no index data",
				"provider", directive.DataProvider,
				"stream", directive.StreamID,
				"error", err)
			return []internal.CachedEvent{}, nil
		}
		return nil, fmt.Errorf("get index composed: %w", err)
	}

	// Convert composed records to cached events for storage
	events := make([]internal.CachedEvent, len(records))
	for i, r := range records {
		events[i] = internal.CachedEvent{
			DataProvider: directive.DataProvider,
			StreamID:     directive.StreamID,
			EventTime:    r.EventTime,
			Value:        r.Value,
		}
	}
	return events, nil
}
