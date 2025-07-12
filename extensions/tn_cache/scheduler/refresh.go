// Package tn_cache implements stream data caching with robust error handling.
// See internal/errors/doc.go for the complete error handling philosophy.
package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"go.opentelemetry.io/otel/attribute"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/errors"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
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
func (s *CacheScheduler) RefreshStreamData(ctx context.Context, directive config.CacheDirective) error {
	// Use middleware for tracing and refresh metrics
	_, err := tracing.TracedWithRefreshMetrics(ctx, tracing.OpRefreshStream, directive.DataProvider, directive.StreamID, s.metrics,
		func(traceCtx context.Context) (any, int, error) {
			s.logger.Debug("refreshing stream",
				"provider", directive.DataProvider,
				"stream", directive.StreamID,
				"type", directive.Type)

			// Always fetch from configured start time - data is mutable
			fromTime := directive.TimeRange.From

			// Wildcards already resolved at startup - this should never happen
			if directive.Type != config.DirectiveSpecific {
				return nil, 0, fmt.Errorf("unexpected directive type in refresh: %s (should be specific after resolution)", directive.Type)
			}

			// Fetch events sequentially to ensure consistency
			// Fetch regular events first
			events, fetchErr := s.fetchSpecificStream(traceCtx, directive, fromTime)
			if fetchErr != nil {
				return nil, 0, fmt.Errorf("fetch stream data: %w", fetchErr)
			}

			// Then fetch index events
			indexEvents, indexFetchErr := s.fetchSpecificIndexStream(traceCtx, directive, fromTime)
			if indexFetchErr != nil {
				return nil, 0, fmt.Errorf("fetch index stream data: %w", indexFetchErr)
			}

			// Cache both types of events atomically
			totalEvents := len(events) + len(indexEvents)
			if totalEvents > 0 {
				if err := s.cacheDB.CacheEventsWithIndex(traceCtx, events, indexEvents); err != nil {
					return nil, 0, fmt.Errorf("cache events with index: %w", err)
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

			// Update gauge metrics after successful refresh
			s.UpdateGaugeMetrics(traceCtx)

			return nil, totalEvents, nil
		}, attribute.String("type", string(directive.Type)))

	return err
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
