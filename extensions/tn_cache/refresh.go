// Package tn_cache implements stream data caching with robust error handling.
// See internal/errors/doc.go for the complete error handling philosophy.
package tn_cache

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/errors"
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

// parseEventTime converts various types to int64 timestamp
func parseEventTime(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case string:
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed, nil
		}
		return 0, fmt.Errorf("invalid timestamp string: %v", val)
	default:
		return 0, fmt.Errorf("unsupported timestamp type: %T", v)
	}
}

// parseEventValue converts various types to *types.Decimal with decimal(36,18) precision
func parseEventValue(v interface{}) (*types.Decimal, error) {
	switch val := v.(type) {
	case *types.Decimal:
		if val == nil {
			return nil, fmt.Errorf("nil decimal value")
		}
		// Ensure decimal(36,18) precision
		if err := val.SetPrecisionAndScale(36, 18); err != nil {
			return nil, fmt.Errorf("set precision and scale: %w", err)
		}
		return val, nil
	case float64:
		// Convert float64 to decimal(36,18) - note: may lose precision if > 15 digits
		return types.ParseDecimalExplicit(strconv.FormatFloat(val, 'f', -1, 64), 36, 18)
	case float32:
		return types.ParseDecimalExplicit(strconv.FormatFloat(float64(val), 'f', -1, 32), 36, 18)
	case int64:
		return types.ParseDecimalExplicit(strconv.FormatInt(val, 10), 36, 18)
	case int:
		return types.ParseDecimalExplicit(strconv.Itoa(val), 36, 18)
	case int32:
		return types.ParseDecimalExplicit(strconv.FormatInt(int64(val), 10), 36, 18)
	case uint64:
		return types.ParseDecimalExplicit(strconv.FormatUint(val, 10), 36, 18)
	case uint32:
		return types.ParseDecimalExplicit(strconv.FormatUint(uint64(val), 10), 36, 18)
	case *big.Int:
		if val == nil {
			return nil, fmt.Errorf("nil big.Int value")
		}
		return types.ParseDecimalExplicit(val.String(), 36, 18)
	case string:
		// Parse string directly as decimal(36,18)
		return types.ParseDecimalExplicit(val, 36, 18)
	case nil:
		return nil, fmt.Errorf("nil value")
	default:
		return nil, fmt.Errorf("unsupported value type: %T", v)
	}
}

// refreshStreamData refreshes the cached data for a single cache directive
func (s *CacheScheduler) refreshStreamData(ctx context.Context, directive config.CacheDirective) error {
	s.logger.Debug("refreshing stream",
		"provider", directive.DataProvider,
		"stream", directive.StreamID,
		"type", directive.Type)

	// First, get stream config to check last refresh time for incremental updates
	streamConfig, err := s.cacheDB.GetStreamConfig(ctx, directive.DataProvider, directive.StreamID)
	if err != nil {
		return fmt.Errorf("get stream config: %w", err)
	}

	// Calculate time range for incremental refresh
	fromTime := directive.TimeRange.From
	if streamConfig != nil && streamConfig.LastRefreshed != "" {
		// Parse last refresh time for incremental update
		lastRefresh, err := time.Parse(time.RFC3339, streamConfig.LastRefreshed)
		if err == nil {
			// Use last refresh time as starting point for incremental refresh
			fromUnix := lastRefresh.Unix()
			fromTime = &fromUnix
		}
	}

	// Then, fetch data for the specific stream
	// Note: At this point, all directives should be DirectiveSpecific because wildcards
	// are resolved to concrete specifications at scheduler startup. We no longer need to handle wildcards here.
	if directive.Type != config.DirectiveSpecific {
		return fmt.Errorf("unexpected directive type in refresh: %s (should be specific after resolution)", directive.Type)
	}

	events, err := s.fetchSpecificStream(ctx, directive, fromTime)

	if err != nil {
		return fmt.Errorf("fetch stream data: %w", err)
	}

	// Finally, store events with automatic duplicate detection
	if len(events) > 0 {
		if err := s.cacheDB.CacheEvents(ctx, events); err != nil {
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

	return nil
}

// fetchSpecificStream fetches data for a specific stream
func (s *CacheScheduler) fetchSpecificStream(ctx context.Context, directive config.CacheDirective, fromTime *int64) ([]internal.CachedEvent, error) {
	// Determine the action to call based on stream type
	// For now, we'll use get_record_composed as the primary action
	action := "get_record_composed"

	// Build arguments for the action call
	args := []any{
		directive.DataProvider,
		directive.StreamID,
		fromTime, // from timestamp
		nil,      // to timestamp (fetch all available)
		nil,      // frozen_at (not applicable for cache refresh)
	}

	var events []internal.CachedEvent

	// Execute the action to fetch stream data
	result, err := s.app.Engine.CallWithoutEngineCtx(
		ctx,
		s.app.DB,
		s.namespace, // configurable database namespace
		action,
		args,
		func(row *common.Row) error {
			// Parse each row into a CachedEvent
			if len(row.Values) >= 2 {
				// Parse event time using utility function
				eventTime, err := parseEventTime(row.Values[0])
				if err != nil {
					return fmt.Errorf("parse event_time: %w", err)
				}

				// Then parse the value using utility function
				value, err := parseEventValue(row.Values[1])
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
