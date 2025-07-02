package tn_cache

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
)

// refreshStreamWithRetry refreshes a stream with exponential backoff retry logic using retry-go
func (s *CacheScheduler) refreshStreamWithRetry(ctx context.Context, instruction config.InstructionDirective, maxRetries int) error {
	return retry.Do(
		func() error {
			return s.refreshStream(ctx, instruction)
		},
		retry.Attempts(uint(maxRetries+1)),
		retry.Delay(1*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.MaxDelay(30*time.Second),
		retry.OnRetry(func(n uint, err error) {
			s.logger.Warn("refresh failed, retrying",
				"provider", instruction.DataProvider,
				"stream", instruction.StreamID,
				"attempt", n,
				"error", err)
		}),
		retry.RetryIf(func(err error) bool {
			// Don't retry non-retryable errors
			if isNonRetryableError(err) {
				s.logger.Debug("non-retryable error, not retrying",
					"provider", instruction.DataProvider,
					"stream", instruction.StreamID,
					"error", err)
				return false
			}
			return true
		}),
		retry.Context(ctx),
		retry.LastErrorOnly(true),
	)
}

// isNonRetryableError determines if an error should not be retried
func isNonRetryableError(err error) bool {
	errStr := strings.ToLower(err.Error())
	
	// Add specific error patterns that shouldn't be retried
	nonRetryablePatterns := []string{
		"schema does not exist",
		"permission denied",
		"invalid configuration",
		"context canceled",
		"context deadline exceeded",
		"unauthorized",
		"forbidden",
	}
	
	for _, pattern := range nonRetryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	
	return false
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

// refreshStream refreshes the cache for a single instruction directive
func (s *CacheScheduler) refreshStream(ctx context.Context, instruction config.InstructionDirective) error {
	s.logger.Debug("refreshing stream",
		"provider", instruction.DataProvider,
		"stream", instruction.StreamID,
		"type", instruction.Type)

	// Get stream config to check last refresh time for incremental updates
	streamConfig, err := s.cacheDB.GetStreamConfig(ctx, instruction.DataProvider, instruction.StreamID)
	if err != nil {
		return fmt.Errorf("get stream config: %w", err)
	}

	// Calculate time range for incremental refresh
	fromTime := instruction.TimeRange.From
	if streamConfig != nil && streamConfig.LastRefreshed != "" {
		// Parse last refresh time for incremental update
		lastRefresh, err := time.Parse(time.RFC3339, streamConfig.LastRefreshed)
		if err == nil {
			// Use last refresh time as starting point for incremental refresh
			fromUnix := lastRefresh.Unix()
			fromTime = &fromUnix
		}
	}

	// Fetch data based on instruction type
	var events []internal.CachedEvent
	switch instruction.Type {
	case config.DirectiveSpecific:
		events, err = s.fetchSpecificStream(ctx, instruction, fromTime)
	case config.DirectiveProviderWildcard:
		events, err = s.fetchProviderStreams(ctx, instruction, fromTime)
	default:
		return fmt.Errorf("unknown directive type: %s", instruction.Type)
	}

	if err != nil {
		return fmt.Errorf("fetch stream data: %w", err)
	}

	// Store events with automatic duplicate detection
	if len(events) > 0 {
		if err := s.cacheDB.CacheEvents(ctx, events); err != nil {
			return fmt.Errorf("cache events: %w", err)
		}
		s.logger.Info("cached events", 
			"count", len(events), 
			"provider", instruction.DataProvider,
			"stream", instruction.StreamID)
	} else {
		s.logger.Debug("no new events to cache",
			"provider", instruction.DataProvider,
			"stream", instruction.StreamID)
	}

	return nil
}

// fetchSpecificStream fetches data for a specific stream
func (s *CacheScheduler) fetchSpecificStream(ctx context.Context, instruction config.InstructionDirective, fromTime *int64) ([]internal.CachedEvent, error) {
	// Determine the action to call based on stream type
	// For now, we'll use get_record_composed as the primary action
	action := "get_record_composed"
	
	// Build arguments for the action call
	args := []any{
		instruction.DataProvider,
		instruction.StreamID,
		fromTime,      // from timestamp
		nil,           // to timestamp (fetch all available)
		nil,           // frozen_at (not applicable for cache refresh)
	}

	var events []internal.CachedEvent
	
	// Call the action with override authorization to bypass permission checks
	result, err := s.app.Engine.CallWithoutEngineCtx(
		ctx,
		s.app.DB,
		s.namespace, // configurable database namespace
		action,
		args,
		func(row *common.Row) error {
			// Parse row into CachedEvent
			if len(row.Values) >= 2 {
				// Parse event time using utility function
				eventTime, err := parseEventTime(row.Values[0])
				if err != nil {
					return fmt.Errorf("parse event_time: %w", err)
				}

				// Parse value using utility function
				value, err := parseEventValue(row.Values[1])
				if err != nil {
					return fmt.Errorf("parse value: %w", err)
				}

				event := internal.CachedEvent{
					DataProvider: instruction.DataProvider,
					StreamID:     instruction.StreamID,
					EventTime:    eventTime,
					Value:        value,
				}
				events = append(events, event)
			}
			return nil
		},
	)

	if err != nil {
		return nil, fmt.Errorf("call action %s: %w", action, err)
	}

	s.logger.Debug("fetched stream data", 
		"action", action,
		"events", len(events),
		"provider", instruction.DataProvider,
		"stream", instruction.StreamID)

	// Log any notices from the action execution
	if len(result.Logs) > 0 {
		s.logger.Debug("action logs", "logs", result.FormatLogs())
	}

	return events, nil
}

// fetchProviderStreams fetches data for all streams from a provider (wildcard directive)
func (s *CacheScheduler) fetchProviderStreams(ctx context.Context, instruction config.InstructionDirective, fromTime *int64) ([]internal.CachedEvent, error) {
	// For wildcard directives, we need to:
	// 1. Get all streams for the provider
	// 2. Fetch data for each stream individually
	
	// This is a simplified implementation - in a real scenario, you might want to:
	// - Query a streams registry to get all available streams for a provider
	// - Batch the requests for efficiency
	// - Handle different stream types appropriately

	s.logger.Warn("provider wildcard refresh not fully implemented",
		"provider", instruction.DataProvider,
		"type", instruction.Type)
	
	// For now, return empty events - this should be implemented based on
	// how the TRUF.NETWORK system discovers available streams for a provider
	return []internal.CachedEvent{}, nil
}