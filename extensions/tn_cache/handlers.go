package tn_cache

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_cache/internal"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
	"go.opentelemetry.io/otel/attribute"
)

// Constants needed by handlers
const (
	// SQL constants
	maxInt8          = int64(9223372036854775000) // Default for NULL upper bounds
	numericPrecision = 36
	numericScale     = 18

	// Error messages
	errExtensionNotEnabled   = "tn_cache extension is not enabled"
	errCacheDBNotInitialized = "cache database not initialized"
	errValueNotDecimal       = "value is not a decimal. received %T"
)

// HandleIsEnabled handles the is_enabled precompile method
func HandleIsEnabled(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	ext := GetExtension()
	enabled := ext != nil && ext.IsEnabled()
	return resultFn([]any{enabled})
}

// checkExtensionEnabled checks if the extension is enabled
func checkExtensionEnabled() error {
	if _, err := safeGetExtension(); err != nil {
		return fmt.Errorf("tn_cache extension not available: %w", err)
	}
	return nil
}

// Helper functions for handlers

func extractTimeParameter(input interface{}) *int64 {
	if input == nil {
		return nil
	}
	t := input.(int64)
	return &t
}

func normalizeDataProvider(input string) string {
	return strings.ToLower(input)
}

func ensureDecimalValue(value interface{}) (*types.Decimal, error) {
	dec, ok := value.(*types.Decimal)
	if !ok {
		return nil, fmt.Errorf(errValueNotDecimal, value)
	}
	if err := dec.SetPrecisionAndScale(numericPrecision, numericScale); err != nil {
		return nil, fmt.Errorf("failed to set precision and scale: %w", err)
	}
	return dec, nil
}

// buildTimeAttributes creates tracing attributes for time parameters
func buildTimeAttributes(fromTime, toTime, baseTime *int64) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	if fromTime != nil {
		attrs = append(attrs, attribute.Int64("from", *fromTime))
	}
	if toTime != nil {
		attrs = append(attrs, attribute.Int64("to", *toTime))
	}
	if baseTime != nil {
		attrs = append(attrs, attribute.Int64("base_time", *baseTime))
	}
	return attrs
}

// fromTimeOrZero returns the fromTime value or 0 if nil
func fromTimeOrZero(fromTime *int64) int64 {
	if fromTime == nil {
		return 0
	}
	return *fromTime
}

// HandleHasCachedData checks if we have cached data for a stream in the given time range
func HandleHasCachedData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	return handleHasCachedData(ctx, app, inputs, resultFn, false)
}

// HandleHasCachedIndexData checks cache availability for index lookups that are base_time aware
func HandleHasCachedIndexData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	return handleHasCachedData(ctx, app, inputs, resultFn, true)
}

func handleHasCachedData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error, respectBaseTime bool) error {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	fromTime := extractTimeParameter(inputs[2])
	toTime := extractTimeParameter(inputs[3])
	var baseTime *int64
	if len(inputs) > 4 {
		baseTime = extractTimeParameter(inputs[4])
	}
	if !respectBaseTime {
		baseTime = nil
	}

	// Use middleware for tracing
	attrs := buildTimeAttributes(fromTime, toTime, baseTime)
	result, err := tracing.TracedOperation(ctx.TxContext.Ctx, tracing.OpCacheCheck, dataProvider, streamID,
		func(traceCtx context.Context) ([]any, error) {
			ctx.TxContext.Ctx = traceCtx

			// Get CacheDB instance
			cacheDB, err := checkCacheDB()
			if err != nil {
				return nil, err
			}

			// Get stream configuration using CacheDB abstraction
			config, err := cacheDB.GetStreamConfigWithBaseTime(traceCtx, dataProvider, streamID, baseTime)
			if err != nil {
				if err == sql.ErrNoRows {
					// Stream not configured for caching
					if ext := GetExtension(); ext != nil && ext.IsEnabled() {
						ext.MetricsRecorder().RecordCacheMiss(traceCtx, dataProvider, streamID, baseTime)
					}
					return []any{false, int64(0), int64(0)}, nil
				}
				return nil, fmt.Errorf("failed to get stream config: %w", err)
			}

			// If stream hasn't been refreshed yet, no cached data
			if config.CacheRefreshedAtTimestamp == 0 {
				if ext := GetExtension(); ext != nil && ext.IsEnabled() {
					ext.MetricsRecorder().RecordCacheMiss(traceCtx, dataProvider, streamID, baseTime)
				}
				return []any{false, int64(0), int64(0)}, nil
			}

			// Special case: if both from and to are NULL, user wants latest value only
			if fromTime == nil && toTime == nil {
				if baseTime != nil && respectBaseTime {
					// For base_time-specific probes, confirm we actually have cached data
					if _, err := cacheDB.GetLastEventBefore(traceCtx, dataProvider, streamID, baseTime, math.MaxInt64); err != nil {
						if err == sql.ErrNoRows {
							if ext := GetExtension(); ext != nil && ext.IsEnabled() {
								ext.MetricsRecorder().RecordCacheMiss(traceCtx, dataProvider, streamID, baseTime)
							}
							return []any{false, int64(0), int64(0)}, nil
						}
						return nil, fmt.Errorf("check latest cached event: %w", err)
					}
				}
				// We consider it cached if the stream has been refreshed, even if empty
				hasData := true
				if ext := GetExtension(); ext != nil && ext.IsEnabled() {
					ext.MetricsRecorder().RecordCacheHit(traceCtx, dataProvider, streamID, baseTime)

					// Calculate and record data age
					refreshTime := time.Unix(config.CacheRefreshedAtTimestamp, 0)
					dataAge := time.Since(refreshTime).Seconds()
					ext.MetricsRecorder().RecordCacheDataAge(traceCtx, dataProvider, streamID, baseTime, dataAge)
				}
				return []any{hasData, config.CacheRefreshedAtTimestamp, config.CacheHeight}, nil
			}

			// If from is NULL but to is not, treat from as 0 (beginning of time)
			effectiveFrom := int64(0)
			if fromTime != nil {
				effectiveFrom = *fromTime
			}

			// Check if requested from_time is within our cached range
			if effectiveFrom < config.FromTimestamp {
				// Requested time is before what we have cached
				if ext := GetExtension(); ext != nil && ext.IsEnabled() {
					ext.MetricsRecorder().RecordCacheMiss(traceCtx, dataProvider, streamID, baseTime)
				}
				return []any{false, int64(0), int64(0)}, nil
			}

			// At this point, we know the stream is configured, has been refreshed,
			// and the requested range starts after our configured from time
			// We consider it cached even if there are no events in the range
			hasData := true

			// Record metrics for cache hit/miss decision
			if ext := GetExtension(); ext != nil && ext.IsEnabled() {
				ext.MetricsRecorder().RecordCacheHit(traceCtx, dataProvider, streamID, baseTime)

				// Calculate and record data age
				refreshTime := time.Unix(config.CacheRefreshedAtTimestamp, 0)
				dataAge := time.Since(refreshTime).Seconds()
				ext.MetricsRecorder().RecordCacheDataAge(traceCtx, dataProvider, streamID, baseTime, dataAge)
			}

			return []any{hasData, config.CacheRefreshedAtTimestamp, config.CacheHeight}, nil
		}, attrs...)

	if err != nil {
		return err
	}

	return resultFn(result)
}

// HandleGetCachedData handles the get_cached_data precompile method
func HandleGetCachedData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	fromTime := extractTimeParameter(inputs[2])
	toTime := extractTimeParameter(inputs[3])
	var baseTime *int64
	if len(inputs) > 4 {
		baseTime = extractTimeParameter(inputs[4])
	}
	// Record queries ignore base_time sharding and always use the sentinel variant.
	baseTime = nil

	// Use middleware for tracing and metrics
	attrs := buildTimeAttributes(fromTime, toTime, baseTime)
	ext := GetExtension()
	var recorder metrics.MetricsRecorder
	if ext != nil && ext.IsEnabled() {
		recorder = ext.MetricsRecorder()
	}
	_, err := tracing.TracedWithCacheMetrics(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID, baseTime, recorder,
		func(traceCtx context.Context) (any, int, error) {
			// Update context for tracing
			ctx.TxContext.Ctx = traceCtx

			// This handler assumes the caller has already verified cache availability
			// via HandleHasCachedData. We proceed directly to fetching data.

			// Obtain CacheDB instance
			cacheDB, err := checkCacheDB()
			if err != nil {
				return nil, 0, err
			}

			// Helper to emit a single CachedEvent
			emit := func(ev *internal.CachedEvent) error {
				dec, err := ensureDecimalValue(ev.Value)
				if err != nil {
					return err
				}
				return resultFn([]any{ev.DataProvider, ev.StreamID, ev.EventTime, dec})
			}

			effectiveFrom := int64(0)
			if fromTime != nil {
				effectiveFrom = *fromTime
			}

			// Case 1: latest value only (both bounds nil)
			if fromTime == nil && toTime == nil {
				event, err := cacheDB.GetLastEventBefore(traceCtx, dataProvider, streamID, baseTime, math.MaxInt64)
				if err != nil {
					if err == sql.ErrNoRows {
						return nil, 0, nil // no data
					}
					return nil, 0, fmt.Errorf("get latest cached event: %w", err)
				}
				if err := emit(event); err != nil {
					return nil, 0, err
				}
				return nil, 1, nil
			}

			// Case 2/3: range queries with anchor logic
			var combined []*internal.CachedEvent

			// anchor record (<= effectiveFrom)
			anchor, err := cacheDB.GetLastEventBefore(traceCtx, dataProvider, streamID, baseTime, effectiveFrom+1)
			if err != nil && err != sql.ErrNoRows {
				return nil, 0, fmt.Errorf("get anchor event: %w", err)
			}
			if err == nil {
				combined = append(combined, anchor)
			}

			effectiveTo := int64(0)
			if toTime != nil {
				effectiveTo = *toTime
			}

			events, err := cacheDB.GetCachedEventsWithBaseTime(traceCtx, dataProvider, streamID, baseTime, effectiveFrom, effectiveTo)
			if err != nil {
				return nil, 0, fmt.Errorf("get interval events: %w", err)
			}

			for i := range events {
				ev := events[i]
				if ev.EventTime > effectiveFrom {
					combined = append(combined, &ev)
				}
			}

			// Emit all events
			for _, ev := range combined {
				if err := emit(ev); err != nil {
					return nil, 0, err
				}
			}

			return nil, len(combined), nil
		}, attrs...)

	return err
}

// HandleGetCachedLastBefore handles the get_cached_last_before precompile method
func HandleGetCachedLastBefore(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	before := extractTimeParameter(inputs[2])
	var baseTime *int64
	if len(inputs) > 3 {
		baseTime = extractTimeParameter(inputs[3])
	}
	// Record queries ignore base_time sharding and always use the sentinel variant.
	baseTime = nil

	// Use middleware for tracing
	var attrs []attribute.KeyValue
	if before != nil {
		attrs = append(attrs, attribute.Int64("before", *before))
	}

	result, err := tracing.TracedOperation(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID,
		func(traceCtx context.Context) ([]any, error) {
			ctx.TxContext.Ctx = traceCtx

			// Obtain CacheDB instance
			cacheDB, err := checkCacheDB()
			if err != nil {
				return nil, err
			}

			// Default to max_int8 if before is NULL
			effectiveBefore := maxInt8
			if before != nil {
				effectiveBefore = *before
			}

			event, err := cacheDB.GetLastEventBefore(traceCtx, dataProvider, streamID, baseTime, effectiveBefore)
			if err != nil {
				if err == sql.ErrNoRows {
					return nil, nil // no data to return
				}
				return nil, fmt.Errorf("get last event before: %w", err)
			}

			dec, err := ensureDecimalValue(event.Value)
			if err != nil {
				return nil, err
			}
			return []any{event.EventTime, dec}, nil
		}, attrs...)

	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	return resultFn(result)
}

// HandleGetCachedFirstAfter handles the get_cached_first_after precompile method
func HandleGetCachedFirstAfter(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	after := extractTimeParameter(inputs[2])
	var baseTime *int64
	if len(inputs) > 3 {
		baseTime = extractTimeParameter(inputs[3])
	}
	// Record queries ignore base_time sharding and always use the sentinel variant.
	baseTime = nil

	// Use middleware for tracing
	var attrs []attribute.KeyValue
	if after != nil {
		attrs = append(attrs, attribute.Int64("after", *after))
	}

	result, err := tracing.TracedOperation(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID,
		func(traceCtx context.Context) ([]any, error) {
			ctx.TxContext.Ctx = traceCtx

			// Obtain CacheDB instance
			cacheDB, err := checkCacheDB()
			if err != nil {
				return nil, err
			}

			// Default to 0 if after is NULL
			effectiveAfter := int64(0)
			if after != nil {
				effectiveAfter = *after
			}

			event, err := cacheDB.GetFirstEventAfter(traceCtx, dataProvider, streamID, baseTime, effectiveAfter)
			if err != nil {
				if err == sql.ErrNoRows {
					return nil, nil
				}
				return nil, fmt.Errorf("get first event after: %w", err)
			}

			dec, err := ensureDecimalValue(event.Value)
			if err != nil {
				return nil, err
			}
			return []any{event.EventTime, dec}, nil
		}, attrs...)

	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	return resultFn(result)
}

// HandleGetCachedIndexData retrieves cached index values for a stream
func HandleGetCachedIndexData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) error {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	fromTime := extractTimeParameter(inputs[2])
	toTime := extractTimeParameter(inputs[3])
	var baseTime *int64
	if len(inputs) > 4 {
		baseTime = extractTimeParameter(inputs[4])
	}

	// Use middleware for tracing
	attrs := buildTimeAttributes(fromTime, toTime, baseTime)
	attrs = append(attrs, attribute.String("type", "index"))

	ext := GetExtension()
	var recorder metrics.MetricsRecorder
	if ext != nil && ext.IsEnabled() {
		recorder = ext.MetricsRecorder()
	}

	_, err := tracing.TracedWithCacheMetrics(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID, baseTime, recorder,
		func(traceCtx context.Context) (any, int, error) {
			ctx.TxContext.Ctx = traceCtx

			// This handler assumes the caller has already verified cache availability
			// via HandleHasCachedData. We proceed directly to fetching data.

			ext, err := safeGetExtension()
			if err != nil {
				return nil, 0, fmt.Errorf("extension unavailable: %w", err)
			}
			if ext.cacheDB == nil {
				return nil, 0, fmt.Errorf("%s", errCacheDBNotInitialized)
			}

			// Determine effective time range
			effectiveFromTime := fromTimeOrZero(fromTime)
			effectiveToTime := int64(0)
			if toTime != nil {
				effectiveToTime = *toTime
			}

			// Get index events from cache
			indexEvents, err := ext.cacheDB.GetCachedIndexWithBaseTime(traceCtx, dataProvider, streamID, baseTime, effectiveFromTime, effectiveToTime)
			if err != nil {
				return nil, 0, fmt.Errorf("get index events: %w", err)
			}

			// Return the data in the expected format
			for _, event := range indexEvents {
				dec, err := ensureDecimalValue(event.Value)
				if err != nil {
					return nil, 0, err
				}
				if err := resultFn([]any{event.EventTime, dec}); err != nil {
					return nil, 0, err
				}
			}

			return nil, len(indexEvents), nil
		}, attrs...)

	return err
}

// checkCacheDB returns the CacheDB instance or an error if the cache DB is not initialised.
func checkCacheDB() (*internal.CacheDB, error) {
	ext, err := safeGetExtension()
	if err != nil {
		return nil, fmt.Errorf("extension unavailable: %w", err)
	}
	if ext.CacheDB() == nil {
		return nil, fmt.Errorf("%s", errCacheDBNotInitialized)
	}
	return ext.CacheDB(), nil
}
