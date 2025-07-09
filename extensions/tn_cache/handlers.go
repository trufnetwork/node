package tn_cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	return resultFn([]any{GetExtension().IsEnabled()})
}

// checkExtensionEnabled checks if the extension is enabled
func checkExtensionEnabled() error {
	if !GetExtension().IsEnabled() {
		return fmt.Errorf(errExtensionNotEnabled)
	}
	return nil
}

// checkCacheDB returns the cached database connection
func checkCacheDB() (sql.DB, error) {
	db := getWrappedCacheDB()
	if db == nil {
		return nil, fmt.Errorf(errCacheDBNotInitialized)
	}
	return db, nil
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
	dec.SetPrecisionAndScale(numericPrecision, numericScale)
	return dec, nil
}

// processSingleRowResult handles the common pattern of returning a single (event_time, value) row
func processSingleRowResult(result *sql.ResultSet, resultFn func([]any) error) error {
	if len(result.Rows) > 0 {
		row := result.Rows[0]
		dec, err := ensureDecimalValue(row[1])
		if err != nil {
			return err
		}
		return resultFn([]any{row[0], dec})
	}
	// No record found - this is not an error, just no data
	return nil
}

// createStreamOperationContext sets up tracing for stream operations
func createStreamOperationContext(ctx context.Context, op tracing.Operation, dataProvider, streamID string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	return tracing.StreamOperation(ctx, op, dataProvider, streamID, attrs...)
}

// buildTimeAttributes creates tracing attributes for time parameters
func buildTimeAttributes(fromTime, toTime *int64) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	if fromTime != nil {
		attrs = append(attrs, attribute.Int64("from", *fromTime))
	}
	if toTime != nil {
		attrs = append(attrs, attribute.Int64("to", *toTime))
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
func HandleHasCachedData(ctx *common.EngineContext, app *common.App, inputs []any, resultFn func([]any) error) (err error) {
	if err := checkExtensionEnabled(); err != nil {
		return err
	}

	// Extract parameters
	dataProvider := normalizeDataProvider(inputs[0].(string))
	streamID := inputs[1].(string)
	fromTime := extractTimeParameter(inputs[2])
	toTime := extractTimeParameter(inputs[3])

	// Set up tracing
	attrs := buildTimeAttributes(fromTime, toTime)
	traceCtx, end := createStreamOperationContext(ctx.TxContext.Ctx, tracing.OpCacheCheck, dataProvider, streamID, attrs...)
	defer func() { end(err) }()
	ctx.TxContext.Ctx = traceCtx

	// Check cached_streams table to see if we have this stream cached
	db, ok := app.DB.(sql.DB)
	if !ok {
		return fmt.Errorf("app.DB is not a sql.DB")
	}

	// Check if stream is configured and has been refreshed
	var configuredFromTime *int64
	var lastRefreshed *int64
	var lastRefreshedTimestamp int64

	result, err := db.Execute(ctx.TxContext.Ctx, `
		SELECT 
			from_timestamp, 
			COALESCE(last_refreshed, 0) as last_refreshed
		FROM `+constants.CacheSchemaName+`.cached_streams
		WHERE data_provider = $1 AND stream_id = $2
	`, dataProvider, streamID)
	if err != nil {
		return fmt.Errorf("failed to query cached_streams: %w", err)
	}

	if len(result.Rows) == 0 {
		// Stream not configured for caching
		return resultFn([]any{false, int64(0)})
	}

	row := result.Rows[0]
	if row[0] != nil {
		t := row[0].(int64)
		configuredFromTime = &t
	}
	if row[1] != nil {
		lastRefreshedTimestamp = row[1].(int64)
		if lastRefreshedTimestamp > 0 {
			lastRefreshed = &lastRefreshedTimestamp
		}
	}

	// If stream hasn't been refreshed yet, no cached data
	if lastRefreshedTimestamp == 0 {
		return resultFn([]any{false, int64(0)})
	}

	// Special case: if both from and to are NULL, user wants latest value only
	if fromTime == nil && toTime == nil {
		// Just check if we have any data at all
		result, err = db.Execute(ctx.TxContext.Ctx, `
			SELECT COUNT(*) > 0 FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2
			LIMIT 1
		`, dataProvider, streamID)
		if err != nil {
			return fmt.Errorf("failed to check for any cached events: %w", err)
		}
		hasData := false
		if len(result.Rows) > 0 && result.Rows[0][0] != nil {
			hasData = result.Rows[0][0].(bool)
		}
		return resultFn([]any{hasData, lastRefreshedTimestamp})
	}

	// If from is NULL but to is not, treat from as 0 (beginning of time)
	effectiveFrom := int64(0)
	if fromTime != nil {
		effectiveFrom = *fromTime
	}

	// Check if requested from_time is within our cached range
	if configuredFromTime != nil && effectiveFrom < *configuredFromTime {
		// Requested time is before what we have cached
		return resultFn([]any{false, int64(0)})
	}

	// At this point, we know the stream is configured and has been refreshed
	// Check if we actually have events in the requested range
	var eventCount int64
	if toTime == nil {
		// No upper bound specified - to is treated as max_int8 (end of time)
		result, err = db.Execute(ctx.TxContext.Ctx, `
			SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3
		`, dataProvider, streamID, effectiveFrom)
	} else {
		// Upper bound specified
		result, err = db.Execute(ctx.TxContext.Ctx, `
			SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3 AND event_time <= $4
		`, dataProvider, streamID, effectiveFrom, *toTime)
	}

	if err != nil {
		return fmt.Errorf("failed to count cached events: %w", err)
	}

	if len(result.Rows) > 0 {
		eventCount = result.Rows[0][0].(int64)
	}

	// Record cache hit/miss metric
	hasData := eventCount > 0
	ext := GetExtension()
	if hasData {
		ext.MetricsRecorder().RecordCacheHit(ctx.TxContext.Ctx, dataProvider, streamID)

		// Calculate and record data age since we already have lastRefreshed
		if lastRefreshed != nil && *lastRefreshed > 0 {
			refreshTime := time.Unix(*lastRefreshed, 0)
			dataAge := time.Since(refreshTime).Seconds()
			ext.MetricsRecorder().RecordCacheDataAge(ctx.TxContext.Ctx, dataProvider, streamID, dataAge)
		}
	} else {
		ext.MetricsRecorder().RecordCacheMiss(ctx.TxContext.Ctx, dataProvider, streamID)
	}

	return resultFn([]any{hasData, lastRefreshedTimestamp})
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

	// Set up tracing
	attrs := buildTimeAttributes(fromTime, toTime)
	traceCtx, end := createStreamOperationContext(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID, attrs...)

	// Track results for hit/miss recording
	var rowCount int
	defer func() {
		end(nil)
		// Record hit/miss in span
		span := trace.SpanFromContext(traceCtx)
		span.SetAttributes(
			attribute.Bool("cache.hit", rowCount > 0),
			attribute.Int("cache.rows", rowCount),
		)
	}()

	// Update context for tracing
	ctx.TxContext.Ctx = traceCtx

	// Get database connection
	db, err := checkCacheDB()
	if err != nil {
		return err
	}

	var result *sql.ResultSet

	// Special case: if both from and to are NULL, return latest value only
	if fromTime == nil && toTime == nil {
		result, err = db.Execute(ctx.TxContext.Ctx, `
			SELECT data_provider, stream_id, event_time, value
			FROM `+constants.CacheSchemaName+`.cached_events
			WHERE data_provider = $1 AND stream_id = $2
			ORDER BY event_time DESC
			LIMIT 1
		`, dataProvider, streamID)
	} else if toTime != nil {
		// Both bounds specified OR from is NULL with to specified
		result, err = db.Execute(ctx.TxContext.Ctx, `
			WITH anchor_record AS (
				SELECT data_provider, stream_id, event_time, value
				FROM `+constants.CacheSchemaName+`.cached_events
				WHERE data_provider = $1 AND stream_id = $2
					AND event_time <= $3
				ORDER BY event_time DESC
				LIMIT 1
			),
			interval_records AS (
				SELECT data_provider, stream_id, event_time, value
				FROM `+constants.CacheSchemaName+`.cached_events
				WHERE data_provider = $1 AND stream_id = $2
					AND event_time > $3 AND event_time <= $4
			),
			combined_results AS (
				SELECT * FROM anchor_record
				UNION ALL
				SELECT * FROM interval_records
			)
			SELECT data_provider, stream_id, event_time, value
			FROM combined_results
			ORDER BY event_time ASC
		`, dataProvider, streamID, fromTimeOrZero(fromTime), *toTime)
	} else {
		// Only from specified, to is NULL (treat as max_int8)
		result, err = db.Execute(ctx.TxContext.Ctx, `
			WITH anchor_record AS (
				SELECT data_provider, stream_id, event_time, value
				FROM `+constants.CacheSchemaName+`.cached_events
				WHERE data_provider = $1 AND stream_id = $2
					AND event_time <= $3
				ORDER BY event_time DESC
				LIMIT 1
			),
			interval_records AS (
				SELECT data_provider, stream_id, event_time, value
				FROM `+constants.CacheSchemaName+`.cached_events
				WHERE data_provider = $1 AND stream_id = $2
					AND event_time > $3
			),
			combined_results AS (
				SELECT * FROM anchor_record
				UNION ALL
				SELECT * FROM interval_records
			)
			SELECT data_provider, stream_id, event_time, value
			FROM combined_results
			ORDER BY event_time ASC
		`, dataProvider, streamID, fromTimeOrZero(fromTime))
	}

	if err != nil {
		return fmt.Errorf("failed to query cached events: %w", err)
	}

	// Record metrics for data served
	rowCount = len(result.Rows)
	if rowCount > 0 {
		GetExtension().MetricsRecorder().RecordCacheDataServed(ctx.TxContext.Ctx, dataProvider, streamID, rowCount)
	}

	// Return each row via resultFn
	for _, row := range result.Rows {
		// Ensure value is properly formatted
		_, err := ensureDecimalValue(row[3])
		if err != nil {
			return err
		}
		if err := resultFn(row); err != nil {
			return err
		}
	}

	return nil
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

	// Set up tracing
	var attrs []attribute.KeyValue
	if before != nil {
		attrs = append(attrs, attribute.Int64("before", *before))
	}
	traceCtx, end := createStreamOperationContext(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID, attrs...)
	defer end(nil)
	ctx.TxContext.Ctx = traceCtx

	// Get database connection
	db, err := checkCacheDB()
	if err != nil {
		return err
	}

	// Default to max_int8 if before is NULL
	effectiveBefore := maxInt8
	if before != nil {
		effectiveBefore = *before
	}

	// Query for the last record before the timestamp
	result, err := db.Execute(ctx.TxContext.Ctx, `
		SELECT event_time, value
		FROM `+constants.CacheSchemaName+`.cached_events
		WHERE data_provider = $1 AND stream_id = $2 AND event_time < $3
		ORDER BY event_time DESC
		LIMIT 1
	`, dataProvider, streamID, effectiveBefore)

	if err != nil {
		return fmt.Errorf("failed to query cached events: %w", err)
	}

	return processSingleRowResult(result, resultFn)
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

	// Set up tracing
	var attrs []attribute.KeyValue
	if after != nil {
		attrs = append(attrs, attribute.Int64("after", *after))
	}
	traceCtx, end := createStreamOperationContext(ctx.TxContext.Ctx, tracing.OpCacheGet, dataProvider, streamID, attrs...)
	defer end(nil)
	ctx.TxContext.Ctx = traceCtx

	// Get database connection
	db, err := checkCacheDB()
	if err != nil {
		return err
	}

	// Default to 0 if after is NULL
	effectiveAfter := int64(0)
	if after != nil {
		effectiveAfter = *after
	}

	// Query for the first record after the timestamp
	result, err := db.Execute(ctx.TxContext.Ctx, `
		SELECT event_time, value
		FROM `+constants.CacheSchemaName+`.cached_events
		WHERE data_provider = $1 AND stream_id = $2 AND event_time >= $3
		ORDER BY event_time ASC
		LIMIT 1
	`, dataProvider, streamID, effectiveAfter)

	if err != nil {
		return fmt.Errorf("failed to query cached events: %w", err)
	}

	return processSingleRowResult(result, resultFn)
}