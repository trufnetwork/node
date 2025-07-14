// Package tracing provides centralized middleware for OpenTelemetry tracing and metrics.
// This reduces boilerplate across the tn_cache extension by wrapping common patterns.
package tracing

import (
	"context"
	"time"

	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// TracedOperation wraps a function with automatic tracing for stream operations.
// It starts a span, executes the function, and properly ends the span with any error.
// The generic type T allows preserving type safety for the return value.
func TracedOperation[T any](ctx context.Context, op Operation, provider, streamID string,
	fn func(context.Context) (T, error), attrs ...attribute.KeyValue) (T, error) {
	traceCtx, end := StreamOperation(ctx, op, provider, streamID, attrs...)
	defer func() {
		// Ensure span ends even if fn panics
		if r := recover(); r != nil {
			end(nil)
			panic(r)
		}
	}()

	result, err := fn(traceCtx)
	end(err)
	return result, err
}

// TracedDatabaseOperation wraps database operations with tracing.
// Similar to TracedOperation but uses DatabaseOperation for non-stream-specific operations.
func TracedDatabaseOperation[T any](ctx context.Context, op Operation,
	fn func(context.Context) (T, error), attrs ...attribute.KeyValue) (T, error) {
	traceCtx, end := DatabaseOperation(ctx, op, attrs...)
	defer func() {
		if r := recover(); r != nil {
			end(nil)
			panic(r)
		}
	}()

	result, err := fn(traceCtx)
	end(err)
	return result, err
}

// TracedSchedulerOperation wraps scheduler operations with tracing.
func TracedSchedulerOperation[T any](ctx context.Context, op Operation,
	fn func(context.Context) (T, error), attrs ...attribute.KeyValue) (T, error) {
	traceCtx, end := SchedulerOperation(ctx, op, attrs...)
	defer func() {
		if r := recover(); r != nil {
			end(nil)
			panic(r)
		}
	}()

	result, err := fn(traceCtx)
	end(err)
	return result, err
}

// TracedTNOperation wraps TrufNetwork API operations with tracing.
func TracedTNOperation[T any](ctx context.Context, op Operation, action string,
	fn func(context.Context) (T, error), attrs ...attribute.KeyValue) (T, error) {
	traceCtx, end := TNOperation(ctx, op, action, attrs...)
	defer func() {
		if r := recover(); r != nil {
			end(nil)
			panic(r)
		}
	}()

	result, err := fn(traceCtx)
	end(err)
	return result, err
}

// MetricsConfig holds common parameters for metrics recording
type MetricsConfig struct {
	Provider string
	StreamID string
	Context  context.Context
	Recorder metrics.MetricsRecorder // Optional metrics recorder
}

// WithCacheMetrics wraps an operation and records data served metrics.
// This should only be used for data fetching operations after cache hit/miss
// has already been determined and recorded by HandleHasCachedData.
func WithCacheMetrics(config MetricsConfig, fn func() (int, error)) (int, error) {
	if config.Recorder == nil {
		return fn()
	}

	count, err := fn()

	// Record data served metrics on successful fetch
	if err == nil {
		config.Recorder.RecordCacheDataServed(config.Context, config.Provider, config.StreamID, count)
	}

	return count, err
}

// WithRefreshMetrics wraps a refresh operation and records timing and result metrics.
func WithRefreshMetrics(config MetricsConfig, fn func() (int, error)) (int, error) {
	if config.Recorder == nil {
		return fn()
	}

	// Record start
	config.Recorder.RecordRefreshStart(config.Context, config.Provider, config.StreamID)
	start := time.Now()

	count, err := fn()
	duration := time.Since(start)

	// Record completion or error
	if err == nil {
		config.Recorder.RecordRefreshComplete(config.Context, config.Provider, config.StreamID, duration, count)
	} else {
		errType := classifyError(err)
		config.Recorder.RecordRefreshError(config.Context, config.Provider, config.StreamID, errType)
	}

	return count, err
}

// TracedWithCacheMetrics combines tracing and cache data served metrics.
// The function should return (result, rowCount, error).
// This assumes cache hit/miss has already been determined by HandleHasCachedData.
func TracedWithCacheMetrics[T any](ctx context.Context, op Operation, provider, streamID string,
	recorder metrics.MetricsRecorder, fn func(context.Context) (T, int, error), attrs ...attribute.KeyValue) (T, error) {
	var result T
	var count int

	res, err := TracedOperation(ctx, op, provider, streamID,
		func(traceCtx context.Context) (T, error) {
			var fnErr error
			result, count, fnErr = fn(traceCtx)

			// Add count to span attributes
			if span := trace.SpanFromContext(traceCtx); span.IsRecording() {
				span.SetAttributes(
					attribute.Int("cache.rows", count),
				)
			}

			return result, fnErr
		}, attrs...)

	// Record data served metrics
	if err == nil && recorder != nil {
		metricsConfig := MetricsConfig{
			Provider: provider,
			StreamID: streamID,
			Context:  ctx,
			Recorder: recorder,
		}
		WithCacheMetrics(metricsConfig, func() (int, error) {
			return count, nil
		})
	}

	return res, err
}

// TracedWithRefreshMetrics combines tracing and refresh metrics.
func TracedWithRefreshMetrics[T any](ctx context.Context, op Operation, provider, streamID string,
	recorder metrics.MetricsRecorder, fn func(context.Context) (T, int, error), attrs ...attribute.KeyValue) (T, error) {
	var result T
	var count int

	metricsConfig := MetricsConfig{
		Provider: provider,
		StreamID: streamID,
		Context:  ctx,
		Recorder: recorder,
	}

	// Wrap with refresh metrics
	_, err := WithRefreshMetrics(metricsConfig, func() (int, error) {
		// Then wrap with tracing
		var innerErr error
		result, innerErr = TracedOperation(ctx, op, provider, streamID,
			func(traceCtx context.Context) (T, error) {
				var fnErr error
				result, count, fnErr = fn(traceCtx)

				// Add count to span
				if span := trace.SpanFromContext(traceCtx); span.IsRecording() {
					span.SetAttributes(attribute.Int("event_count", count))
				}

				return result, fnErr
			}, attrs...)

		return count, innerErr
	})

	return result, err
}

// classifyError categorizes errors for metrics to keep cardinality low
func classifyError(err error) string {
	if err == nil {
		return "none"
	}

	// Use the ClassifyError function from metrics package
	return metrics.ClassifyError(err)
}

// NoOpEndFunc is a no-op function for cases where tracing is disabled
var NoOpEndFunc = func(error) {}

// IsTracingEnabled checks if tracing is enabled via configuration
// This should be configured based on the extension's state when initializing middleware
func IsTracingEnabled(enabled bool) bool {
	return enabled
}

// ConditionalTrace returns either a real trace operation or no-op based on config
func ConditionalTrace(ctx context.Context, op Operation, provider, streamID string, enabled bool,
	attrs ...attribute.KeyValue) (context.Context, func(error)) {
	if !enabled {
		return ctx, NoOpEndFunc
	}
	return StreamOperation(ctx, op, provider, streamID, attrs...)
}
