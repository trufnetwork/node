// Package metrics provides observability for the tn_cache extension.
// It uses a plugin pattern to ensure zero overhead when OpenTelemetry is not available.
package metrics

import (
	"context"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
	"go.opentelemetry.io/otel"
)

// MetricsRecorder defines the interface for recording cache metrics.
// This allows for pluggable implementations - either real OTEL metrics or no-op.
type MetricsRecorder interface {
	// Cache effectiveness metrics
	RecordCacheHit(ctx context.Context, dataProvider, streamID string)
	RecordCacheMiss(ctx context.Context, dataProvider, streamID string)
	RecordCacheDataServed(ctx context.Context, dataProvider, streamID string, rowCount int)
	RecordCacheDataAge(ctx context.Context, dataProvider, streamID string, ageSeconds float64)

	// Refresh operation metrics
	RecordRefreshStart(ctx context.Context, dataProvider, streamID string)
	RecordRefreshComplete(ctx context.Context, dataProvider, streamID string, duration time.Duration, eventCount int)
	RecordRefreshError(ctx context.Context, dataProvider, streamID string, errType string)

	// Resource metrics
	RecordStreamConfigured(ctx context.Context, count int)
	RecordStreamActive(ctx context.Context, count int)
	RecordCacheSize(ctx context.Context, dataProvider, streamID string, eventCount int64)

	// Resolution metrics
	RecordResolutionDuration(ctx context.Context, duration time.Duration, streamCount int)
	RecordResolutionError(ctx context.Context, errType string)
}

// NewMetricsRecorder creates a metrics recorder instance.
// It automatically detects if OpenTelemetry is available and returns
// either a real OTEL implementation or a no-op implementation.
func NewMetricsRecorder(logger log.Logger) MetricsRecorder {
	// Try to get the global meter provider
	meter := otel.GetMeterProvider().Meter("github.com/trufnetwork/kwil-db/extensions/tn_cache")

	// Try to create a test metric to verify OTEL is functional
	_, err := meter.Int64Counter("tn_cache.test")
	if err != nil {
		logger.Debug("OpenTelemetry not available, metrics disabled")
		return NewNoOpMetrics()
	}

	// OTEL is available, create real metrics recorder
	otelMetrics, err := NewOTELMetrics(meter, logger)
	if err != nil {
		logger.Warn("failed to initialize OTEL metrics, falling back to no-op", "error", err)
		return NewNoOpMetrics()
	}

	logger.Info("OpenTelemetry metrics initialized successfully")
	return otelMetrics
}

// ClassifyError categorizes errors for metric labels to keep cardinality low
func ClassifyError(err error) string {
	if err == nil {
		return "none"
	}

	// Add error classification logic here based on your error types
	// For now, return a generic classification
	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "context deadline exceeded"):
		return "timeout"
	case strings.Contains(errStr, "context canceled"):
		return "cancelled"
	case strings.Contains(errStr, "tx is closed"):
		return "connection_error"
	case strings.Contains(errStr, "no rows in result set"):
		return "not_found"
	default:
		return "unknown"
	}
}