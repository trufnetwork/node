// Package metrics provides observability for the tn_vacuum extension.
// It uses a plugin pattern to ensure zero overhead when OpenTelemetry is not available.
package metrics

import (
	"context"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/core/log"
	"go.opentelemetry.io/otel"
)

// MetricsRecorder defines the interface for recording vacuum metrics.
// This allows for pluggable implementations - either real OTEL metrics or no-op.
type MetricsRecorder interface {
	// Vacuum operation metrics
	RecordVacuumStart(ctx context.Context, mechanism string)
	RecordVacuumComplete(ctx context.Context, mechanism string, duration time.Duration, tablesProcessed int)
	RecordVacuumError(ctx context.Context, mechanism string, errType string)
	RecordVacuumSkipped(ctx context.Context, reason string)

	// Resource metrics
	RecordLastRunHeight(ctx context.Context, height int64)
}

// NewMetricsRecorder creates a metrics recorder instance.
// It automatically detects if OpenTelemetry is available and returns
// either a real OTEL implementation or a no-op implementation.
func NewMetricsRecorder(logger log.Logger) MetricsRecorder {
	// Try to get the global meter provider
	meter := otel.GetMeterProvider().Meter("github.com/trufnetwork/kwil-db/extensions/tn_vacuum")

	// Try to create a test metric to verify OTEL is functional
	_, err := meter.Int64Counter("tn_vacuum.test")
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

	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "context deadline exceeded"):
		return "timeout"
	case strings.Contains(errStr, "context canceled"):
		return "cancelled"
	case strings.Contains(errStr, "connection"):
		return "connection_error"
	case strings.Contains(errStr, "pg_repack unavailable") || strings.Contains(errStr, "not found in PATH"):
		return "binary_unavailable"
	case strings.Contains(errStr, "execution failed"):
		return "execution_failed"
	case strings.Contains(errStr, "database") || strings.Contains(errStr, "sql"):
		return "database_error"
	case strings.Contains(errStr, "permission denied") || strings.Contains(errStr, "unauthorized"):
		return "permission_denied"
	default:
		return "unknown"
	}
}

