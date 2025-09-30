package metrics

import (
	"context"
	"time"
)

// NoOpMetrics is a no-op implementation of MetricsRecorder.
// It does nothing and has zero overhead.
type NoOpMetrics struct{}

// NewNoOpMetrics creates a new no-op metrics recorder.
func NewNoOpMetrics() MetricsRecorder {
	return &NoOpMetrics{}
}

func (n *NoOpMetrics) RecordVacuumStart(ctx context.Context, mechanism string) {}
func (n *NoOpMetrics) RecordVacuumComplete(ctx context.Context, mechanism string, duration time.Duration, tablesProcessed int) {
}
func (n *NoOpMetrics) RecordVacuumError(ctx context.Context, mechanism string, errType string) {}
func (n *NoOpMetrics) RecordVacuumSkipped(ctx context.Context, reason string)                  {}
func (n *NoOpMetrics) RecordLastRunHeight(ctx context.Context, height int64)                   {}

