package metrics

import (
	"context"
	"time"
)

// NoOpMetrics is a no-op implementation of MetricsRecorder.
// It provides zero overhead when metrics are not needed.
type NoOpMetrics struct{}

// NewNoOpMetrics creates a new no-op metrics recorder
func NewNoOpMetrics() *NoOpMetrics {
	return &NoOpMetrics{}
}

// All methods are empty and will be inlined by the compiler

func (n *NoOpMetrics) RecordCacheHit(ctx context.Context, dataProvider, streamID string, baseTime *int64) {
}

func (n *NoOpMetrics) RecordCacheMiss(ctx context.Context, dataProvider, streamID string, baseTime *int64) {
}

func (n *NoOpMetrics) RecordCacheDataServed(ctx context.Context, dataProvider, streamID string, baseTime *int64, rowCount int) {
}

func (n *NoOpMetrics) RecordCacheDataAge(ctx context.Context, dataProvider, streamID string, baseTime *int64, ageSeconds float64) {
}

func (n *NoOpMetrics) RecordRefreshStart(ctx context.Context, dataProvider, streamID string) {}

func (n *NoOpMetrics) RecordRefreshComplete(ctx context.Context, dataProvider, streamID string, duration time.Duration, eventCount int) {
}

func (n *NoOpMetrics) RecordRefreshError(ctx context.Context, dataProvider, streamID string, errType string) {
}

func (n *NoOpMetrics) RecordStreamConfigured(ctx context.Context, count int) {}

func (n *NoOpMetrics) RecordStreamActive(ctx context.Context, count int) {}

func (n *NoOpMetrics) RecordCacheSize(ctx context.Context, dataProvider, streamID string, eventCount int64) {
}

func (n *NoOpMetrics) RecordResolutionDuration(ctx context.Context, duration time.Duration, streamCount int) {
}

func (n *NoOpMetrics) RecordResolutionError(ctx context.Context, errType string) {}

func (n *NoOpMetrics) RecordResolutionStreamDiscovered(ctx context.Context, dataProvider, streamID string) {
}

func (n *NoOpMetrics) RecordResolutionStreamRemoved(ctx context.Context, dataProvider, streamID string) {
}

func (n *NoOpMetrics) RecordRefreshSkipped(ctx context.Context, dataProvider, streamID, reason string) {
}
