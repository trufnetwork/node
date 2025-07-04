package metrics

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/kwil-db/core/log"
)

func TestNoOpMetrics(t *testing.T) {
	// Test that NoOpMetrics implements the interface and doesn't panic
	metrics := NewNoOpMetrics()
	ctx := context.Background()

	// These should all be no-ops and not panic
	metrics.RecordCacheHit(ctx, "provider1", "stream1")
	metrics.RecordCacheMiss(ctx, "provider1", "stream1")
	metrics.RecordCacheDataServed(ctx, "provider1", "stream1", 100)
	metrics.RecordCacheDataAge(ctx, "provider1", "stream1", 3600.0)
	metrics.RecordRefreshStart(ctx, "provider1", "stream1")
	metrics.RecordRefreshComplete(ctx, "provider1", "stream1", 5*time.Second, 50)
	metrics.RecordRefreshError(ctx, "provider1", "stream1", "timeout")
	metrics.RecordCircuitBreakerStateChange(ctx, "provider1", "stream1", gobreaker.StateClosed, gobreaker.StateOpen)
	metrics.RecordStreamConfigured(ctx, 10)
	metrics.RecordStreamActive(ctx, 8)
	metrics.RecordCacheSize(ctx, "provider1", "stream1", 1000)
	metrics.RecordResolutionDuration(ctx, 2*time.Second, 5)
	metrics.RecordResolutionError(ctx, "not_found")

	// If we get here without panics, the test passes
	assert.True(t, true)
}

func TestMetricsRecorderFactory(t *testing.T) {
	// Test that the factory returns a valid MetricsRecorder
	logger := log.New(log.WithWriter(io.Discard))
	metrics := NewMetricsRecorder(logger)

	// The factory should return either NoOpMetrics or OTELMetrics
	// In test environments, it might return either depending on OTEL availability
	assert.NotNil(t, metrics, "NewMetricsRecorder should not return nil")
	
	// Verify it implements the interface by calling a method
	ctx := context.Background()
	// This should not panic regardless of which implementation is returned
	metrics.RecordCacheHit(ctx, "test", "test")
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "none",
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: "timeout",
		},
		{
			name:     "generic error",
			err:      assert.AnError,
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifyError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}