package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/node/extensions/tn_cache/metrics"
)

func TestMetricsRecorder(t *testing.T) {
	ctx := context.Background()
	logger := log.NewStdoutLogger()
	
	// Create metrics recorder (will use no-op since OTEL is not initialized in tests)
	recorder := metrics.NewMetricsRecorder(logger)
	assert.NotNil(t, recorder)
	
	// Test that all new metrics methods can be called without errors
	t.Run("RefreshSkipped", func(t *testing.T) {
		// Should not panic
		recorder.RecordRefreshSkipped(ctx, "provider1", "stream1", "not_synced")
		recorder.RecordRefreshSkipped(ctx, "provider2", "stream2", "schedule_miss")
	})
	
	t.Run("ResolutionMetrics", func(t *testing.T) {
		// Should not panic
		recorder.RecordResolutionDuration(ctx, 100*time.Millisecond, 10)
		recorder.RecordResolutionError(ctx, "timeout")
		recorder.RecordResolutionStreamDiscovered(ctx, "provider1", "new_stream")
		recorder.RecordResolutionStreamRemoved(ctx, "provider1", "old_stream")
	})
	
	t.Run("ErrorClassification", func(t *testing.T) {
		testCases := []struct {
			errMsg   string
			expected string
		}{
			{"context deadline exceeded", "timeout"},
			{"context canceled", "cancelled"},
			{"tx is closed", "connection_error"},
			{"connection refused", "connection_error"},
			{"no rows in result set", "not_found"},
			{"not found", "not_found"},
			{"permission denied", "permission_denied"},
			{"unauthorized", "permission_denied"},
			{"invalid input", "validation_error"},
			{"validation failed", "validation_error"},
			{"failed to resolve", "resolution_error"},
			{"resolution error", "resolution_error"},
			{"database error", "database_error"},
			{"sql: no rows", "database_error"},
			{"unknown error", "unknown"},
		}
		
		for _, tc := range testCases {
			t.Run(tc.errMsg, func(t *testing.T) {
				err := assert.AnError
				err = &testError{msg: tc.errMsg}
				result := metrics.ClassifyError(err)
				assert.Equal(t, tc.expected, result)
			})
		}
		
		// Test nil error
		assert.Equal(t, "none", metrics.ClassifyError(nil))
	})
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}