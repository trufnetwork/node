package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
)

func TestNoOpMetrics(t *testing.T) {
	ctx := context.Background()
	m := NewNoOpMetrics()

	// Should not panic
	m.RecordVacuumStart(ctx, "test")
	m.RecordVacuumComplete(ctx, "test", time.Second, 5)
	m.RecordVacuumError(ctx, "test", "error")
	m.RecordVacuumSkipped(ctx, "reason")
	m.RecordLastRunHeight(ctx, 100)
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
			name:     "timeout error",
			err:      errors.New("context deadline exceeded"),
			expected: "timeout",
		},
		{
			name:     "cancelled error",
			err:      errors.New("context canceled"),
			expected: "cancelled",
		},
		{
			name:     "connection error",
			err:      errors.New("connection refused"),
			expected: "connection_error",
		},
		{
			name:     "binary unavailable",
			err:      errors.New("pg_repack unavailable: not found in PATH"),
			expected: "binary_unavailable",
		},
		{
			name:     "execution failed",
			err:      errors.New("pg_repack execution failed"),
			expected: "execution_failed",
		},
		{
			name:     "database error",
			err:      errors.New("sql error: syntax error"),
			expected: "database_error",
		},
		{
			name:     "permission denied",
			err:      errors.New("permission denied"),
			expected: "permission_denied",
		},
		{
			name:     "unknown error",
			err:      errors.New("something went wrong"),
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifyError(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNewMetricsRecorder(t *testing.T) {
	logger := log.New()
	m := NewMetricsRecorder(logger)
	require.NotNil(t, m)

	// Should return a valid metrics recorder (either OTEL or NoOp)
	ctx := context.Background()
	m.RecordVacuumStart(ctx, "test")
}
