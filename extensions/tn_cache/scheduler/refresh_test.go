package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	errors2 "github.com/trufnetwork/node/extensions/tn_cache/internal/errors"
)

// TestParseEventTime removed - tests basic type conversions (Go stdlib) rather than custom logic

// TestParseEventValue removed - tests basic type conversions rather than custom logic

func TestIsNonRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"schema error", errors.New("schema does not exist"), true},
		{"permission error", errors.New("permission denied"), true},
		{"context cancelled", errors.New("context canceled"), true},
		{"network error", errors.New("connection refused"), false},
		{"timeout error", errors.New("i/o timeout"), false},
		{"mixed case permission", errors.New("Permission Denied"), true},
		{"unrelated error", errors.New("some random error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := errors2.IsNonRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Mock scheduler for testing retry logic
type mockSchedulerForRetry struct {
	attempts    int
	maxAttempts int
	shouldFail  bool
}

func (m *mockSchedulerForRetry) refreshStreamData(ctx context.Context, directive config.CacheDirective) error {
	m.attempts++

	if m.shouldFail && m.attempts <= m.maxAttempts {
		return errors.New("simulated temporary failure")
	}

	return nil
}

func TestRetryLogic(t *testing.T) {
	// This test demonstrates the retry logic concept
	// In practice, we'd need more sophisticated mocking for the actual scheduler

	t.Run("eventually succeeds", func(t *testing.T) {
		mock := &mockSchedulerForRetry{
			maxAttempts: 2, // Fail first 2 attempts, succeed on 3rd
			shouldFail:  true,
		}

		// Simulate retry logic
		maxRetries := 3
		var lastErr error

		for attempt := 0; attempt <= maxRetries; attempt++ {
			directive := config.CacheDirective{
				DataProvider: "test",
				StreamID:     "test",
			}

			if err := mock.refreshStreamData(context.Background(), directive); err != nil {
				lastErr = err
				if attempt < maxRetries {
					// Would normally wait with backoff here
					continue
				}
			} else {
				lastErr = nil
				break
			}
		}

		require.NoError(t, lastErr, "Should eventually succeed")
		assert.Equal(t, 3, mock.attempts, "Should attempt 3 times")
	})

	t.Run("fails after max retries", func(t *testing.T) {
		mock := &mockSchedulerForRetry{
			maxAttempts: 10, // Always fail
			shouldFail:  true,
		}

		// Simulate retry logic
		maxRetries := 2
		var lastErr error

		for attempt := 0; attempt <= maxRetries; attempt++ {
			directive := config.CacheDirective{
				DataProvider: "test",
				StreamID:     "test",
			}

			if err := mock.refreshStreamData(context.Background(), directive); err != nil {
				lastErr = err
				if attempt < maxRetries {
					continue
				}
			} else {
				lastErr = nil
				break
			}
		}

		require.Error(t, lastErr, "Should fail after max retries")
		assert.Equal(t, 3, mock.attempts, "Should attempt 3 times total")
	})
}

// TestNamespaceConfiguration removed - tests trivial if-statement logic
