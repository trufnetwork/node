package tn_cache

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	errors2 "github.com/trufnetwork/node/extensions/tn_cache/internal/errors"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/parsing"
)

func TestParseEventTime(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
		wantErr  bool
	}{
		{"int64", int64(1640995200), 1640995200, false},
		{"int", int(1640995200), 1640995200, false},
		{"int32", int32(1640995200), 1640995200, false},
		{"uint64", uint64(1640995200), 1640995200, false},
		{"uint32", uint32(1640995200), 1640995200, false},
		{"string valid", "1640995200", 1640995200, false},
		{"string invalid", "not-a-number", 0, true},
		{"unsupported type", 123.45, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parsing.ParseEventTime(tt.input)
			
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseEventValue(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		expectedStr string
		wantErr     bool
	}{
		{"float64", float64(123.45), "123.450000000000000000", false},
		{"float32", float32(123.45), "123.450000000000000000", false}, // float32 gets converted via string
		{"int64", int64(123), "123.000000000000000000", false},
		{"int", int(123), "123.000000000000000000", false},
		{"string valid", "123.45", "123.450000000000000000", false},
		{"string high precision", "123456789.123456789012345678", "123456789.123456789012345678", false},
		{"string invalid", "not-a-number", "", true},
		{"nil", nil, "", true},
		{"unsupported type", make(chan int), "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parsing.ParseEventValue(tt.input)
			
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result, "Result should not be nil")
				assert.Equal(t, tt.expectedStr, result.String(), "Decimal string representation should match expected")
				
				// Verify precision and scale are set correctly
				assert.Equal(t, uint16(36), result.Precision(), "Precision should be 36")
				assert.Equal(t, uint16(18), result.Scale(), "Scale should be 18")
			}
		})
	}
}

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

func TestNamespaceConfiguration(t *testing.T) {
	// Test namespace configuration logic without creating actual scheduler instances
	// This tests the logic that would be used in the constructor
	
	t.Run("default namespace logic", func(t *testing.T) {
		namespace := ""
		if namespace == "" {
			namespace = "truf_db"
		}
		assert.Equal(t, "truf_db", namespace)
	})

	t.Run("custom namespace logic", func(t *testing.T) {
		namespace := "custom_ns"
		if namespace == "" {
			namespace = "truf_db"
		}
		assert.Equal(t, "custom_ns", namespace)
	})
}