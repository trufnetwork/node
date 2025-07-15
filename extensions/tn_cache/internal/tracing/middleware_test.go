package tracing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
)

func TestTracedOperation(t *testing.T) {
	t.Run("successful operation", func(t *testing.T) {
		ctx := context.Background()
		expectedResult := "test result"

		result, err := tracing.TracedOperation(ctx, tracing.OpCacheGet, "provider1", "stream1",
			func(traceCtx context.Context) (string, error) {
				// Verify we got a context
				assert.NotNil(t, traceCtx)
				return expectedResult, nil
			})

		assert.NoError(t, err)
		assert.Equal(t, expectedResult, result)
	})

	t.Run("operation with error", func(t *testing.T) {
		ctx := context.Background()
		expectedError := errors.New("test error")

		result, err := tracing.TracedOperation(ctx, tracing.OpCacheGet, "provider1", "stream1",
			func(traceCtx context.Context) (string, error) {
				return "", expectedError
			})

		assert.Equal(t, expectedError, err)
		assert.Equal(t, "", result)
	})

	t.Run("operation with attributes", func(t *testing.T) {
		ctx := context.Background()
		attrs := []attribute.KeyValue{
			attribute.Int64("from", 1000),
			attribute.Int64("to", 2000),
		}

		result, err := tracing.TracedOperation(ctx, tracing.OpCacheGet, "provider1", "stream1",
			func(traceCtx context.Context) (int, error) {
				return 42, nil
			}, attrs...)

		assert.NoError(t, err)
		assert.Equal(t, 42, result)
	})

	t.Run("panic recovery", func(t *testing.T) {
		ctx := context.Background()

		assert.Panics(t, func() {
			_, _ = tracing.TracedOperation(ctx, tracing.OpCacheGet, "provider1", "stream1",
				func(traceCtx context.Context) (any, error) {
					panic("test panic")
				})
		})
	})
}

func TestTracedDatabaseOperation(t *testing.T) {
	t.Run("successful database operation", func(t *testing.T) {
		ctx := context.Background()
		expectedRows := 10

		result, err := tracing.TracedDatabaseOperation(ctx, tracing.OpDBCacheEvents,
			func(traceCtx context.Context) (int, error) {
				return expectedRows, nil
			}, attribute.Int("count", expectedRows))

		assert.NoError(t, err)
		assert.Equal(t, expectedRows, result)
	})
}

func TestWithCacheMetrics(t *testing.T) {
	t.Run("data served with count", func(t *testing.T) {
		config := tracing.MetricsConfig{
			Provider: "provider1",
			StreamID: "stream1",
			Context:  context.Background(),
		}

		count, err := tracing.WithCacheMetrics(config, func() (int, error) {
			return 5, nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 5, count)
	})

	t.Run("data served with no data", func(t *testing.T) {
		config := tracing.MetricsConfig{
			Provider: "provider1",
			StreamID: "stream1",
			Context:  context.Background(),
		}

		count, err := tracing.WithCacheMetrics(config, func() (int, error) {
			return 0, nil // Empty result
		})

		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("error handling", func(t *testing.T) {
		config := tracing.MetricsConfig{
			Provider: "provider1",
			StreamID: "stream1",
			Context:  context.Background(),
		}

		expectedError := errors.New("test error")
		count, err := tracing.WithCacheMetrics(config, func() (int, error) {
			return 0, expectedError
		})

		assert.Equal(t, expectedError, err)
		assert.Equal(t, 0, count)
	})
}

func TestWithRefreshMetrics(t *testing.T) {
	t.Run("successful refresh", func(t *testing.T) {
		config := tracing.MetricsConfig{
			Provider: "provider1",
			StreamID: "stream1",
			Context:  context.Background(),
		}

		count, err := tracing.WithRefreshMetrics(config, func() (int, error) {
			// Simulate some work
			time.Sleep(10 * time.Millisecond)
			return 100, nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 100, count)
	})

	t.Run("refresh with error", func(t *testing.T) {
		config := tracing.MetricsConfig{
			Provider: "provider1",
			StreamID: "stream1",
			Context:  context.Background(),
		}

		expectedError := errors.New("refresh failed")
		count, err := tracing.WithRefreshMetrics(config, func() (int, error) {
			return 0, expectedError
		})

		assert.Equal(t, expectedError, err)
		assert.Equal(t, 0, count)
	})
}

func TestTracedWithCacheMetrics(t *testing.T) {
	t.Run("combined tracing and data served metrics", func(t *testing.T) {
		ctx := context.Background()
		expectedResult := "data"
		expectedCount := 10

		result, err := tracing.TracedWithCacheMetrics(ctx, tracing.OpCacheGet, "provider1", "stream1", nil,
			func(traceCtx context.Context) (string, int, error) {
				assert.NotNil(t, traceCtx)
				return expectedResult, expectedCount, nil
			})

		assert.NoError(t, err)
		assert.Equal(t, expectedResult, result)
	})

	t.Run("combined tracing and data served - empty result", func(t *testing.T) {
		ctx := context.Background()
		expectedResult := "data"

		result, err := tracing.TracedWithCacheMetrics(ctx, tracing.OpCacheGet, "provider1", "stream1", nil,
			func(traceCtx context.Context) (string, int, error) {
				return expectedResult, 0, nil // 0 count
			})

		assert.NoError(t, err)
		assert.Equal(t, expectedResult, result)
	})

	t.Run("with additional attributes", func(t *testing.T) {
		ctx := context.Background()
		attrs := []attribute.KeyValue{
			attribute.Int64("from", 1000),
			attribute.Int64("to", 2000),
		}

		result, err := tracing.TracedWithCacheMetrics(ctx, tracing.OpCacheGet, "provider1", "stream1", nil,
			func(traceCtx context.Context) ([]int, int, error) {
				return []int{1, 2, 3}, 3, nil
			}, attrs...)

		assert.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, result)
	})
}

func TestTracedWithRefreshMetrics(t *testing.T) {
	t.Run("refresh with tracing and metrics", func(t *testing.T) {
		ctx := context.Background()
		expectedEvents := []string{"event1", "event2"}
		expectedCount := 2

		result, err := tracing.TracedWithRefreshMetrics(ctx, tracing.OpRefreshStream, "provider1", "stream1", nil,
			func(traceCtx context.Context) ([]string, int, error) {
				// Simulate refresh work
				time.Sleep(5 * time.Millisecond)
				return expectedEvents, expectedCount, nil
			})

		assert.NoError(t, err)
		assert.Equal(t, expectedEvents, result)
	})
}

func TestConditionalTrace(t *testing.T) {
	t.Run("tracing enabled", func(t *testing.T) {
		ctx := context.Background()
		
		traceCtx, endFunc := tracing.ConditionalTrace(ctx, tracing.OpCacheGet, "provider1", "stream1", true)
		
		assert.NotNil(t, traceCtx)
		assert.NotNil(t, endFunc)
		
		// Should not panic
		endFunc(nil)
	})
	
	t.Run("tracing disabled", func(t *testing.T) {
		ctx := context.Background()
		
		traceCtx, endFunc := tracing.ConditionalTrace(ctx, tracing.OpCacheGet, "provider1", "stream1", false)
		
		assert.Equal(t, ctx, traceCtx) // Should return same context
		assert.NotNil(t, endFunc)
		
		// Should not panic
		endFunc(nil)
	})
}

func TestIsTracingEnabled(t *testing.T) {
	// Test with tracing enabled
	assert.True(t, tracing.IsTracingEnabled(true))
	
	// Test with tracing disabled
	assert.False(t, tracing.IsTracingEnabled(false))
}

// Benchmark tests to ensure middleware doesn't add significant overhead
func BenchmarkTracedOperation(b *testing.B) {
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tracing.TracedOperation(ctx, tracing.OpCacheGet, "provider1", "stream1",
			func(traceCtx context.Context) (int, error) {
				return i, nil
			})
	}
}

func BenchmarkDirectOperation(b *testing.B) {
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Direct operation without middleware for comparison
		_, _ = func(traceCtx context.Context) (int, error) {
			return i, nil
		}(ctx)
	}
}