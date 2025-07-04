package tn_cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sony/gobreaker"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
)

// Circuit breaker configuration constants
const (
	DefaultCircuitBreakerMaxRequests  = 3
	DefaultCircuitBreakerInterval     = 10 * time.Second
	DefaultCircuitBreakerTimeout      = 60 * time.Second
	DefaultCircuitBreakerFailureRatio = 0.6
)

// getCircuitBreaker returns or creates a circuit breaker for a stream
func (s *CacheScheduler) getCircuitBreaker(streamKey string) *gobreaker.CircuitBreaker {
	// First, check if circuit breaker already exists
	s.breakerMu.RLock()
	cb, exists := s.breakers[streamKey]
	s.breakerMu.RUnlock()

	if exists {
		return cb
	}

	// Create new circuit breaker if it doesn't exist
	s.breakerMu.Lock()
	defer s.breakerMu.Unlock()

	// Double-check after acquiring write lock
	if cb, exists := s.breakers[streamKey]; exists {
		return cb
	}

	cb = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        streamKey,
		MaxRequests: DefaultCircuitBreakerMaxRequests,
		Interval:    DefaultCircuitBreakerInterval,
		Timeout:     DefaultCircuitBreakerTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= DefaultCircuitBreakerMaxRequests && failureRatio >= DefaultCircuitBreakerFailureRatio
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			s.logger.Info("circuit breaker state changed",
				"stream", name,
				"from", from.String(),
				"to", to.String())
			
			// Extract provider and stream from the name (format: "provider/stream")
			// Note: name is the streamKey which is "provider/stream"
			parts := strings.Split(name, "/")
			if len(parts) == 2 {
				provider := parts[0]
				stream := parts[1]
				
				// Record metric
				s.metrics.RecordCircuitBreakerStateChange(context.Background(), provider, stream, from, to)
			}
		},
	})

	s.breakers[streamKey] = cb
	return cb
}

// refreshStreamDataWithCircuitBreaker wraps stream data refresh with circuit breaker
func (s *CacheScheduler) refreshStreamDataWithCircuitBreaker(ctx context.Context, directive config.CacheDirective) error {
	streamKey := fmt.Sprintf("%s/%s", directive.DataProvider, directive.StreamID)
	// Get or create circuit breaker for this stream
	cb := s.getCircuitBreaker(streamKey)

	// Execute the refresh operation with circuit breaker protection
	_, err := cb.Execute(func() (interface{}, error) {
		return nil, s.refreshStreamDataWithRetry(ctx, directive, 3)
	})

	return err
}