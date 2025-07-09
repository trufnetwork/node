package tn_cache

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
	"github.com/trufnetwork/node/extensions/tn_cache/internal/constants"
)

// TestHelper provides test-only functions for the tn_cache extension
type TestHelper struct{}

// RefreshCacheSync synchronously refreshes the cache for a specific stream
// This is only available in test builds and allows tests to trigger cache
// refresh without waiting for the scheduler
func (TestHelper) RefreshCacheSync(ctx context.Context, dataProvider, streamID string) (int, error) {
	if !isEnabled {
		return 0, fmt.Errorf("tn_cache extension is not enabled")
	}

	if scheduler == nil {
		return 0, fmt.Errorf("scheduler not initialized")
	}

	// Create a directive for this specific stream
	directive := config.CacheDirective{
		DataProvider: dataProvider,
		StreamID:     streamID,
		Type:         config.DirectiveSpecific,
		TimeRange: config.TimeRange{
			From: nil, // Use configured from_timestamp
		},
	}

	// Use the scheduler's refresh method with retry
	err := scheduler.refreshStreamDataWithRetry(ctx, directive, 3)
	if err != nil {
		return 0, fmt.Errorf("failed to refresh stream: %w", err)
	}

	// Get the count of cached records
	db := getWrappedCacheDB()
	if db == nil {
		return 0, fmt.Errorf("failed to get cache database connection")
	}

	result, err := db.Execute(ctx, `
		SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events 
		WHERE data_provider = $1 AND stream_id = $2`,
		dataProvider, streamID)
	if err != nil {
		return 0, fmt.Errorf("failed to count cached records: %w", err)
	}

	if len(result.Rows) > 0 {
		return int(result.Rows[0][0].(int64)), nil
	}

	return 0, nil
}

// WaitForInitialization waits for the extension to be fully initialized
// This is useful in tests to ensure the extension is ready before proceeding
func (TestHelper) WaitForInitialization(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if isEnabled && cacheDB != nil && scheduler != nil && cachePool != nil {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for extension initialization")
			}
		}
	}
}

// IsInitialized checks if the extension is fully initialized
func (TestHelper) IsInitialized() bool {
	return isEnabled && cacheDB != nil && scheduler != nil && cachePool != nil
}

// GetTestHelper returns a test helper instance
// This is the public API for tests to access test-only functionality
func GetTestHelper() *TestHelper {
	return &TestHelper{}
}