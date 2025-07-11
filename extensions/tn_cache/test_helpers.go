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

// RefreshStreamCacheSync synchronously refreshes the cache for a specific stream
// This is only available in test builds and allows tests to trigger cache
// refresh without waiting for the scheduler
func (TestHelper) RefreshStreamCacheSync(ctx context.Context, dataProvider, streamID string) (int, error) {
	ext := GetExtension()
	if ext == nil || !ext.IsEnabled() {
		return 0, fmt.Errorf("tn_cache extension is not enabled")
	}

	if ext.Scheduler() == nil {
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
	err := ext.Scheduler().RefreshStreamData(ctx, directive)
	if err != nil {
		return 0, fmt.Errorf("failed to refresh stream: %w", err)
	}

	// Get the count of cached records
	db := GetExtension().CacheDB()
	if db == nil {
		return 0, fmt.Errorf("failed to get cache database connection")
	}

	pool := ext.CachePool()

	rows, err := pool.Query(ctx, `
		SELECT COUNT(*) FROM `+constants.CacheSchemaName+`.cached_events 
		WHERE data_provider = $1 AND stream_id = $2`,
		dataProvider, streamID)
	if err != nil {
		return 0, fmt.Errorf("failed to count cached records: %w", err)
	}

	var count int64
	if err := rows.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to scan count: %w", err)
	}

	return int(count), nil
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
			ext := GetExtension()
			if ext != nil && ext.IsEnabled() && ext.CacheDB() != nil && ext.Scheduler() != nil && ext.CachePool() != nil {
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
	ext := GetExtension()
	return ext != nil && ext.IsEnabled() && ext.CacheDB() != nil && ext.Scheduler() != nil && ext.CachePool() != nil
}

// TriggerResolution triggers the cache extension's stream resolution process
// to re-discover streams matching the configured cache directives (wildcards, etc).
// This is essential for tests that create new streams dynamically, as these streams
// won't be picked up by the periodic resolution schedule.
func (TestHelper) TriggerResolution(ctx context.Context) error {
	ext := GetExtension()
	if ext == nil || !ext.IsEnabled() {
		return fmt.Errorf("tn_cache extension is not enabled")
	}

	scheduler := ext.Scheduler()
	if scheduler == nil {
		return fmt.Errorf("scheduler not initialized")
	}

	// Trigger the global resolution process
	err := scheduler.TriggerResolution(ctx)
	if err != nil {
		return fmt.Errorf("failed to trigger stream resolution: %w", err)
	}

	return nil
}

// GetTestHelper returns a test helper instance
// This is the public API for tests to access test-only functionality
func GetTestHelper() *TestHelper {
	return &TestHelper{}
}
