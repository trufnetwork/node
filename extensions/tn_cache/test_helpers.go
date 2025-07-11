package tn_cache

import (
	"context"
	"fmt"
	"time"

	"github.com/trufnetwork/node/extensions/tn_cache/config"
)

// TestHelper provides test-only functions for the tn_cache extension
type TestHelper struct{}

// RefreshStreamCacheSync synchronously refreshes the cache for a specific stream
// RequireExtension fails the test if extension is not available
func RequireExtension(t interface{ Fatalf(format string, args ...interface{}) }) *Extension {
	ext := GetExtension()
	if ext == nil {
		t.Fatalf("tn_cache extension not initialized - test requires extension")
	}
	if !ext.IsEnabled() {
		t.Fatalf("tn_cache extension not enabled - test requires enabled extension")
	}
	return ext
}

// This is only available in test builds and allows tests to trigger cache
// refresh without waiting for the scheduler
func (TestHelper) RefreshStreamCacheSync(ctx context.Context, dataProvider, streamID string) (int, error) {
	ext, err := safeGetExtension()
	if err != nil {
		return 0, fmt.Errorf("tn_cache extension not available: %w", err)
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
	if err := ext.Scheduler().RefreshStreamData(ctx, directive); err != nil {
		return 0, fmt.Errorf("failed to refresh stream: %w", err)
	}

	// Get the count of cached records using the CacheDB interface
	cacheDB := ext.CacheDB()
	if cacheDB == nil {
		return 0, fmt.Errorf("failed to get cache database connection")
	}

	// Use the QueryCachedStreamsWithCounts method to get the count
	streamInfos, err := cacheDB.QueryCachedStreamsWithCounts(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to query cached streams: %w", err)
	}

	// Find the specific stream
	for _, info := range streamInfos {
		if info.DataProvider == dataProvider && info.StreamID == streamID {
			return int(info.EventCount), nil
		}
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
			if ext, err := safeGetExtension(); err == nil && ext.CacheDB() != nil && ext.Scheduler() != nil {
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
	ext, err := safeGetExtension()
	return err == nil && ext.CacheDB() != nil && ext.Scheduler() != nil
}

// TriggerResolution triggers the cache extension's stream resolution process
// to re-discover streams matching the configured cache directives (wildcards, etc).
// This is essential for tests that create new streams dynamically, as these streams
// won't be picked up by the periodic resolution schedule.
func (TestHelper) TriggerResolution(ctx context.Context) error {
	ext, err := safeGetExtension()
	if err != nil {
		return fmt.Errorf("tn_cache extension not available: %w", err)
	}

	scheduler := ext.Scheduler()
	if scheduler == nil {
		return fmt.Errorf("scheduler not initialized")
	}

	// Trigger the global resolution process
	if err := scheduler.TriggerResolution(ctx); err != nil {
		return fmt.Errorf("failed to trigger stream resolution: %w", err)
	}

	return nil
}

// GetTestHelper returns a test helper instance
// This is the public API for tests to access test-only functionality
func GetTestHelper() *TestHelper {
	return &TestHelper{}
}
