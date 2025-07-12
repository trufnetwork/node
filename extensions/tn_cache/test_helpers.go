package tn_cache

import (
	"context"
	"fmt"
	"time"
)

// TestHelper provides test-only functions for the tn_cache extension
type TestHelper struct{}

// RefreshStreamCacheSync synchronously refreshes the cache for a specific stream
// RequireExtension fails the test if extension is not available
func RequireExtension(t interface {
	Fatalf(format string, args ...interface{})
}) *Extension {
	ext := GetExtension()
	if ext == nil {
		t.Fatalf("tn_cache extension not initialized - test requires extension")
	}
	if !ext.IsEnabled() {
		t.Fatalf("tn_cache extension not enabled - test requires enabled extension")
	}
	return ext
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
	ext.logger.Info("Triggering resolution start")
	if err := scheduler.TriggerResolution(ctx); err != nil {
		return fmt.Errorf("failed to trigger stream resolution: %w", err)
	}
	ext.logger.Info("Triggering resolution complete")

	return nil
}

// GetTestHelper returns a test helper instance
// This is the public API for tests to access test-only functionality
func GetTestHelper() *TestHelper {
	return &TestHelper{}
}

// RefreshAllStreamsSync synchronously triggers resolution and refreshes all configured streams
func (TestHelper) RefreshAllStreamsSync(ctx context.Context) (int, error) {
	ext, err := safeGetExtension()
	if err != nil {
		return 0, fmt.Errorf("tn_cache extension not available: %w", err)
	}

	scheduler := ext.Scheduler()
	if scheduler == nil {
		return 0, fmt.Errorf("scheduler not initialized")
	}

	// First, trigger resolution to discover any new streams
	if err := scheduler.TriggerResolution(ctx); err != nil {
		return 0, fmt.Errorf("failed to trigger resolution: %w", err)
	}

	// Now get all current cached streams
	cacheDB := ext.CacheDB()
	if cacheDB == nil {
		return 0, fmt.Errorf("failed to get cache database connection")
	}

	directives := scheduler.GetCurrentDirectives()

	totalRecords := 0
	for _, directive := range directives {
		if err := scheduler.RefreshStreamData(ctx, directive); err != nil {
			return 0, fmt.Errorf("failed to refresh stream %s:%s: %w", directive.DataProvider, directive.StreamID, err)
		}

		// Update count - re-query single stream count
		updatedInfos, err := cacheDB.QueryCachedStreamsWithCounts(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to query updated counts: %w", err)
		}
		for _, updated := range updatedInfos {
			if updated.DataProvider == directive.DataProvider && updated.StreamID == directive.StreamID {
				totalRecords += int(updated.EventCount)
				break
			}
		}
	}

	return totalRecords, nil
}
