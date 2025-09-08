// Package cache provides helper functions for cache extension testing
package cache

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/config"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/node/types/sql"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	// Import for extension registration
	"github.com/trufnetwork/node/extensions/tn_cache"
)

// CacheTestHelper provides utilities for cache-enabled tests
type CacheTestHelper struct {
	cacheConfig *CacheOptions
	platform    *kwilTesting.Platform
	testHelper  *tn_cache.TestHelper
}

// SetupCacheTest sets up cache configuration and test DB injection
func SetupCacheTest(ctx context.Context, platform *kwilTesting.Platform, cacheConfig *CacheOptions) *CacheTestHelper {
	// Set up test database configuration (uses test container defaults)
	tn_cache.SetTestDBConfiguration(config.DBConfig{
		Host:   "localhost",
		Port:   "52853", // Default test container port
		User:   "kwild",
		Pass:   "kwild",
		DBName: "kwil_test_db",
	})

	// Inject the test's database connection
	tn_cache.SetTestDB(platform.DB)

	// Configure the extension
	configMap := make(map[string]string)
	for k, v := range cacheConfig.Build() {
		configMap[k] = fmt.Sprintf("%v", v)
	}
	tn_cache.SetTestConfiguration(configMap)

	helper := &CacheTestHelper{
		cacheConfig: cacheConfig,
		platform:    platform,
	}
	helper.testHelper = tn_cache.GetTestHelper()
	return helper
}

// Cleanup cleans up test injection
func (h *CacheTestHelper) Cleanup() {
	tn_cache.SetTestDB(nil)
}

// IsCacheEnabled returns whether the cache is enabled
func (h *CacheTestHelper) IsCacheEnabled() bool {
	return h.cacheConfig.enabled
}

// GetDB returns the test database for direct queries
func (h *CacheTestHelper) GetDB() sql.DB {
	return h.platform.DB
}

// SetDB sets the test database for direct queries
func (h *CacheTestHelper) SetDB(db sql.DB) {
	h.platform.DB = db
	tn_cache.SetTestDB(db)
}

// WithCache is a test wrapper that sets up and tears down cache configuration
func WithCache(t TestingT, ctx context.Context, platform *kwilTesting.Platform, cacheConfig *CacheOptions, testFunc func(*CacheTestHelper)) {
	helper := SetupCacheTest(ctx, platform, cacheConfig)
	defer helper.Cleanup()
	testFunc(helper)
}

// RunInTx runs the provided test function within a database transaction
func (h *CacheTestHelper) RunInTx(t TestingT, testFn func(t TestingT, txPlatform *kwilTesting.Platform)) {
	WithTx(h.platform, func(t TestingT, txPlatform *kwilTesting.Platform) {
		// Save original DB and set tx DB
		originalDB := h.GetDB()
		h.SetDB(txPlatform.DB)
		defer h.SetDB(originalDB) // Restore after

		testFn(t, txPlatform)
	})(t)
}

// WithTx runs testFunc inside a real DB transaction and rolls back at the end
func WithTx(platform *kwilTesting.Platform, testFunc func(TestingT, *kwilTesting.Platform)) func(TestingT) {
	return func(t TestingT) {
		tx, err := platform.DB.BeginTx(context.Background())
		if err != nil {
			t.Fatalf("begin tx: %v", err)
			return
		}
		defer tx.Rollback(context.Background())

		txPlatform := &kwilTesting.Platform{
			Engine:   platform.Engine,
			DB:       tx,
			Deployer: platform.Deployer,
			Logger:   platform.Logger,
		}

		testFunc(t, txPlatform)
	}
}

// TestingT interface for test functions
type TestingT interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// SetupCacheExtension creates a full cache extension setup for testing
func SetupCacheExtension(ctx context.Context, cacheConfig *CacheOptions, platform *kwilTesting.Platform) error {
	// Setup basic cache test
	helper := SetupCacheTest(ctx, platform, cacheConfig)
	defer helper.Cleanup()

	// Create mock service for extension setup
	mockService := &common.Service{
		Logger: log.NewStdoutLogger(),
		LocalConfig: &config.Config{
			DB: config.DBConfig{
				Host:   "localhost",
				Port:   "52853",
				User:   "kwild",
				Pass:   "kwild",
				DBName: "kwil_test_db",
			},
			Extensions: map[string]map[string]string{
				tn_cache.ExtensionName: cacheConfig.Build(),
			},
		},
	}

	// Parse and setup extension
	processedConfig, err := tn_cache.ParseConfig(mockService)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	ext, err := tn_cache.SetupCacheExtension(ctx, processedConfig, platform.Engine, mockService)
	if err != nil {
		return fmt.Errorf("failed to setup extension: %w", err)
	}

	// Register extension for cleanup
	tn_cache.SetExtension(ext)
	defer func() {
		tn_cache.SetExtension(nil)
		// Stop background tasks
		if ext.Scheduler() != nil {
			if stopErr := ext.Scheduler().Stop(); stopErr != nil {
				mockService.Logger.Error("failed to stop scheduler", "error", stopErr)
			}
		}
		if ext.SyncChecker() != nil {
			ext.SyncChecker().Stop()
		}
		ext.Close()
	}()

	return nil
}
