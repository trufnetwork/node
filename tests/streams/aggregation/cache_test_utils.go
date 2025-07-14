package tests

import (
	"context"
	"testing"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

// wrapTestWithCacheModes wraps a test function to run it twice - once with cache disabled and once enabled
func wrapTestWithCacheModes(t *testing.T, testName string, testFunc func(*testing.T, bool) func(context.Context, *kwilTesting.Platform) error) func(context.Context, *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Run without cache
		t.Run(testName+"_without_cache", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			err := testFunc(t, false)(ctx, platform)
			if err != nil {
				t.Fatalf("Test failed without cache: %v", err)
			}
		}))

		// Run with cache
		t.Run(testName+"_with_cache", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			err := testFunc(t, true)(ctx, txPlatform)
			if err != nil {
				t.Fatalf("Test failed with cache: %v", err)
			}
		}))

		return nil
	}
}