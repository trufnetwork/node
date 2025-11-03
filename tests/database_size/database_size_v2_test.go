//go:build kwiltest

/*
DATABASE SIZE V2 TEST SUITE

This test file covers the database size v2 actions that use the database_size precompile extension
for accurate database size calculation while avoiding consensus risks:

- get_database_size_v2() - returns raw database size in bytes using extension
- get_database_size_v2_pretty() - returns human-readable database size using extension

These actions use the database_size extension which provides PostgreSQL functions like
pg_database_size() for 100% accuracy compared to the ~77% accurate manual calculations
in the original get_database_size, while being safe for consensus through the extension system.
*/

package tests

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	trufTypes "github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

func TestDatabaseSizeV2Actions(t *testing.T) {
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "database_size_v2_test",
		SeedStatements: migrations.GetSeedScriptStatements(),
		FunctionTests: []kwilTesting.TestFunc{
			testGetDatabaseSizeV2(t),
			testGetDatabaseSizeV2Pretty(t),
		},
	}, &testutils.Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
	})
}

// Test get_database_size_v2 action - returns raw size in bytes using extension
func testGetDatabaseSizeV2(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Call get_database_size_v2 - now uses database_size extension
		result, err := procedure.GetDatabaseSizeV2(ctx, procedure.GetDatabaseSizeInput{
			Platform: platform,
			Locator: trufTypes.StreamLocator{
				DataProvider: deployer,
			},
			Height: 0,
		})

		require.NoError(t, err, "get_database_size_v2 should execute without error")
		require.Len(t, result, 1, "Should return exactly one row")
		require.Len(t, result[0], 1, "Row should have exactly one column (database_size)")

		// Parse the size and verify it's a valid positive integer
		sizeStr := result[0][0]
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		require.NoError(t, err, "Database size should be a valid integer")
		assert.Greater(t, size, int64(0), "Database size should be positive")

		// Basic sanity check - size should be reasonable (between 1KB and 100MB for test)
		assert.Greater(t, size, int64(1024), "Database size should be at least 1KB")
		assert.Less(t, size, int64(100*1024*1024), "Database size should be less than 100MB for test")

		t.Logf("Database size v2 (extension): %d bytes", size)
		return nil
	}
}

// Test get_database_size_v2_pretty action - returns human-readable size using extension
func testGetDatabaseSizeV2Pretty(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Call get_database_size_v2_pretty - now uses database_size extension
		result, err := procedure.GetDatabaseSizeV2Pretty(ctx, procedure.GetDatabaseSizeInput{
			Platform: platform,
			Locator: trufTypes.StreamLocator{
				DataProvider: deployer,
			},
			Height: 0,
		})

		require.NoError(t, err, "get_database_size_v2_pretty should execute without error")
		require.Len(t, result, 1, "Should return exactly one row")
		require.Len(t, result[0], 1, "Row should have exactly one column (database_size_pretty)")

		// Verify it's a human-readable format (should contain common size units)
		prettySizeStr := result[0][0]
		assert.NotEmpty(t, prettySizeStr, "Pretty size should not be empty")

		// Should contain typical PostgreSQL size units (bytes, kB, MB, GB)
		containsUnit := false
		units := []string{"bytes", "kB", "MB", "GB", "TB"}
		for _, unit := range units {
			if len(prettySizeStr) > len(unit) && prettySizeStr[len(prettySizeStr)-len(unit):] == unit {
				containsUnit = true
				break
			}
		}
		assert.True(t, containsUnit, "Pretty size should contain a valid unit suffix: %s", prettySizeStr)

		t.Logf("Database size v2 pretty (extension): %s", prettySizeStr)
		return nil
	}
}