//go:build kwiltest

/*
DATABASE SIZE V2 TEST SUITE

This test file covers the database size v2 actions that use PostgreSQL native functions
for accurate database size calculation instead of manual estimation:

- get_database_size_v2() - returns raw database size in bytes
- get_database_size_v2_pretty() - returns human-readable database size
- get_table_sizes_v2() - returns breakdown of table sizes

These actions use PostgreSQL functions like pg_database_size() for 100% accuracy
compared to the ~77% accurate manual calculations in the original get_database_size.
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
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testGetDatabaseSizeV2(t),
			testGetDatabaseSizeV2Pretty(t),
			testGetTableSizesV2(t),
		},
	}, &testutils.Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
	})
}

// Test get_database_size_v2 action - returns raw size in bytes
func testGetDatabaseSizeV2(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Call get_database_size_v2
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

		t.Logf("Database size v2 (raw): %d bytes", size)
		return nil
	}
}

// Test get_database_size_v2_pretty action - returns human-readable size
func testGetDatabaseSizeV2Pretty(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Call get_database_size_v2_pretty
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

		t.Logf("Database size v2 (pretty): %s", prettySizeStr)
		return nil
	}
}

// Test get_table_sizes_v2 action - returns table breakdown
func testGetTableSizesV2(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Call get_table_sizes_v2
		result, err := procedure.GetTableSizesV2(ctx, procedure.GetDatabaseSizeInput{
			Platform: platform,
			Locator: trufTypes.StreamLocator{
				DataProvider: deployer,
			},
			Height: 0,
		})

		require.NoError(t, err, "get_table_sizes_v2 should execute without error")
		require.Greater(t, len(result), 0, "Should return at least one row")

		// Each row should have 3 columns: table_name, size_bytes, size_pretty
		for i, row := range result {
			require.Len(t, row, 3, "Row %d should have exactly 3 columns", i)

			tableName := row[0]
			sizeBytes := row[1]
			sizePretty := row[2]

			assert.NotEmpty(t, tableName, "Table name should not be empty")
			assert.NotEmpty(t, sizeBytes, "Size bytes should not be empty")
			assert.NotEmpty(t, sizePretty, "Size pretty should not be empty")

			// Verify size_bytes is a valid integer
			size, err := strconv.ParseInt(sizeBytes, 10, 64)
			require.NoError(t, err, "Size bytes should be a valid integer for table %s", tableName)
			assert.GreaterOrEqual(t, size, int64(0), "Size should be non-negative for table %s", tableName)

			t.Logf("Table: %s, Size: %d bytes (%s)", tableName, size, sizePretty)
		}

		return nil
	}
}
