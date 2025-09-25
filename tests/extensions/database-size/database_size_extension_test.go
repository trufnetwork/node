//go:build kwiltest

package database_size_test

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
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestDatabaseSizeExtension tests the database_size extension functionality including:
// - Extension initialization and method registration
// - Database size calculation using PostgreSQL native functions
// - Integration with ACTION that uses the extension methods
func TestDatabaseSizeExtension(t *testing.T) {
	// Run the test with standard test configuration
	// The database_size extension should be automatically available
	testutils.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "database_size_extension_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testDatabaseSizeExtensionMethods(t),
			testGetDbSizeThreeAction(t),
		},
	}, &testutils.Options{
		Options: &kwilTesting.Options{
			UseTestContainer: true,
		},
	})
}

// Test the database_size extension schema and SQL functions
func testDatabaseSizeExtensionMethods(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Test that the extension created the schema and functions
		// The extension should create ext_database_size schema with SQL functions

		// Test ext_database_size.get_database_size() function
		result, err := platform.DB.Execute(ctx, "SELECT ext_database_size.get_database_size()")
		require.NoError(t, err, "ext_database_size.get_database_size() should work")
		require.Len(t, result.Rows, 1, "Should return one row")
		require.Len(t, result.Rows[0], 1, "Row should have one column")

		// Verify it returns a valid positive integer
		size := result.Rows[0][0].(int64)
		assert.Greater(t, size, int64(0), "Database size should be positive")
		assert.Greater(t, size, int64(1024), "Database size should be at least 1KB")
		assert.Less(t, size, int64(100*1024*1024), "Database size should be less than 100MB for test")

		t.Logf("Database size (extension SQL function): %d bytes", size)

		// Test ext_database_size.get_database_size_pretty() function
		result, err = platform.DB.Execute(ctx, "SELECT ext_database_size.get_database_size_pretty()")
		require.NoError(t, err, "ext_database_size.get_database_size_pretty() should work")
		require.Len(t, result.Rows, 1, "Should return one row")
		require.Len(t, result.Rows[0], 1, "Row should have one column")

		// Verify it returns a valid human-readable size
		prettySize := result.Rows[0][0].(string)
		assert.NotEmpty(t, prettySize, "Pretty size should not be empty")

		// Should contain typical PostgreSQL size units
		containsUnit := false
		units := []string{"bytes", "kB", "MB", "GB", "TB"}
		for _, unit := range units {
			if len(prettySize) > len(unit) && prettySize[len(prettySize)-len(unit):] == unit {
				containsUnit = true
				break
			}
		}
		assert.True(t, containsUnit, "Pretty size should contain a valid unit suffix: %s", prettySize)

		t.Logf("Database size pretty (extension SQL function): %s", prettySize)

		return nil
	}
}

// Test the get_db_size_three ACTION that uses the database_size extension
func testGetDbSizeThreeAction(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer := util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000000")
		platform = procedure.WithSigner(platform, deployer.Bytes())

		// Call get_db_size_three ACTION
		result, err := procedure.GetDbSizeThree(ctx, procedure.GetDatabaseSizeInput{
			Platform: platform,
			Locator: types.StreamLocator{
				DataProvider: deployer,
			},
			Height: 0,
		})

		require.NoError(t, err, "get_db_size_three should execute without error")
		require.Len(t, result, 1, "Should return exactly one row")
		require.Len(t, result[0], 2, "Row should have exactly two columns (database_size, database_size_pretty)")

		// Parse the size and verify it's a valid positive integer
		sizeStr := result[0][0]
		prettyStr := result[0][1]

		size, err := strconv.ParseInt(sizeStr, 10, 64)
		require.NoError(t, err, "Database size should be a valid integer")
		assert.Greater(t, size, int64(0), "Database size should be positive")

		// Verify pretty string is not empty
		assert.NotEmpty(t, prettyStr, "Pretty size should not be empty")

		// Should contain typical PostgreSQL size units
		containsUnit := false
		units := []string{"bytes", "kB", "MB", "GB", "TB"}
		for _, unit := range units {
			if len(prettyStr) > len(unit) && prettyStr[len(prettyStr)-len(unit):] == unit {
				containsUnit = true
				break
			}
		}
		assert.True(t, containsUnit, "Pretty size should contain a valid unit suffix: %s", prettyStr)

		// Basic sanity check - size should be reasonable
		assert.Greater(t, size, int64(1024), "Database size should be at least 1KB")
		assert.Less(t, size, int64(100*1024*1024), "Database size should be less than 100MB for test")

		t.Logf("Database size three (ACTION using extension): %d bytes (%s)", size, prettyStr)

		return nil
	}
}