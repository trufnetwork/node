//go:build kwiltest

package database_size

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

func TestDatabaseSizeExtension(t *testing.T) {
	ctx := context.Background()

	t.Run("TestPrecompileInitialization", func(t *testing.T) {
		// Create a minimal service for testing
		logger := log.New()
		service := &common.Service{
			Logger: logger,
		}

		// Test that the precompile initializer works
		precompile, err := InitializeDatabaseSizePrecompile(ctx, service, nil, "test_alias", nil)
		require.NoError(t, err)
		assert.NotNil(t, precompile.OnStart, "OnStart hook should be defined")

		// Test OnStart hook with minimal app
		if precompile.OnStart != nil {
			app := &common.App{
				Service: service,
			}
			err = precompile.OnStart(ctx, app)
			require.NoError(t, err)
		}
	})

	t.Run("TestExtensionConstants", func(t *testing.T) {
		assert.Equal(t, "database_size", ExtensionName, "Extension name should be correct")
	})
}

// Integration test that uses the full testing framework
func TestDatabaseSizeWithDatabase(t *testing.T) {
	// This test runs with the full test framework like our ACTION tests
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "database_size_extension_test",
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Test GetDatabaseSize helper function
				size, err := GetDatabaseSize(ctx, platform.DB)
				require.NoError(t, err)
				assert.Greater(t, size, int64(0), "Database size should be positive")
				t.Logf("Database size: %d bytes", size)

				// Test GetDatabaseSizePretty helper function
				prettySize, err := GetDatabaseSizePretty(ctx, platform.DB)
				require.NoError(t, err)
				assert.NotEmpty(t, prettySize, "Pretty size should not be empty")
				t.Logf("Database size (pretty): %s", prettySize)

				// Compare with direct query
				result, err := platform.DB.Execute(ctx, "SELECT pg_database_size('kwild')")
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				directSize := result.Rows[0][0].(int64)

				assert.Equal(t, directSize, size, "Extension and direct query should return same size")

				return nil
			},
		},
	}, &kwilTesting.Options{
		UseTestContainer: true,
	})
}