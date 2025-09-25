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
		assert.Len(t, precompile.Methods, 2, "Should have 2 precompile methods")

		// Verify method names
		methodNames := make([]string, len(precompile.Methods))
		for i, method := range precompile.Methods {
			methodNames[i] = method.Name
		}
		assert.Contains(t, methodNames, "get_database_size", "Should have get_database_size method")
		assert.Contains(t, methodNames, "get_database_size_pretty", "Should have get_database_size_pretty method")
	})

	t.Run("TestExtensionConstants", func(t *testing.T) {
		assert.Equal(t, "database_size", ExtensionName, "Extension name should be correct")
		assert.Equal(t, "ext_database_size", SchemaName, "Schema name should be correct")
	})
}

// Integration test that uses the full testing framework
func TestDatabaseSizeWithDatabase(t *testing.T) {
	// This test runs with the full test framework like our ACTION tests
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name: "database_size_extension_unit_test",
		FunctionTests: []kwilTesting.TestFunc{
			func(ctx context.Context, platform *kwilTesting.Platform) error {
				// Test GetDatabaseSize helper function
				size, err := GetDatabaseSize(ctx, platform.DB)
				if err != nil {
					t.Errorf("GetDatabaseSize failed: %v", err)
					return err
				}
				if size <= 0 {
					t.Errorf("Database size should be positive, got %d", size)
					return nil
				}
				t.Logf("Database size: %d bytes", size)

				// Test GetDatabaseSizePretty helper function
				prettySize, err := GetDatabaseSizePretty(ctx, platform.DB)
				if err != nil {
					t.Errorf("GetDatabaseSizePretty failed: %v", err)
					return err
				}
				if prettySize == "" {
					t.Errorf("Pretty size should not be empty")
					return nil
				}
				t.Logf("Database size (pretty): %s", prettySize)

				// Compare with direct query
				result, err := platform.DB.Execute(ctx, "SELECT pg_database_size('kwild')")
				if err != nil {
					t.Errorf("Direct query failed: %v", err)
					return err
				}
				if len(result.Rows) != 1 {
					t.Errorf("Expected 1 row, got %d", len(result.Rows))
					return nil
				}
				directSize := result.Rows[0][0].(int64)

				if directSize != size {
					t.Errorf("Extension and direct query should return same size: extension=%d, direct=%d", size, directSize)
				}

				return nil
			},
		},
	}, &kwilTesting.Options{
		UseTestContainer: true,
	})
}
