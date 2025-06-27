package extensions

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientTypes "github.com/trufnetwork/kwil-db/core/client/types"
	"github.com/trufnetwork/node/tests/setup"
)

// This test verifies that the graceful check extension works correctly in a Kwil database.
// It tests the integration between a custom Kwil node and the hello extension by:
// 1. Building the custom Docker image with extensions if it doesn't exist
// 2. Setting up a single Kwil node with the custom Docker image
// 3. Creating a database schema that uses the hello extension
// 4. Deploying the schema to the node
// 5. Executing a graceful check action that calls the hello.greet() function
// 6. Verifying that the extension responds correctly

const (
	dockerImageName = "tn-db:local"
	testChainID     = "truflation-dev"
)

func TestGracefulCheckExtension(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Check if Docker image exists, build if not
	err := ensureDockerImageExists(ctx)
	require.NoError(t, err, "Failed to ensure Docker image exists")

	// Generate a test private key for signing transactions
	privateKey, err := setup.GenerateKey()
	require.NoError(t, err, "Failed to generate private key")

	// Create test fixture
	fixture := setup.NewSimpleNodeFixture(t)
	defer func() {
		if teardownErr := fixture.Teardown(ctx); teardownErr != nil {
			t.Logf("Warning: failed to teardown fixture: %v", teardownErr)
		}
	}()

	// Setup the node with our custom image and use privateKey as database owner
	config := &setup.KwilNodeConfig{
		DBOwnerPrivateKey: privateKey,
		ChainID:           testChainID,
	}

	err = fixture.Setup(ctx, dockerImageName, config)
	require.NoError(t, err, "Failed to setup test fixture")

	// Check what extensions and namespaces are available using read-only client
	readOnlyClient := fixture.Client()
	namespacesResult, err := readOnlyClient.Query(ctx, "SELECT name FROM info.namespaces", nil, true)
	require.NoError(t, err, "Failed to query namespaces")
	t.Logf("Available namespaces: %v", namespacesResult.Values)

	// Create a client signed with the DB owner's key for deploying schemas

	dbOwnerClient, err := fixture.CreateDBOwnerClient(ctx)
	require.NoError(t, err, "Failed to create DB owner client")

	dbOwnerAddress := setup.GetEthereumAddress(privateKey)
	t.Logf("DB owner address: %s", dbOwnerAddress)

	// Read the schema SQL
	schemaSQL, err := readSchemaFile()
	require.NoError(t, err, "Failed to read schema file")

	// Deploy the schema
	txHash, err := dbOwnerClient.ExecuteSQL(ctx, schemaSQL, nil, clientTypes.WithSyncBroadcast(true))
	require.NoError(t, err, "Failed to deploy schema")

	// Wait for the transaction to be confirmed
	txResp, err := fixture.WaitForTx(ctx, txHash, 30*time.Second)
	require.NoError(t, err, "Failed to wait for schema deployment transaction")
	require.Equal(t, uint32(0), txResp.Result.Code, "Schema deployment failed: %s", txResp.Result.Log)

	// Call the graceful_check action
	result, err := dbOwnerClient.Call(ctx, "main", "graceful_check", nil)
	require.NoError(t, err, "Failed to call graceful_check action")

	// Verify the result
	require.NotNil(t, result, "Call result should not be nil")
	require.NotNil(t, result.QueryResult, "Query result should not be nil")

	// Expect three greeting rows
	values := result.QueryResult.Values
	require.Len(t, values, 3, "Should return exactly three records")

	expected := []string{"hello", "hola", "bonjour"}
	for i, record := range values {
		require.Len(t, record, 1, "Each record should have one field")
		assert.Equal(t, expected[i], record[0], "Unexpected greeting at row %d", i)
	}

	t.Logf("✓ Extension test passed: received %d greeting messages", len(values))
}

// ensureDockerImageExists checks if the Docker image exists and builds it if not
func ensureDockerImageExists(ctx context.Context) error {
	// Always rebuild the image to ensure latest extension code is included
	fmt.Printf("Building (or rebuilding) Docker image %s...\n", dockerImageName)

	projectRoot, err := getProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to find project root: %w", err)
	}

	buildCmd := exec.CommandContext(ctx, "docker", "build",
		"-t", dockerImageName,
		"-f", "deployments/Dockerfile",
		".",
	)
	buildCmd.Dir = projectRoot
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr

	if err := buildCmd.Run(); err != nil {
		return fmt.Errorf("failed to build Docker image: %w", err)
	}

	fmt.Printf("Successfully built (or rebuilt) Docker image %s\n", dockerImageName)
	return nil
}

// getProjectRoot finds the tsn-db project root directory
func getProjectRoot() (string, error) {
	// Start from current directory and walk up to find tsn-db root
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		// Check if this directory contains go.mod and deployments/Dockerfile
		goModPath := filepath.Join(dir, "go.mod")
		dockerfilePath := filepath.Join(dir, "deployments", "Dockerfile")

		if _, err := os.Stat(goModPath); err == nil {
			if _, err := os.Stat(dockerfilePath); err == nil {
				return dir, nil
			}
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached filesystem root
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("could not find tsn-db project root")
}

// readSchemaFile reads the graceful_check.sql schema file
func readSchemaFile() (string, error) {
	// Get the directory of this test file
	testDir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Navigate to the schema file
	var schemaPath string
	if strings.Contains(testDir, "tests/extensions") {
		schemaPath = filepath.Join(testDir, "graceful_check.sql")
	} else {
		// We might be running from project root
		schemaPath = filepath.Join(testDir, "tests", "extensions", "graceful_check.sql")
	}

	content, err := os.ReadFile(schemaPath)
	if err != nil {
		return "", fmt.Errorf("failed to read schema file %s: %w", schemaPath, err)
	}

	return string(content), nil
}
