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
	testChainID     = "trufnetwork-dev"
	testOwner       = "0x742d35Cc6652C0532925a3b8d6Fd5d607E2B93F1"
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

	// Get the user identifier from the private key to use as DB owner
	userID, err := setup.CreateAccountIdentifier(privateKey.Public())
	require.NoError(t, err, "Failed to create account identifier")
	dbOwner := fmt.Sprintf("0x%x", userID.Identifier)

	// Create test fixture
	fixture := setup.NewSimpleNodeFixture(t)
	defer func() {
		if teardownErr := fixture.Teardown(ctx); teardownErr != nil {
			t.Logf("Warning: failed to teardown fixture: %v", teardownErr)
		}
	}()

	// Setup the node with our custom image and the generated user as owner
	config := &setup.KwilNodeConfig{
		DBOwnerPubKey: dbOwner,
		ChainID:       testChainID,
	}

	err = fixture.Setup(ctx, dockerImageName, config)
	require.NoError(t, err, "Failed to setup test fixture")

	// Check what extensions and namespaces are available
	client := fixture.Client()
	namespacesResult, err := client.Query(ctx, "SELECT name FROM info.namespaces", nil, true)
	require.NoError(t, err, "Failed to query namespaces")
	t.Logf("Available namespaces: %v", namespacesResult.Values)

	extensionsResult, err := client.Query(ctx, "SELECT namespace, extension FROM info.extensions", nil, true)
	if err != nil {
		t.Logf("Failed to query extensions (might be expected): %v", err)
	} else {
		t.Logf("Available extensions: %v", extensionsResult.Values)
	}

	// Create a signed client for deploying schemas
	signedClient, err := fixture.CreateSignedClient(ctx, privateKey)
	require.NoError(t, err, "Failed to create signed client")

	// Read the schema SQL
	schemaSQL, err := readSchemaFile()
	require.NoError(t, err, "Failed to read schema file")

	// Deploy the schema
	txHash, err := signedClient.ExecuteSQL(ctx, schemaSQL, nil, clientTypes.WithSyncBroadcast(true))
	require.NoError(t, err, "Failed to deploy schema")

	// Wait for the transaction to be confirmed
	txResp, err := fixture.WaitForTx(ctx, txHash, 30*time.Second)
	require.NoError(t, err, "Failed to wait for schema deployment transaction")
	require.Equal(t, uint32(0), txResp.Result.Code, "Schema deployment failed: %s", txResp.Result.Log)

	// Call the graceful_check action
	result, err := signedClient.Call(ctx, "hello", "graceful_check", nil)
	require.NoError(t, err, "Failed to call graceful_check action")

	// Verify the result
	require.NotNil(t, result, "Call result should not be nil")
	require.NotNil(t, result.QueryResult, "Query result should not be nil")

	// The hello extension should return "hello"
	values := result.QueryResult.Values
	require.Len(t, values, 1, "Should return exactly one record")

	record := values[0]
	require.Len(t, record, 1, "Record should have one field")
	msg := record[0]
	assert.Equal(t, "hello", msg, "Extension should return 'hello'")

	t.Logf("✓ Extension test passed: received expected message '%s'", msg)
}

// ensureDockerImageExists checks if the Docker image exists and builds it if not
func ensureDockerImageExists(ctx context.Context) error {
	// Check if image already exists
	cmd := exec.CommandContext(ctx, "docker", "images", "-q", dockerImageName)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to check for existing Docker image: %w", err)
	}

	// If image exists, we're done
	if strings.TrimSpace(string(output)) != "" {
		fmt.Printf("Docker image %s already exists\n", dockerImageName)
		return nil
	}

	// Build the image
	fmt.Printf("Building Docker image %s...\n", dockerImageName)

	// Get the project root directory
	projectRoot, err := getProjectRoot()
	if err != nil {
		return fmt.Errorf("failed to find project root: %w", err)
	}

	// Build command: docker build -t tn-db:local -f deployments/Dockerfile .
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

	fmt.Printf("Successfully built Docker image %s\n", dockerImageName)
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
