package utils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// kwildGRPCPort is the internal gRPC port for Kwil's Tendermint RPC
	kwildGRPCPort = "26657/tcp"
	// kwildRPCPort is the internal port for the Kwil JSON-RPC service
	kwildRPCPort = "8484/tcp"
	// postgresPort is the internal port for PostgreSQL
	postgresPort = "5432/tcp"

	postgresImage = "kwildb/postgres:16.8-1"
	postgresUser  = "kwild"
	postgresPass  = "kwild"
	postgresDB    = "kwild"
)

// KwilNodeConfig allows for programmatic configuration of a kwild node for testing.
type KwilNodeConfig struct {
	// DBOwnerPubKey is the public key of the database owner.
	DBOwnerPubKey string
	// Any additional configuration parameters can be added here.
}

// LocalNodeFixture manages the lifecycle of a kwild node and a backing PostgreSQL database using testcontainers-go.
type LocalNodeFixture struct {
	t *testing.T

	// KwilProvider is the public RPC endpoint for the kwild node.
	KwilProvider string
	// KwilGRPCProvider is the public gRPC endpoint for the kwild node.
	KwilGRPCProvider string
	// tempDir is the temporary directory holding test assets (e.g., config files).
	tempDir string

	network  testcontainers.Network
	postgres testcontainers.Container
	kwild    testcontainers.Container
}

// NewLocalNodeFixture creates a new, uninitialized test fixture.
func NewLocalNodeFixture(t *testing.T) *LocalNodeFixture {
	return &LocalNodeFixture{t: t}
}

// Setup initializes the test environment. It creates a network, starts a PostgreSQL container,
// generates a kwild configuration, and starts the kwild node container.
// It requires a pre-built kwild docker image tag.
func (f *LocalNodeFixture) Setup(ctx context.Context, kwilImageTag string, cfg *KwilNodeConfig) (err error) {
	f.t.Log("Setting up local node fixture...")

	// Defer a cleanup function that checks if the test failed.
	// This ensures resources are terminated, but artifacts are preserved on failure.
	defer func() {
		if err != nil {
			f.t.Logf("Setup failed: %v. Tearing down...", err)
			f.Teardown(ctx) // Attempt to clean up even on setup failure.
		}
	}()

	// 1. Create a temporary directory for configs and data.
	f.tempDir, err = os.MkdirTemp("", "kwil-test-")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	// 2. Create a new Docker network for the test environment.
	net, err := testcontainers.NewNetwork(ctx, testcontainers.NetworkRequest{
		Name:           fmt.Sprintf("kwil-test-net-%s", randomSuffix(f.t)),
		CheckDuplicate: true,
	})
	if err != nil {
		return fmt.Errorf("failed to create network: %w", err)
	}
	f.network = net

	// 3. Start PostgreSQL container.
	pgAlias := "postgres"
	pgReq := testcontainers.ContainerRequest{
		Image:        postgresImage,
		ExposedPorts: []string{postgresPort},
		Name:         fmt.Sprintf("postgres-%s", randomSuffix(f.t)),
		Hostname:     pgAlias,
		Networks:     []string{f.network.Name},
		Env: map[string]string{
			"POSTGRES_HOST_AUTH_METHOD": "trust",
			"POSTGRES_INITDB_ARGS":      "--data-checksums",
			"POSTGRES_USER":             postgresUser,
			"POSTGRES_PASSWORD":         postgresPass,
			"POSTGRES_DB":               postgresDB,
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(20 * time.Second),
	}
	f.postgres, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: pgReq,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to start postgres container: %w", err)
	}

	// 4. Generate kwild config.toml
	configPath := filepath.Join(f.tempDir, "config")
	require.NoError(f.t, os.Mkdir(configPath, 0755))
	if err = f.generateKwildConfig(configPath, pgAlias, cfg); err != nil {
		return fmt.Errorf("failed to generate kwild config: %w", err)
	}

	// 5. Start kwild container.
	kwildAlias := "kwild"
	kwildReq := testcontainers.ContainerRequest{
		Image:        kwilImageTag,
		ExposedPorts: []string{kwildRPCPort, kwildGRPCPort},
		Name:         fmt.Sprintf("kwild-%s", randomSuffix(f.t)),
		Hostname:     kwildAlias,
		Networks:     []string{f.network.Name},
		WaitingFor: wait.ForHTTP("/api/v1/health").WithPort(kwildRPCPort).WithStatusCode(200).
			WithStartupTimeout(30 * time.Second),
		Cmd: []string{
			"start",
			"--root", "/home/kwil/.kwild", // kwild user home dir in container
		},
		Mounts: testcontainers.ContainerMounts{
			testcontainers.BindMount(configPath, "/home/kwil/.kwild/config"),
		},
	}
	f.kwild, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: kwildReq,
		Started:          true,
	})
	if err != nil {
		return fmt.Errorf("failed to start kwild container: %w", err)
	}

	// 6. Get public endpoints for the services.
	rpcHost, err := f.kwild.Host(ctx)
	require.NoError(f.t, err)
	rpcPort, err := f.kwild.MappedPort(ctx, kwildRPCPort)
	require.NoError(f.t, err)
	f.KwilProvider = fmt.Sprintf("http://%s:%s", rpcHost, rpcPort.Port())

	grpcPort, err := f.kwild.MappedPort(ctx, kwildGRPCPort)
	require.NoError(f.t, err)
	f.KwilGRPCProvider = fmt.Sprintf("%s:%s", rpcHost, grpcPort.Port())

	f.t.Logf("✅ Local node fixture setup complete. Kwil provider: %s", f.KwilProvider)
	return nil
}

// Teardown terminates all containers and networks. If the test has failed,
// it preserves the temporary directory with logs and configs for debugging.
func (f *LocalNodeFixture) Teardown(ctx context.Context) {
	f.t.Log("Tearing down local node fixture...")

	if f.kwild != nil {
		if err := f.kwild.Terminate(ctx); err != nil {
			f.t.Logf("Failed to terminate kwild container: %v", err)
		}
	}
	if f.postgres != nil {
		if err := f.postgres.Terminate(ctx); err != nil {
			f.t.Logf("Failed to terminate postgres container: %v", err)
		}
	}
	if f.network != nil {
		if err := f.network.Remove(ctx); err != nil {
			f.t.Logf("Failed to remove network: %v", err)
		}
	}

	if f.t.Failed() {
		f.t.Logf("🚨 Test failed. Preserving test artifacts in: %s", f.tempDir)
	} else {
		if f.tempDir != "" {
			if err := os.RemoveAll(f.tempDir); err != nil {
				f.t.Logf("Failed to remove temp directory: %v", err)
			}
		}
	}
}

// generateKwildConfig creates a config.toml file for the kwild node.
func (f *LocalNodeFixture) generateKwildConfig(configDir, pgHost string, cfg *KwilNodeConfig) error {
	configToml := `
# This is a simplified config file for integration testing.
root = "/home/kwil/.kwild"
db_owner = "%s"

[app]
json_rpc_address = "tcp://0.0.0.0:8484"

[db]
  host = "%s"
  port = %d
  user = "%s"
  pass = "%s"
  name = "%s"

[consensus]
  create_empty_blocks = true
  create_empty_blocks_interval = "30s"
  timeout_propose = "500ms"
  timeout_prevote = "200ms"
  timeout_precommit = "200ms"
  timeout_commit = "1s"

[p2p]
  listen_address = "tcp://0.0.0.0:26656"
  persistent_peers = ""
  seeds = ""
`
	configContent := fmt.Sprintf(configToml, cfg.DBOwnerPubKey, pgHost, 5432, postgresUser, postgresPass, postgresDB)
	return os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(configContent), 0644)
}

// randomSuffix creates a short random string for naming test resources to avoid collisions.
func randomSuffix(t *testing.T) string {
	// In a real scenario, you might use a library like "github.com/google/uuid"
	// but for simplicity, we use the test name and a timestamp.
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
