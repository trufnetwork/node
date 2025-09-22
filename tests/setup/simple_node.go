package setup

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/trufnetwork/kwil-db/core/client"
	clientTypes "github.com/trufnetwork/kwil-db/core/client/types"
	"github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/kwil-db/core/types"
	authExt "github.com/trufnetwork/kwil-db/extensions/auth"
)

// This file provides a simple single-node test fixture for Kwil database testing.
// It sets up a containerized Kwil node with PostgreSQL backend for integration tests.
// The fixture handles container lifecycle, client connections, and basic node configuration.
// should be able to interact with the node using the client
// SimpleNodeFixture provides a simple single-node setup for testing extensions
type SimpleNodeFixture struct {
	t           *testing.T
	container   testcontainers.Container
	pgContainer testcontainers.Container
	endpoint    string
	client      *client.Client
	config      *KwilNodeConfig
}

// KwilNodeConfig holds configuration for the kwild node
type KwilNodeConfig struct {
	DBOwnerPrivateKey crypto.PrivateKey
	ChainID           string
	Image             string
}

// NewSimpleNodeFixture creates a new simple node fixture
func NewSimpleNodeFixture(t *testing.T) *SimpleNodeFixture {
	return &SimpleNodeFixture{
		t: t,
	}
}

// Setup starts a single kwild node with the given configuration
func (f *SimpleNodeFixture) Setup(ctx context.Context, image string, config *KwilNodeConfig) error {
	if config == nil {
		config = &KwilNodeConfig{}
	}

	// Set defaults
	if config.ChainID == "" {
		config.ChainID = "kwil-testnet"
	}
	if config.DBOwnerPrivateKey == nil {
		panic("db owner private key is nil")
	}

	// Store config for later use
	f.config = config

	// Create a Docker network for container communication
	networkName := "kwil-test-network"
	network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name: networkName,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create network: %w", err)
	}
	defer network.Remove(ctx) // Clean up network when done

	// Start PostgreSQL container first
	pgContainer, err := f.startPostgres(ctx, networkName)
	if err != nil {
		return fmt.Errorf("failed to start postgres: %w", err)
	}
	f.pgContainer = pgContainer

	// Start kwild container
	kwildContainer, err := f.startKwild(ctx, image, config, networkName, "postgres", "5432")
	if err != nil {
		return fmt.Errorf("failed to start kwild: %w", err)
	}

	f.container = kwildContainer

	// Get kwild endpoint
	kwildHost, err := kwildContainer.Host(ctx)
	if err != nil {
		return fmt.Errorf("failed to get kwild host: %w", err)
	}

	kwildPort, err := kwildContainer.MappedPort(ctx, "8484")
	if err != nil {
		return fmt.Errorf("failed to get kwild port: %w", err)
	}

	f.endpoint = fmt.Sprintf("http://%s:%s", kwildHost, kwildPort.Port())

	// Create read-only client (no signer)
	f.client, err = client.NewClient(ctx, f.endpoint, &clientTypes.Options{
		ChainID: config.ChainID,
	})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	return nil
}

// startPostgres starts a PostgreSQL container
func (f *SimpleNodeFixture) startPostgres(ctx context.Context, networkName string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "kwildb/postgres:16.8-1",
		ExposedPorts: []string{"5432/tcp"},
		Name:         "postgres",
		Networks:     []string{networkName},
		Env: map[string]string{
			"POSTGRES_DB":       "kwil",
			"POSTGRES_USER":     "kwil",
			"POSTGRES_PASSWORD": "kwil",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(30 * time.Second),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// startKwild starts a kwild container
func (f *SimpleNodeFixture) startKwild(ctx context.Context, image string, config *KwilNodeConfig, networkName, pgHost, pgPort string) (testcontainers.Container, error) {
	log.Println("Starting kwild container")

	// Get the proper DB owner identifier using the same method as kwild setup init
	signer := auth.GetUserSigner(config.DBOwnerPrivateKey)
	dbOwnerIdentifier, err := authExt.GetIdentifierFromSigner(signer)
	if err != nil {
		return nil, fmt.Errorf("failed to get DB owner identifier: %w", err)
	}

	log.Println("db owner identifier:", dbOwnerIdentifier)
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"8484/tcp"},
		Networks:     []string{networkName},
		Env: map[string]string{
			// Database connection
			"KWILD_DB_HOST": pgHost,
			"KWILD_DB_PORT": pgPort,
			"KWILD_DB_USER": "kwil",
			"KWILD_DB_PASS": "kwil",
			"KWILD_DB_NAME": "kwil",
			// Required for config.sh initialization
			"SETUP_CHAIN_ID": config.ChainID,
			"SETUP_DB_OWNER": dbOwnerIdentifier,
			// Additional configuration overrides
			"KWILD_APP_JSONRPC_LISTEN_ADDR": "0.0.0.0:8484",
			"KWILD_APP_P2P_LISTEN_ADDR":     "0.0.0.0:6600",
			"KWILD_APP_ADMIN_LISTEN_ADDR":   "0.0.0.0:8485",
			"KWILD_LOG_LEVEL":               "info",
			"KWILD_LOG_FORMAT":              "plain",
		},
		// Don't override Cmd - let the container use its default entrypoint
		WaitingFor: wait.ForLog("JSON-RPC server listening").
			WithStartupTimeout(60 * time.Second),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

// Client returns the client for interacting with the node
func (f *SimpleNodeFixture) Client() *client.Client {
	return f.client
}

// Endpoint returns the JSON-RPC endpoint
func (f *SimpleNodeFixture) Endpoint() string {
	return f.endpoint
}

// Teardown stops and removes the containers
func (f *SimpleNodeFixture) Teardown(ctx context.Context) error {
	var errs []error

	if f.container != nil {
		if err := f.container.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to terminate kwild container: %w", err))
		}
	}

	if f.pgContainer != nil {
		if err := f.pgContainer.Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to terminate postgres container: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("teardown errors: %v", errs)
	}

	return nil
}

// CreateClient creates a new client with optional signer
func (f *SimpleNodeFixture) CreateClient(ctx context.Context, privateKey crypto.PrivateKey) (*client.Client, error) {
	// Get chain ID from the main client
	chainInfo, err := f.client.ChainInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain info: %w", err)
	}

	options := &clientTypes.Options{
		ChainID: chainInfo.ChainID,
	}

	// Add signer if private key provided
	if privateKey != nil {
		signer := auth.GetUserSigner(privateKey)
		options.Signer = signer
	}

	return client.NewClient(ctx, f.endpoint, options)
}

// CreateSignedClient creates a client with a signer for sending transactions
// Deprecated: Use CreateClient instead
func (f *SimpleNodeFixture) CreateSignedClient(ctx context.Context, privateKey crypto.PrivateKey) (*client.Client, error) {
	return f.CreateClient(ctx, privateKey)
}

// CreateDBOwnerClient creates a client signed with the DB owner's private key
func (f *SimpleNodeFixture) CreateDBOwnerClient(ctx context.Context) (*client.Client, error) {
	if f.config == nil || f.config.DBOwnerPrivateKey == nil {
		return nil, fmt.Errorf("no DB owner private key available")
	}
	return f.CreateClient(ctx, f.config.DBOwnerPrivateKey)
}

// CreateReadOnlyClient creates a client without a signer (for queries only)
func (f *SimpleNodeFixture) CreateReadOnlyClient(ctx context.Context) (*client.Client, error) {
	return f.CreateClient(ctx, nil)
}

// WaitForTx waits for a transaction to be confirmed
func (f *SimpleNodeFixture) WaitForTx(ctx context.Context, txHash types.Hash, timeout time.Duration) (*types.TxQueryResponse, error) {
	return f.client.WaitTx(ctx, txHash, timeout)
}

// GenerateKey generates a new secp256k1 private key
func GenerateKey() (crypto.PrivateKey, error) {
	pk, _, err := crypto.GenerateSecp256k1Key(nil)
	return pk, err
}

// GetEthereumAddress returns the Ethereum address for a private key using the same method as Kwil
func GetEthereumAddress(privateKey crypto.PrivateKey) string {
	signer := auth.GetUserSigner(privateKey)
	identifier, err := authExt.GetIdentifierFromSigner(signer)
	if err != nil {
		panic(fmt.Sprintf("failed to get identifier: %v", err))
	}
	return identifier
}
