package tn_local

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
	kwilsql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/tests/utils"
)

func TestSetupSchema(t *testing.T) {
	var statements []string
	mockTx := &utils.MockTx{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			statements = append(statements, stmt)
			return &kwilsql.ResultSet{}, nil
		},
	}
	mockDB := &utils.MockDB{
		BeginTxFn: func(ctx context.Context) (kwilsql.Tx, error) {
			return mockTx, nil
		},
	}

	logger := log.New(log.WithWriter(io.Discard))
	localDB := NewLocalDB(mockDB, logger)

	err := localDB.SetupSchema(context.Background())
	require.NoError(t, err)

	// Verify schema creation
	require.True(t, containsSQL(statements, "CREATE SCHEMA IF NOT EXISTS "+SchemaName),
		"should create schema")

	// Verify streams table
	require.True(t, containsSQL(statements, SchemaName+".streams"),
		"should create streams table")
	require.True(t, containsSQL(statements, "data_provider TEXT NOT NULL"),
		"streams should have data_provider column")
	require.True(t, containsSQL(statements, "stream_type TEXT NOT NULL"),
		"streams should have stream_type column")

	// Verify primitive_events table
	require.True(t, containsSQL(statements, SchemaName+".primitive_events"),
		"should create primitive_events table")
	require.True(t, containsSQL(statements, "NUMERIC(36,18)"),
		"primitive_events should use NUMERIC(36,18) for value")

	// Verify primitive_events index
	require.True(t, containsSQL(statements, "local_pe_stream_time_idx"),
		"should create primitive_events index")

	// Verify taxonomies table
	require.True(t, containsSQL(statements, SchemaName+".taxonomies"),
		"should create taxonomies table")
	require.True(t, containsSQL(statements, "taxonomy_id UUID PRIMARY KEY"),
		"taxonomies should have UUID primary key")
}

func TestSetupSchema_RollbackOnError(t *testing.T) {
	rolledBack := false
	mockTx := &utils.MockTx{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			if strings.Contains(stmt, "streams") {
				return nil, context.DeadlineExceeded
			}
			return &kwilsql.ResultSet{}, nil
		},
		RollbackFn: func(ctx context.Context) error {
			rolledBack = true
			return nil
		},
	}
	mockDB := &utils.MockDB{
		BeginTxFn: func(ctx context.Context) (kwilsql.Tx, error) {
			return mockTx, nil
		},
	}

	logger := log.New(log.WithWriter(io.Discard))
	localDB := NewLocalDB(mockDB, logger)

	err := localDB.SetupSchema(context.Background())
	require.Error(t, err)
	require.True(t, rolledBack, "transaction should be rolled back on error")
}

func TestExtensionSingleton(t *testing.T) {
	// Reset for test isolation — never copy sync.Once (contains mutex)
	prev := extensionInstance
	extensionInstance = nil
	once = sync.Once{}
	t.Cleanup(func() {
		extensionInstance = prev
		once = sync.Once{}
		if prev != nil {
			once.Do(func() {}) // mark as done since instance already exists
		}
	})

	ext1 := GetExtension()
	ext2 := GetExtension()
	require.Same(t, ext1, ext2, "GetExtension should return same instance")
	require.False(t, ext1.isEnabled.Load(), "default extension should be disabled")

	// configure updates the existing instance in-place (preserves pointer identity).
	// Passing a non-empty node address transitions the extension to enabled.
	ext1.configure(ext1.logger, nil, nil, testNodeAddress)
	require.True(t, ext1.isEnabled.Load())
	require.Equal(t, testNodeAddress, ext1.nodeAddress)
	require.Same(t, ext1, GetExtension(), "still same pointer after configure")
}

func TestExtensionConfigureEmptyAddressStaysDisabled(t *testing.T) {
	// configure() with an empty node address must NOT enable the extension —
	// this is the read-only / ed25519 fallback path. Handlers will refuse
	// requests with the disabled error.
	prev := extensionInstance
	extensionInstance = nil
	once = sync.Once{}
	t.Cleanup(func() {
		extensionInstance = prev
		once = sync.Once{}
		if prev != nil {
			once.Do(func() {})
		}
	})

	ext := GetExtension()
	ext.configure(ext.logger, nil, nil, "")
	require.False(t, ext.isEnabled.Load(), "empty node address must keep extension disabled")
	require.Empty(t, ext.nodeAddress)
}

func TestServiceInterface(t *testing.T) {
	ext := &Extension{}
	ext.isEnabled.Store(true)

	require.Equal(t, ServiceName, ext.Name())

	methods := ext.Methods()
	require.Contains(t, methods, jsonrpc.Method("local.create_stream"))
	require.Contains(t, methods, jsonrpc.Method("local.insert_records"))
	require.Contains(t, methods, jsonrpc.Method("local.insert_taxonomy"))
	require.Contains(t, methods, jsonrpc.Method("local.get_record"))
	require.Contains(t, methods, jsonrpc.Method("local.get_index"))
	require.Contains(t, methods, jsonrpc.Method("local.list_streams"))

	health, ok := ext.Health(context.Background())
	require.True(t, ok)
	require.NotNil(t, health)
}

// testNodeAddress is the lowercase Ethereum address used as the implicit
// data_provider for handler tests. It is the lowercase form of testDP, so
// any test that previously asserted captured args matched a lowercased
// testDP keeps working without value changes.
const testNodeAddress = "0xec36224a679218ae28fcece8d3c68595b87dd832"

// newTestExtension creates an Extension with a mock DB and a stub node address
// for handler tests. The node address mirrors what the production engineReadyHook
// would derive from the secp256k1 ValidatorSigner.
func newTestExtension(db kwilsql.DB) *Extension {
	ext := &Extension{
		logger:      log.New(log.WithWriter(io.Discard)),
		db:          db,
		nodeAddress: testNodeAddress,
	}
	ext.isEnabled.Store(true)
	return ext
}

func containsSQL(statements []string, substr string) bool {
	for _, s := range statements {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}
