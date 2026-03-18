package tn_local

import (
	"context"
	"fmt"
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

	// configure updates the existing instance in-place (preserves pointer identity)
	ext1.configure(ext1.logger, nil, nil)
	require.True(t, ext1.isEnabled.Load())
	require.Same(t, ext1, GetExtension(), "still same pointer after configure")
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

// newTestExtension creates an Extension with a mock DB for handler tests.
func newTestExtension(db kwilsql.DB) *Extension {
	ext := &Extension{
		logger: log.New(log.WithWriter(io.Discard)),
		db:     db,
	}
	ext.isEnabled.Store(true)
	return ext
}

func TestCreateStream_Success(t *testing.T) {
	var capturedStmt string
	var capturedArgs []any
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			capturedStmt = stmt
			capturedArgs = args
			return &kwilsql.ResultSet{}, nil
		},
	}
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.CreateStream(context.Background(), &CreateStreamRequest{
		DataProvider: "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832",
		StreamID:     "st00000000000000000000000000test",
		StreamType:   "primitive",
	})

	require.Nil(t, rpcErr, "expected no error")
	require.NotNil(t, resp)
	require.Contains(t, capturedStmt, "INSERT INTO "+SchemaName+".streams")
	require.Equal(t, "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832", capturedArgs[0])
	require.Equal(t, "st00000000000000000000000000test", capturedArgs[1])
	require.Equal(t, "primitive", capturedArgs[2])
}

func TestCreateStream_ComposedType(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			return &kwilsql.ResultSet{}, nil
		},
	}
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.CreateStream(context.Background(), &CreateStreamRequest{
		DataProvider: "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832",
		StreamID:     "st00000000000000000000000000test",
		StreamType:   "composed",
	})

	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
}

func TestCreateStream_InvalidStreamID(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	tests := []struct {
		name     string
		streamID string
		wantMsg  string
	}{
		{"too short", "st00", "must be exactly 32 characters"},
		{"too long", "st000000000000000000000000000test1", "must be exactly 32 characters"},
		{"wrong prefix", "xx00000000000000000000000000test", "must start with 'st'"},
		{"empty", "", "must be exactly 32 characters"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, rpcErr := ext.CreateStream(context.Background(), &CreateStreamRequest{
				DataProvider: "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832",
				StreamID:     tt.streamID,
				StreamType:   "primitive",
			})
			require.NotNil(t, rpcErr)
			require.Contains(t, rpcErr.Message, tt.wantMsg)
		})
	}
}

func TestCreateStream_InvalidStreamType(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.CreateStream(context.Background(), &CreateStreamRequest{
		DataProvider: "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832",
		StreamID:     "st00000000000000000000000000test",
		StreamType:   "invalid",
	})
	require.NotNil(t, rpcErr)
	require.Contains(t, rpcErr.Message, "must be 'primitive' or 'composed'")
}

func TestCreateStream_InvalidDataProvider(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	tests := []struct {
		name         string
		dataProvider string
	}{
		{"no 0x prefix", "EC36224A679218Ae28FCeCe8d3c68595B87Dd832"},
		{"too short", "0xEC36224A679218Ae28"},
		{"too long", "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832FF"},
		{"invalid chars", "0xGG36224A679218Ae28FCeCe8d3c68595B87Dd832"},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, rpcErr := ext.CreateStream(context.Background(), &CreateStreamRequest{
				DataProvider: tt.dataProvider,
				StreamID:     "st00000000000000000000000000test",
				StreamType:   "primitive",
			})
			require.NotNil(t, rpcErr)
			require.Contains(t, rpcErr.Message, "data_provider must be a valid Ethereum address")
		})
	}
}

func TestCreateStream_DuplicateStream(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			return nil, fmt.Errorf("duplicate key value violates unique constraint")
		},
	}
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.CreateStream(context.Background(), &CreateStreamRequest{
		DataProvider: "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832",
		StreamID:     "st00000000000000000000000000test",
		StreamType:   "primitive",
	})
	require.NotNil(t, rpcErr)
	require.Contains(t, rpcErr.Message, "stream already exists")
}

func TestCreateStream_DBError(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.CreateStream(context.Background(), &CreateStreamRequest{
		DataProvider: "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832",
		StreamID:     "st00000000000000000000000000test",
		StreamType:   "primitive",
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInternal), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "failed to create stream")
}

func containsSQL(statements []string, substr string) bool {
	for _, s := range statements {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}
