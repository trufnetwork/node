package tn_local

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
	kwilsql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/tests/utils"
)

func TestCreateStream_NilRequest(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	resp, rpcErr := ext.CreateStream(context.Background(), nil)
	require.Nil(t, resp)
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "missing request")
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
	ext.height.Store(42)

	resp, rpcErr := ext.CreateStream(context.Background(), &CreateStreamRequest{
		DataProvider: "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832",
		StreamID:     "st00000000000000000000000000test",
		StreamType:   "primitive",
	})

	require.Nil(t, rpcErr, "expected no error")
	require.NotNil(t, resp)
	require.Contains(t, capturedStmt, "INSERT INTO "+SchemaName+".streams")
	require.Len(t, capturedArgs, 4, "INSERT should have 4 parameters")
	// data_provider should be lowercased (matching consensus behavior)
	require.Equal(t, "0xec36224a679218ae28fcece8d3c68595b87dd832", capturedArgs[0])
	require.Equal(t, "st00000000000000000000000000test", capturedArgs[1])
	require.Equal(t, "primitive", capturedArgs[2])
	// created_at should propagate the block height set on the extension
	createdAt, ok := capturedArgs[3].(int64)
	require.True(t, ok, "created_at should be int64")
	require.Equal(t, int64(42), createdAt, "created_at should equal the block height")
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
			require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
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
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
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
			require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
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
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "stream already exists")
}

func TestCreateStream_DuplicateStream_PgError(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			return nil, &pgconn.PgError{Code: pgUniqueViolation, Message: "unique_violation"}
		},
	}
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.CreateStream(context.Background(), &CreateStreamRequest{
		DataProvider: "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832",
		StreamID:     "st00000000000000000000000000test",
		StreamType:   "primitive",
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
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
