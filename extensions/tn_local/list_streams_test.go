package tn_local

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
	kwilsql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/tests/utils"
)

func TestListStreams_Empty(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			return &kwilsql.ResultSet{Rows: [][]any{}}, nil
		},
	}
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.ListStreams(context.Background(), &ListStreamsRequest{})
	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.Empty(t, resp.Streams)
}

func TestListStreams_ReturnsAll(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			return &kwilsql.ResultSet{
				Columns: []string{"data_provider", "stream_id", "stream_type", "created_at"},
				Rows: [][]any{
					{"0xabc123", "st00000000000000000000000000aaaa", "primitive", int64(100)},
					{"0xabc123", "st00000000000000000000000000bbbb", "composed", int64(200)},
				},
			}, nil
		},
	}
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.ListStreams(context.Background(), &ListStreamsRequest{})
	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.Len(t, resp.Streams, 2)

	require.Equal(t, "0xabc123", resp.Streams[0].DataProvider)
	require.Equal(t, "st00000000000000000000000000aaaa", resp.Streams[0].StreamID)
	require.Equal(t, "primitive", resp.Streams[0].StreamType)
	require.Equal(t, int64(100), resp.Streams[0].CreatedAt)

	require.Equal(t, "0xabc123", resp.Streams[1].DataProvider)
	require.Equal(t, "st00000000000000000000000000bbbb", resp.Streams[1].StreamID)
	require.Equal(t, "composed", resp.Streams[1].StreamType)
	require.Equal(t, int64(200), resp.Streams[1].CreatedAt)
}

func TestListStreams_DBError(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.ListStreams(context.Background(), &ListStreamsRequest{})
	require.Nil(t, resp)
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInternal), rpcErr.Code)
}
