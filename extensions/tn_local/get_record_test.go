package tn_local

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
	kwilsql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/tests/utils"
)

func TestGetRecord_NilRequest(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	resp, rpcErr := ext.GetRecord(context.Background(), nil)
	require.Nil(t, resp)
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "missing request")
}

func TestGetRecord_StreamNotFound(t *testing.T) {
	mockDB := mockDBWithStream(0, "", nil)
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.GetRecord(context.Background(), &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     testSID,
	})
	require.Nil(t, resp)
	require.NotNil(t, rpcErr)
	require.Contains(t, rpcErr.Message, "stream not found")
}

func TestGetRecord_InvalidDataProvider(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.GetRecord(context.Background(), &GetRecordRequest{
		DataProvider: "invalid",
		StreamID:     testSID,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
}

func TestGetRecord_InvalidStreamID(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.GetRecord(context.Background(), &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     "short",
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
}

func TestGetRecord_FromAfterTo(t *testing.T) {
	mockDB := mockDBForQuery(1, "primitive", nil)
	ext := newTestExtension(mockDB)

	from := int64(2)
	to := int64(1)
	resp, rpcErr := ext.GetRecord(context.Background(), &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
	})
	require.Nil(t, resp)
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "from_time must be <= to_time")
}

func TestGetRecord_Primitive_LatestRecord(t *testing.T) {
	// Both from and to nil → return latest record
	mockDB := mockDBForQuery(1, "primitive", [][]any{
		{int64(5000), "99.500000000000000000"},
	})
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.GetRecord(context.Background(), &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     testSID,
	})
	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.Len(t, resp.Records, 1)
	require.Equal(t, int64(5000), resp.Records[0].EventTime)
	require.Equal(t, "99.500000000000000000", resp.Records[0].Value)
}

func TestGetRecord_Primitive_TimeRange(t *testing.T) {
	from := int64(1000)
	to := int64(5000)
	mockDB := mockDBForQuery(1, "primitive", [][]any{
		{int64(1000), "10.000000000000000000"},
		{int64(2000), "20.000000000000000000"},
		{int64(3000), "30.000000000000000000"},
	})
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.GetRecord(context.Background(), &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
	})
	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.Len(t, resp.Records, 3)
	require.Equal(t, int64(1000), resp.Records[0].EventTime)
	require.Equal(t, int64(2000), resp.Records[1].EventTime)
	require.Equal(t, int64(3000), resp.Records[2].EventTime)
}

func TestGetRecord_Primitive_EmptyResult(t *testing.T) {
	mockDB := mockDBForQuery(1, "primitive", nil)
	ext := newTestExtension(mockDB)

	from := int64(1000)
	to := int64(5000)
	resp, rpcErr := ext.GetRecord(context.Background(), &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
	})
	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Records, "Records should be non-nil empty slice, not nil")
	require.Empty(t, resp.Records)
}

func TestGetRecord_Composed_Dispatches(t *testing.T) {
	// Verify that composed streams route to the composed query path
	mockDB := mockDBForQuery(1, "composed", [][]any{
		{int64(1000), "50.000000000000000000"},
	})
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.GetRecord(context.Background(), &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     testSID,
	})
	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.Len(t, resp.Records, 1)
}

// mockDBForQuery creates a MockDB that returns stream lookup results
// and query results for GET operations.
func mockDBForQuery(streamRef int64, streamType string, queryRows [][]any) *utils.MockDB {
	return &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			// Stream lookup queries
			if strings.Contains(stmt, "SELECT id, stream_type") {
				if streamRef == 0 {
					return &kwilsql.ResultSet{Rows: [][]any{}}, nil
				}
				return &kwilsql.ResultSet{
					Columns: []string{"id", "stream_type"},
					Rows:    [][]any{{streamRef, streamType}},
				}, nil
			}
			// Query results
			if queryRows == nil {
				return &kwilsql.ResultSet{Rows: [][]any{}}, nil
			}
			return &kwilsql.ResultSet{
				Columns: []string{"event_time", "value"},
				Rows:    queryRows,
			}, nil
		},
	}
}
