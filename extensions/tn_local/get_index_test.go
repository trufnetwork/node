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

func TestGetIndex_NilRequest(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	resp, rpcErr := ext.GetIndex(context.Background(), nil)
	require.Nil(t, resp)
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
}

func TestGetIndex_StreamNotFound(t *testing.T) {
	mockDB := mockDBForQuery(0, "", nil)
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.GetIndex(context.Background(), &GetIndexRequest{
		DataProvider: testDP,
		StreamID:     testSID,
	})
	require.Nil(t, resp)
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "stream not found")
}

func TestGetIndex_Primitive_DefaultBaseTime(t *testing.T) {
	// The handler calls:
	// 1. dbLookupStreamRef → SELECT id, stream_type
	// 2. dbGetFirstEventTime → SELECT pe.event_time ... ORDER BY pe.event_time ASC
	// 3. dbGetRecordPrimitive(from=nil, to=baseTime) → latest record query (ORDER BY DESC)
	// 4. dbGetRecordPrimitive(from=1000, to=5000) → range query with CTE
	//
	// We track call order to return different results for each step.
	queryCallNum := 0
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			// Stream lookup (called multiple times, always returns same)
			if strings.Contains(stmt, "SELECT id, stream_type") {
				return &kwilsql.ResultSet{
					Columns: []string{"id", "stream_type"},
					Rows:    [][]any{{int64(1), "primitive"}},
				}, nil
			}

			queryCallNum++
			switch queryCallNum {
			case 1:
				// dbGetFirstEventTime
				return &kwilsql.ResultSet{
					Columns: []string{"event_time"},
					Rows:    [][]any{{int64(1000)}},
				}, nil
			case 2:
				// dbGetRecordPrimitive(from=nil, to=baseTime) → get_last_record
				return &kwilsql.ResultSet{
					Columns: []string{"event_time", "value"},
					Rows:    [][]any{{int64(1000), "10.000000000000000000"}},
				}, nil
			default:
				// dbGetRecordPrimitive(from=1000, to=5000) → range query
				return &kwilsql.ResultSet{
					Columns: []string{"event_time", "value"},
					Rows: [][]any{
						{int64(1000), "10.000000000000000000"},
						{int64(2000), "20.000000000000000000"},
						{int64(3000), "30.000000000000000000"},
					},
				}, nil
			}
		},
	}
	ext := newTestExtension(mockDB)

	from := int64(1000)
	to := int64(5000)
	resp, rpcErr := ext.GetIndex(context.Background(), &GetIndexRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
	})
	require.Nil(t, rpcErr, "expected no error, got: %v", rpcErr)
	require.NotNil(t, resp)
	require.Len(t, resp.Records, 3)

	// Index = (value / base) * 100
	// base = 10.0 (first event)
	// 10/10*100 = 100, 20/10*100 = 200, 30/10*100 = 300
	require.Equal(t, "100.000000000000000000", resp.Records[0].Value)
	require.Equal(t, "200.000000000000000000", resp.Records[1].Value)
	require.Equal(t, "300.000000000000000000", resp.Records[2].Value)
}

func TestGetIndex_ExplicitBaseTime(t *testing.T) {
	queryCallNum := 0
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			if strings.Contains(stmt, "SELECT id, stream_type") {
				return &kwilsql.ResultSet{
					Columns: []string{"id", "stream_type"},
					Rows:    [][]any{{int64(1), "primitive"}},
				}, nil
			}
			queryCallNum++
			switch queryCallNum {
			case 1:
				// Base value query (get_last_record: from=nil, to=baseTime)
				return &kwilsql.ResultSet{
					Columns: []string{"event_time", "value"},
					Rows:    [][]any{{int64(2000), "50.000000000000000000"}},
				}, nil
			default:
				// Range query
				return &kwilsql.ResultSet{
					Columns: []string{"event_time", "value"},
					Rows: [][]any{
						{int64(2000), "50.000000000000000000"},
						{int64(3000), "75.000000000000000000"},
						{int64(4000), "100.000000000000000000"},
					},
				}, nil
			}
		},
	}
	ext := newTestExtension(mockDB)

	from := int64(2000)
	to := int64(5000)
	baseTime := int64(2000)
	resp, rpcErr := ext.GetIndex(context.Background(), &GetIndexRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
		BaseTime:     &baseTime,
	})
	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.Len(t, resp.Records, 3)

	// base = 50, index = (val/50)*100
	require.Equal(t, "100.000000000000000000", resp.Records[0].Value)
	require.Equal(t, "150.000000000000000000", resp.Records[1].Value)
	require.Equal(t, "200.000000000000000000", resp.Records[2].Value)
}

func TestGetIndex_BaseValueZero(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			if strings.Contains(stmt, "SELECT id, stream_type") {
				return &kwilsql.ResultSet{
					Columns: []string{"id", "stream_type"},
					Rows:    [][]any{{int64(1), "primitive"}},
				}, nil
			}
			if strings.Contains(stmt, "ORDER BY pe.event_time ASC") {
				return &kwilsql.ResultSet{
					Columns: []string{"event_time"},
					Rows:    [][]any{{int64(1000)}},
				}, nil
			}
			// Return base value of 0
			return &kwilsql.ResultSet{
				Columns: []string{"event_time", "value"},
				Rows:    [][]any{{int64(1000), "0.000000000000000000"}},
			}, nil
		},
	}
	ext := newTestExtension(mockDB)

	from := int64(1000)
	to := int64(5000)
	_, rpcErr := ext.GetIndex(context.Background(), &GetIndexRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "base value is 0")
}

func TestGetIndex_EmptyStream(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			if strings.Contains(stmt, "SELECT id, stream_type") {
				return &kwilsql.ResultSet{
					Columns: []string{"id", "stream_type"},
					Rows:    [][]any{{int64(1), "primitive"}},
				}, nil
			}
			// No first event time
			return &kwilsql.ResultSet{Rows: [][]any{}}, nil
		},
	}
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.GetIndex(context.Background(), &GetIndexRequest{
		DataProvider: testDP,
		StreamID:     testSID,
	})
	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.Empty(t, resp.Records)
}
