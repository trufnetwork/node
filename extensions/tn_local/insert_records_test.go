package tn_local

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
	kwilsql "github.com/trufnetwork/kwil-db/node/types/sql"
	"github.com/trufnetwork/node/tests/utils"
)

// mockDBWithStream returns a MockDB that simulates a stream lookup returning the given
// streamRef and streamType, and captures INSERT statements via executeFn.
func mockDBWithStream(streamRef int64, streamType string, executeFn func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error)) *utils.MockDB {
	return &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			if strings.Contains(stmt, "SELECT") {
				if streamRef == 0 {
					return &kwilsql.ResultSet{Rows: [][]any{}}, nil
				}
				return &kwilsql.ResultSet{
					Columns: []string{"id", "stream_type"},
					Rows:    [][]any{{streamRef, streamType}},
				}, nil
			}
			if executeFn != nil {
				return executeFn(ctx, stmt, args...)
			}
			return &kwilsql.ResultSet{}, nil
		},
		BeginTxFn: func(ctx context.Context) (kwilsql.Tx, error) {
			return &utils.MockTx{
				ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
					if executeFn != nil {
						return executeFn(ctx, stmt, args...)
					}
					return &kwilsql.ResultSet{}, nil
				},
			}, nil
		},
	}
}

const testDP = "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832"
const testSID = "st00000000000000000000000000test"

func TestInsertRecords_NilRequest(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	resp, rpcErr := ext.InsertRecords(context.Background(), nil)
	require.Nil(t, resp)
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "missing request")
}

func TestInsertRecords_EmptyArrays(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{},
		StreamID:     []string{},
		EventTime:    []int64{},
		Value:        []string{},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "records must not be empty")
}

func TestInsertRecords_ArrayLengthMismatch(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP, testDP},
		StreamID:     []string{testSID},
		EventTime:    []int64{1000},
		Value:        []string{"1.0"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "array lengths mismatch")
}

func TestInsertRecords_Success(t *testing.T) {
	var capturedStmts []string
	var capturedArgs [][]any
	mockDB := mockDBWithStream(42, "primitive", func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		capturedStmts = append(capturedStmts, stmt)
		capturedArgs = append(capturedArgs, args)
		return &kwilsql.ResultSet{}, nil
	})
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP, testDP},
		StreamID:     []string{testSID, testSID},
		EventTime:    []int64{1000, 2000},
		Value:        []string{"123.456", "789.012"},
	})

	require.Nil(t, rpcErr, "expected no error")
	require.NotNil(t, resp)

	// Two INSERT statements (one per record)
	require.Len(t, capturedStmts, 2)
	for _, stmt := range capturedStmts {
		require.Contains(t, stmt, "INSERT INTO "+SchemaName+".primitive_events")
	}

	// Each INSERT has 4 args: stream_ref, event_time, value, created_at
	require.Len(t, capturedArgs[0], 4)
	require.Equal(t, int64(42), capturedArgs[0][0]) // stream_ref
	require.Equal(t, int64(1000), capturedArgs[0][1])
	require.Equal(t, "123.456", capturedArgs[0][2])
	// created_at = block height (0 before any blocks are processed in tests)
	createdAt, ok := capturedArgs[0][3].(int64)
	require.True(t, ok, "created_at should be int64")
	require.GreaterOrEqual(t, createdAt, int64(0))

	require.Equal(t, int64(42), capturedArgs[1][0])
	require.Equal(t, int64(2000), capturedArgs[1][1])
	require.Equal(t, "789.012", capturedArgs[1][2])
}

func TestInsertRecords_ZeroValuesFiltered(t *testing.T) {
	var capturedArgs [][]any
	mockDB := mockDBWithStream(42, "primitive", func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		capturedArgs = append(capturedArgs, args)
		return &kwilsql.ResultSet{}, nil
	})
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP, testDP, testDP},
		StreamID:     []string{testSID, testSID, testSID},
		EventTime:    []int64{1000, 2000, 3000},
		Value:        []string{"1.5", "0", "2.5"},
	})

	require.Nil(t, rpcErr)
	require.NotNil(t, resp)

	// Only 2 inserts — the "0" record is filtered like consensus
	require.Len(t, capturedArgs, 2)
	require.Equal(t, int64(1000), capturedArgs[0][1])
	require.Equal(t, "1.5", capturedArgs[0][2])
	require.Equal(t, int64(3000), capturedArgs[1][1])
	require.Equal(t, "2.5", capturedArgs[1][2])
}

func TestInsertRecords_AllZerosNoInsert(t *testing.T) {
	insertCalled := false
	mockDB := mockDBWithStream(42, "primitive", func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		insertCalled = true
		return &kwilsql.ResultSet{}, nil
	})
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP},
		StreamID:     []string{testSID},
		EventTime:    []int64{1000},
		Value:        []string{"0"},
	})

	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.False(t, insertCalled, "should not call dbInsertRecords when all values are zero")
}

func TestInsertRecords_StreamNotFound(t *testing.T) {
	mockDB := mockDBWithStream(0, "", nil)
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP},
		StreamID:     []string{testSID},
		EventTime:    []int64{1000},
		Value:        []string{"1.0"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "stream not found")
}

func TestInsertRecords_ComposedStreamRejected(t *testing.T) {
	mockDB := mockDBWithStream(42, "composed", nil)
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP},
		StreamID:     []string{testSID},
		EventTime:    []int64{1000},
		Value:        []string{"1.0"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "is not a primitive stream")
}

func TestInsertRecords_DBError(t *testing.T) {
	mockDB := mockDBWithStream(42, "primitive", func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		return nil, fmt.Errorf("connection refused")
	})
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP},
		StreamID:     []string{testSID},
		EventTime:    []int64{1000},
		Value:        []string{"1.0"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInternal), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "failed to insert records")
}

func TestInsertRecords_InvalidDataProvider(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{"invalid"},
		StreamID:     []string{testSID},
		EventTime:    []int64{1000},
		Value:        []string{"1.0"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "data_provider must be a valid Ethereum address")
}

func TestInsertRecords_InvalidStreamID(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP},
		StreamID:     []string{"bad"},
		EventTime:    []int64{1000},
		Value:        []string{"1.0"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "stream_id must be exactly 32 characters")
}

func TestInsertRecords_InvalidValue(t *testing.T) {
	mockDB := mockDBWithStream(42, "primitive", nil)
	ext := newTestExtension(mockDB)

	tests := []struct {
		name    string
		value   string
		wantMsg string
	}{
		{"non-numeric", "hello", "invalid record value at index 0"},
		{"empty", "", "invalid record value at index 0"},
		{"NaN", "NaN", "must be a finite number"},
		{"Inf", "Inf", "must be a finite number"},
		{"-Inf", "-Inf", "must be a finite number"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
				DataProvider: []string{testDP},
				StreamID:     []string{testSID},
				EventTime:    []int64{1000},
				Value:        []string{tt.value},
			})
			require.NotNil(t, rpcErr)
			require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
			require.Contains(t, rpcErr.Message, tt.wantMsg)
		})
	}
}

func TestInsertRecords_InvalidValueAtIndex(t *testing.T) {
	mockDB := mockDBWithStream(42, "primitive", nil)
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP, testDP},
		StreamID:     []string{testSID, testSID},
		EventTime:    []int64{1000, 2000},
		Value:        []string{"1.0", "bad"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "invalid record value at index 1")
}

func TestInsertRecords_LookupDBError(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertRecords(context.Background(), &InsertRecordsRequest{
		DataProvider: []string{testDP},
		StreamID:     []string{testSID},
		EventTime:    []int64{1000},
		Value:        []string{"1.0"},
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInternal), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "failed to look up stream")
}
