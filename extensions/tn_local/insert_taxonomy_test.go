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

const testComposedSID = "st0000000000000000000000composed"
const testChildSID1 = "st000000000000000000000000child1"
const testChildSID2 = "st000000000000000000000000child2"

// mockDBForTaxonomy returns a MockDB that simulates stream lookups for taxonomy tests.
// parentSID is the parent stream ID (used to distinguish parent from child lookups).
// parentRef/parentType is returned for the parent stream lookup.
// childRefs maps "dp/sid" -> ref for child lookups.
// executeFn captures INSERT/SELECT statements in transactions.
func mockDBForTaxonomy(
	parentRef int64, parentType string,
	childRefs map[string]int64,
	executeFn func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error),
) *utils.MockDB {
	lookupFn := func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		if strings.Contains(stmt, "SELECT") && len(args) >= 2 {
			dp, _ := args[0].(string)
			sid, _ := args[1].(string)
			key := dp + "/" + sid

			// First check the childRefs map
			if ref, ok := childRefs[key]; ok {
				return &kwilsql.ResultSet{
					Columns: []string{"id", "stream_type"},
					Rows:    [][]any{{ref, "primitive"}},
				}, nil
			}

			// Check if this matches the parent stream (by stream_id)
			if parentRef != 0 && sid == testComposedSID {
				return &kwilsql.ResultSet{
					Columns: []string{"id", "stream_type"},
					Rows:    [][]any{{parentRef, parentType}},
				}, nil
			}

			// Not found
			return &kwilsql.ResultSet{Rows: [][]any{}}, nil
		}
		if executeFn != nil {
			return executeFn(ctx, stmt, args...)
		}
		return &kwilsql.ResultSet{}, nil
	}

	return &utils.MockDB{
		ExecuteFn: lookupFn,
		BeginTxFn: func(ctx context.Context) (kwilsql.Tx, error) {
			return &utils.MockTx{
				ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
					// group_sequence query
					if strings.Contains(stmt, "MAX(group_sequence)") {
						return &kwilsql.ResultSet{
							Columns: []string{"next_seq"},
							Rows:    [][]any{{int64(1)}},
						}, nil
					}
					if executeFn != nil {
						return executeFn(ctx, stmt, args...)
					}
					return &kwilsql.ResultSet{}, nil
				},
			}, nil
		},
	}
}

func TestInsertTaxonomy_NilRequest(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	resp, rpcErr := ext.InsertTaxonomy(context.Background(), nil)
	require.Nil(t, resp)
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "missing request")
}

func TestInsertTaxonomy_EmptyChildren(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{},
		ChildStreamIDs:     []string{},
		Weights:            []string{},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "at least 1 child")
}

func TestInsertTaxonomy_ArrayLengthMismatch(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP, testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "child array lengths mismatch")
}

func TestInsertTaxonomy_Success(t *testing.T) {
	lowDP := strings.ToLower(testDP)
	childRefs := map[string]int64{
		lowDP + "/" + testChildSID1: 10,
		lowDP + "/" + testChildSID2: 11,
	}

	var capturedStmts []string
	var capturedArgs [][]any
	mockDB := mockDBForTaxonomy(100, "composed", childRefs, func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		capturedStmts = append(capturedStmts, stmt)
		capturedArgs = append(capturedArgs, args)
		return &kwilsql.ResultSet{}, nil
	})
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP, testDP},
		ChildStreamIDs:     []string{testChildSID1, testChildSID2},
		Weights:            []string{"0.6", "0.4"},
		StartDate:          1000,
	})

	require.Nil(t, rpcErr, "expected no error")
	require.NotNil(t, resp)

	// Two INSERT statements (one per child)
	insertCount := 0
	for _, stmt := range capturedStmts {
		if strings.Contains(stmt, "INSERT INTO "+SchemaName+".taxonomies") {
			insertCount++
		}
	}
	require.Equal(t, 2, insertCount, "should have 2 taxonomy INSERT statements")
}

func TestInsertTaxonomy_SingleChild(t *testing.T) {
	lowDP := strings.ToLower(testDP)
	childRefs := map[string]int64{
		lowDP + "/" + testChildSID1: 10,
	}

	var insertCalled bool
	mockDB := mockDBForTaxonomy(100, "composed", childRefs, func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		if strings.Contains(stmt, "INSERT") {
			insertCalled = true
			// Verify args: taxonomy_id, parent_ref, child_ref, weight, start_date, group_seq, created_at
			require.Len(t, args, 7)
			require.Equal(t, int64(100), args[1]) // parent_ref
			require.Equal(t, int64(10), args[2])  // child_ref
			require.Equal(t, "1.0", args[3])      // weight
			require.Equal(t, int64(500), args[4])  // start_date
		}
		return &kwilsql.ResultSet{}, nil
	})
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          500,
	})

	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.True(t, insertCalled, "INSERT should have been called")
}

func TestInsertTaxonomy_ParentNotFound(t *testing.T) {
	mockDB := mockDBForTaxonomy(0, "", nil, nil)
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "parent stream not found")
}

func TestInsertTaxonomy_ParentNotComposed(t *testing.T) {
	mockDB := mockDBForTaxonomy(100, "primitive", nil, nil)
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "is not a composed stream")
}

func TestInsertTaxonomy_ChildNotFound(t *testing.T) {
	// Parent exists, but child doesn't exist in local storage
	mockDB := mockDBForTaxonomy(100, "composed", map[string]int64{}, nil)
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "child stream not found in local storage")
}

func TestInsertTaxonomy_InvalidParentDataProvider(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       "invalid",
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "data_provider must be a valid Ethereum address")
}

func TestInsertTaxonomy_InvalidParentStreamID(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           "bad",
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "stream_id must be exactly 32 characters")
}

func TestInsertTaxonomy_InvalidChildDataProvider(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{"bad_address"},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "child 0: data_provider must be a valid Ethereum address")
}

func TestInsertTaxonomy_InvalidChildStreamID(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{"short"},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "child 0: stream_id must be exactly 32 characters")
}

func TestInsertTaxonomy_InvalidWeight(t *testing.T) {
	tests := []struct {
		name    string
		weight  string
		wantMsg string
	}{
		{"non-numeric", "abc", "weight must be a valid decimal number"},
		{"negative", "-1.0", "weight must be non-negative"},
		{"NaN", "NaN", "weight must be a valid decimal number"},
		{"Inf", "Inf", "weight must be a valid decimal number"},
		{"too many decimal places", "1.1234567890123456789", "more than 18 decimal places"},
		{"too many integral digits", "1234567890123456789.0", "more than 18 integral digits"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext := newTestExtension(&utils.MockDB{})
			_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
				DataProvider:       testDP,
				StreamID:           testComposedSID,
				ChildDataProviders: []string{testDP},
				ChildStreamIDs:     []string{testChildSID1},
				Weights:            []string{tt.weight},
				StartDate:          0,
			})
			require.NotNil(t, rpcErr)
			require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
			require.Contains(t, rpcErr.Message, tt.wantMsg)
		})
	}
}

func TestInsertTaxonomy_DuplicateChild(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP, testDP},
		ChildStreamIDs:     []string{testChildSID1, testChildSID1},
		Weights:            []string{"0.5", "0.5"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "child 1: duplicate child_data_provider/child_stream_id tuple")
}

func TestInsertTaxonomy_DuplicateChildMixedCase(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	// Same address, different casing — should still detect as duplicate after normalization
	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{"0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832", "0xec36224a679218ae28fcece8d3c68595b87dd832"},
		ChildStreamIDs:     []string{testChildSID1, testChildSID1},
		Weights:            []string{"0.5", "0.5"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "child 1: duplicate child_data_provider/child_stream_id tuple")
}

func TestInsertTaxonomy_DBInsertError(t *testing.T) {
	lowDP := strings.ToLower(testDP)
	childRefs := map[string]int64{
		lowDP + "/" + testChildSID1: 10,
	}

	mockDB := mockDBForTaxonomy(100, "composed", childRefs, func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		if strings.Contains(stmt, "INSERT") {
			return nil, fmt.Errorf("disk full")
		}
		return &kwilsql.ResultSet{}, nil
	})
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInternal), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "failed to insert taxonomy")
}

func TestInsertTaxonomy_LowercaseNormalization(t *testing.T) {
	mixedCaseDP := "0xEC36224A679218Ae28FCeCe8d3c68595B87Dd832"
	lowDP := strings.ToLower(mixedCaseDP)
	childRefs := map[string]int64{
		lowDP + "/" + testChildSID1: 10,
	}

	var capturedParentDP string
	var capturedChildDP string

	// Use a custom mock that captures the lowercased lookup args
	lookupCalls := 0
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			if strings.Contains(stmt, "SELECT") && len(args) >= 2 {
				lookupCalls++
				dp := args[0].(string)
				sid := args[1].(string)
				if lookupCalls == 1 {
					capturedParentDP = dp
					return &kwilsql.ResultSet{
						Columns: []string{"id", "stream_type"},
						Rows:    [][]any{{int64(100), "composed"}},
					}, nil
				}
				capturedChildDP = dp
				key := dp + "/" + sid
				if ref, ok := childRefs[key]; ok {
					return &kwilsql.ResultSet{
						Columns: []string{"id", "stream_type"},
						Rows:    [][]any{{ref, "primitive"}},
					}, nil
				}
				return &kwilsql.ResultSet{Rows: [][]any{}}, nil
			}
			return &kwilsql.ResultSet{}, nil
		},
		BeginTxFn: func(ctx context.Context) (kwilsql.Tx, error) {
			return &utils.MockTx{
				ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
					if strings.Contains(stmt, "MAX(group_sequence)") {
						return &kwilsql.ResultSet{
							Columns: []string{"next_seq"},
							Rows:    [][]any{{int64(1)}},
						}, nil
					}
					return &kwilsql.ResultSet{}, nil
				},
			}, nil
		},
	}
	ext := newTestExtension(mockDB)

	resp, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       mixedCaseDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{mixedCaseDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})

	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
	require.Equal(t, lowDP, capturedParentDP, "parent data_provider should be lowercased")
	require.Equal(t, lowDP, capturedChildDP, "child data_provider should be lowercased")
}

func TestInsertTaxonomy_ParentLookupDBError(t *testing.T) {
	mockDB := &utils.MockDB{
		ExecuteFn: func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	ext := newTestExtension(mockDB)

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInternal), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "failed to look up parent stream")
}

func TestInsertTaxonomy_NegativeStartDate(t *testing.T) {
	ext := newTestExtension(&utils.MockDB{})

	_, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"1.0"},
		StartDate:          -1,
	})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(jsonrpc.ErrorInvalidParams), rpcErr.Code)
	require.Contains(t, rpcErr.Message, "start_date must be >= 0")
}

func TestInsertTaxonomy_ZeroWeight(t *testing.T) {
	lowDP := strings.ToLower(testDP)
	childRefs := map[string]int64{
		lowDP + "/" + testChildSID1: 10,
	}

	mockDB := mockDBForTaxonomy(100, "composed", childRefs, func(ctx context.Context, stmt string, args ...any) (*kwilsql.ResultSet, error) {
		return &kwilsql.ResultSet{}, nil
	})
	ext := newTestExtension(mockDB)

	// Zero weight should be allowed (non-negative)
	resp, rpcErr := ext.InsertTaxonomy(context.Background(), &InsertTaxonomyRequest{
		DataProvider:       testDP,
		StreamID:           testComposedSID,
		ChildDataProviders: []string{testDP},
		ChildStreamIDs:     []string{testChildSID1},
		Weights:            []string{"0"},
		StartDate:          0,
	})
	require.Nil(t, rpcErr)
	require.NotNil(t, resp)
}
