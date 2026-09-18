package tn_settlement

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/trufnetwork/kwil-db/core/types"
)

// fakeNode is a JSON-RPC endpoint that behaves like a node whose transaction is
// still sitting in the mempool: user.broadcast returns a hash, and user.tx_query
// reports Height -1 with no result, exactly as node.TxQuery does for a tx that
// has not been included in a block yet.
type fakeNode struct {
	broadcasts atomic.Int32
	txQueries  atomic.Int32
	// committed makes tx_query return an execution result, as it does once the
	// transaction has been included in a block.
	committed bool
}

func (f *fakeNode) handler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		var result any
		switch req.Method {
		case "user.broadcast":
			f.broadcasts.Add(1)
			result = map[string]any{"tx_hash": types.Hash{0x01}}
		case "user.tx_query":
			f.txQueries.Add(1)
			if f.committed {
				result = map[string]any{
					"tx_hash":   types.Hash{0x01},
					"height":    int64(42),
					"tx_result": &types.TxResult{Code: uint32(types.CodeOk), Log: "ok"},
				}
			} else {
				// Mempool-resident: a height of -1 and no result at all.
				result = map[string]any{"tx_hash": types.Hash{0x01}, "height": int64(-1)}
			}
		default:
			t.Errorf("unexpected JSON-RPC method %q", req.Method)
		}

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  result,
		}))
	}
}

func newTestBroadcaster(t *testing.T, node *fakeNode) TxBroadcaster {
	t.Helper()
	srv := httptest.NewServer(node.handler(t))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return makeBroadcasterFromURL(u)
}

// T1. An accept-only broadcast must return as soon as the mempool takes the
// transaction. Asking the node for an execution result here cannot succeed —
// the transaction is in the mempool, so TxQuery has none to give — and waiting
// for one is what serialises a ladder's captures one per block.
func TestBroadcastAcceptReturnsWithoutQueryingTheResult(t *testing.T) {
	node := &fakeNode{}
	b := newTestBroadcaster(t, node)

	start := time.Now()
	hash, res, err := b.BroadcastTx(context.Background(), &types.Transaction{}, 0)
	elapsed := time.Since(start)

	require.NoError(t, err, "accept-only broadcast must not fail for an uncommitted tx")
	require.Nil(t, res, "accept-only reports mempool acceptance, so there is no result")
	require.Equal(t, types.Hash{0x01}, hash)
	require.Zero(t, node.txQueries.Load(), "accept-only must not query the transaction result")
	require.Equal(t, int32(1), node.broadcasts.Load())
	require.Less(t, elapsed, time.Second, "accept-only must not poll")
}

// T2. Commit mode is unchanged: it still fetches the execution result, which is
// what isPermanentSettleError classifies settle_market quarantine decisions from.
func TestBroadcastCommitStillReturnsTheResult(t *testing.T) {
	node := &fakeNode{committed: true}
	b := newTestBroadcaster(t, node)

	hash, res, err := b.BroadcastTx(context.Background(), &types.Transaction{}, 1)

	require.NoError(t, err)
	require.NotNil(t, res, "commit mode must return the execution result")
	require.Equal(t, uint32(types.CodeOk), res.Code)
	require.Equal(t, types.Hash{0x01}, hash)
	require.Equal(t, int32(1), node.txQueries.Load())
}
