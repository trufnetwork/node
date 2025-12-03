package tn_settlement

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	rpcclient "github.com/trufnetwork/kwil-db/core/rpc/client"
	rpcuser "github.com/trufnetwork/kwil-db/core/rpc/client/user/jsonrpc"
	"github.com/trufnetwork/kwil-db/core/types"
)

// txBroadcasterFunc adapts a function to the TxBroadcaster interface
type txBroadcasterFunc func(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error)

func (f txBroadcasterFunc) BroadcastTx(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error) {
	return f(ctx, tx, sync)
}

// normalizeListenAddressForClient converts a server listen address into a client URL.
// - Adds http:// scheme if missing
// - Rewrites wildcard or empty hosts (0.0.0.0/::/[::]/"") to loopback 127.0.0.1
func normalizeListenAddressForClient(listen string) (*url.URL, error) {
	if listen == "" {
		return nil, fmt.Errorf("empty listen address")
	}
	endpoint := listen
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		// No port present, check the host directly
		cleanHost := strings.Trim(u.Host, "[]")
		if cleanHost == "" {
			u.Host = "127.0.0.1"
		} else if ip := net.ParseIP(cleanHost); ip != nil && ip.IsUnspecified() {
			u.Host = "127.0.0.1"
		}
	} else {
		cleanHost := strings.Trim(host, "[]")
		if cleanHost == "" {
			u.Host = net.JoinHostPort("127.0.0.1", port)
		} else if ip := net.ParseIP(cleanHost); ip != nil && ip.IsUnspecified() {
			u.Host = net.JoinHostPort("127.0.0.1", port)
		}
	}
	return u, nil
}

// makeBroadcasterFromURL creates a TxBroadcaster backed by the user JSON-RPC client
func makeBroadcasterFromURL(u *url.URL) TxBroadcaster {
	userClient := rpcuser.NewClient(u)
	return txBroadcasterFunc(func(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error) {
		// Map sync flag to broadcast mode (callers should pass 1 for WaitCommit)
		mode := rpcclient.BroadcastWaitAccept
		if sync == uint8(rpcclient.BroadcastWaitCommit) || sync == 1 {
			mode = rpcclient.BroadcastWaitCommit
		}
		h, err := userClient.Broadcast(ctx, tx, mode)
		if err != nil {
			return types.Hash{}, nil, err
		}

		// Query the transaction result to get the log output for parsing
		var txQueryResp *types.TxQueryResponse
		var queryErr error
		if mode == rpcclient.BroadcastWaitAccept {
			// In Accept mode, commit may not be immediate: perform short polling
			for tries := 0; tries < 10; tries++ {
				txQueryResp, queryErr = userClient.TxQuery(ctx, h)
				if queryErr == nil && txQueryResp != nil && txQueryResp.Result != nil {
					break
				}
				// brief backoff (non-blocking if ctx is canceled)
				select {
				case <-ctx.Done():
					return types.Hash{}, nil, ctx.Err()
				case <-time.After(200 * time.Millisecond):
				}
			}
			if queryErr != nil {
				return types.Hash{}, nil, fmt.Errorf("failed to query transaction result: %w", queryErr)
			}
		} else {
			txQueryResp, queryErr = userClient.TxQuery(ctx, h)
			if queryErr != nil {
				return types.Hash{}, nil, fmt.Errorf("failed to query transaction result: %w", queryErr)
			}
		}

		if txQueryResp == nil || txQueryResp.Result == nil {
			return types.Hash{}, nil, fmt.Errorf("transaction result is nil")
		}

		return h, txQueryResp.Result, nil
	})
}
