package tn_digest

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"

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
// - Rewrites wildcard hosts (0.0.0.0/::) to loopback 127.0.0.1
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
	if err == nil {
		if host == "0.0.0.0" || host == "::" || host == "[::]" {
			host = "127.0.0.1"
			u.Host = net.JoinHostPort(host, port)
		}
	}
	return u, nil
}

// makeBroadcasterFromURL creates a TxBroadcaster backed by the user JSON-RPC client.
func makeBroadcasterFromURL(u *url.URL) TxBroadcaster {
	userClient := rpcuser.NewClient(u)
	return txBroadcasterFunc(func(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error) {
		// Accept to mempool; do not wait for commit here.
		h, err := userClient.Broadcast(ctx, tx, rpcclient.BroadcastWaitAccept)
		if err != nil {
			return types.Hash{}, nil, err
		}
		return h, nil, nil
	})
}
