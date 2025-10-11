package tn_attestation

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/trufnetwork/kwil-db/common"
	rpcclient "github.com/trufnetwork/kwil-db/core/rpc/client"
	userjsonrpc "github.com/trufnetwork/kwil-db/core/rpc/client/user/jsonrpc"
	"github.com/trufnetwork/kwil-db/core/types"
)

// ensureBroadcaster initializes the RPC client for transaction submission if not already set.
// Called during startup and leader acquisition to ensure the leader can broadcast sign_attestation
// transactions. Prefers extension-specific rpc_url config, falling back to node's RPC endpoint.
func (e *signerExtension) ensureBroadcaster(service *common.Service) {
	if service == nil || service.LocalConfig == nil {
		return
	}
	if e.Broadcaster() != nil && e.TxQueryClient() != nil {
		return
	}

	endpoint := ""
	if cfg, ok := service.LocalConfig.Extensions[ExtensionName]; ok {
		if v := strings.TrimSpace(cfg["rpc_url"]); v != "" {
			endpoint = v
		}
	}

	if endpoint == "" && service.LocalConfig.RPC.ListenAddress != "" {
		endpoint = service.LocalConfig.RPC.ListenAddress
	}
	if endpoint == "" {
		e.Logger().Warn("tn_attestation: cannot build broadcaster (no rpc endpoint configured)")
		return
	}

	u, err := normalizeListenAddressForClient(endpoint)
	if err != nil {
		e.Logger().Warn("tn_attestation: invalid rpc endpoint", "endpoint", endpoint, "error", err)
		return
	}

	broadcaster, queryClient := makeBroadcasterFromURL(u)
	e.setBroadcaster(broadcaster)
	e.setTxQueryClient(queryClient)
	e.startStatusWorker()
}

// normalizeListenAddressForClient converts a server bind address (e.g., "0.0.0.0:8080")
// to a client-usable localhost URL. Needed because the extension runs on the same node
// but cannot connect to wildcard addresses like 0.0.0.0 or [::].
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
		clean := strings.Trim(host, "[]")
		if clean == "" {
			u.Host = net.JoinHostPort("127.0.0.1", port)
		} else if ip := net.ParseIP(clean); ip != nil && ip.IsUnspecified() {
			u.Host = net.JoinHostPort("127.0.0.1", port)
		}
	}
	return u, nil
}

// makeBroadcasterFromURL creates broadcaster and query client from the normalized RPC endpoint.
// Returns the same instance for both interfaces to share a single connection pool.
func makeBroadcasterFromURL(u *url.URL) (TxBroadcaster, TxQueryClient) {
	client := userjsonrpc.NewClient(u)
	br := &jsonRPCBroadcaster{client: client}
	return br, br
}

type jsonRPCBroadcaster struct {
	client *userjsonrpc.Client
}

func (b *jsonRPCBroadcaster) BroadcastTx(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error) {
	mode := rpcclient.BroadcastWaitAccept
	if sync == uint8(rpcclient.BroadcastWaitCommit) || sync == 1 {
		mode = rpcclient.BroadcastWaitCommit
	}

	hash, err := b.client.Broadcast(ctx, tx, mode)
	if err != nil {
		return types.Hash{}, nil, err
	}

	if mode == rpcclient.BroadcastWaitAccept {
		return hash, nil, nil
	}

	resp, err := b.client.TxQuery(ctx, hash)
	if err != nil {
		return hash, nil, fmt.Errorf("tx query failed: %w", err)
	}
	if resp == nil || resp.Result == nil {
		return hash, nil, fmt.Errorf("transaction result missing")
	}

	return hash, resp.Result, nil
}

func (b *jsonRPCBroadcaster) TxQuery(ctx context.Context, txHash types.Hash) (*types.TxQueryResponse, error) {
	return b.client.TxQuery(ctx, txHash)
}

// TxBroadcaster matches the subset of the JSON-RPC client used by the signing
// worker to inject transactions.
type TxBroadcaster interface {
	BroadcastTx(ctx context.Context, tx *types.Transaction, sync uint8) (types.Hash, *types.TxResult, error)
}

// TxQueryClient interface for querying transaction status.
type TxQueryClient interface {
	TxQuery(ctx context.Context, txHash types.Hash) (*types.TxQueryResponse, error)
}
