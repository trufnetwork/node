package tn_attestation

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/trufnetwork/kwil-db/common"
	rpcclient "github.com/trufnetwork/kwil-db/core/rpc/client"
	userjsonrpc "github.com/trufnetwork/kwil-db/core/rpc/client/user/jsonrpc"
	"github.com/trufnetwork/kwil-db/core/types"
)

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
}

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

	var resp *types.TxQueryResponse
	var queryErr error
	if mode == rpcclient.BroadcastWaitAccept {
		for tries := 0; tries < 10; tries++ {
			resp, queryErr = b.client.TxQuery(ctx, hash)
			if queryErr == nil && resp != nil && resp.Result != nil {
				break
			}
			select {
			case <-ctx.Done():
				return types.Hash{}, nil, ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
		if queryErr != nil {
			return types.Hash{}, nil, fmt.Errorf("tx query failed: %w", queryErr)
		}
	} else {
		resp, queryErr = b.client.TxQuery(ctx, hash)
		if queryErr != nil {
			return types.Hash{}, nil, fmt.Errorf("tx query failed: %w", queryErr)
		}
	}

	if resp == nil || resp.Result == nil {
		return types.Hash{}, nil, fmt.Errorf("transaction result missing")
	}

	return hash, resp.Result, nil
}

func (b *jsonRPCBroadcaster) TxQuery(ctx context.Context, txHash types.Hash) (*types.TxQueryResponse, error) {
	return b.client.TxQuery(ctx, txHash)
}
