package client

import (
	"context"
	"fmt"
	accountspb "kwil/api/protobuf/accounts/v0"
	cfgpb "kwil/api/protobuf/config/v0"
	pricingpb "kwil/api/protobuf/pricing/v0"
	txpb "kwil/api/protobuf/tx/v0"
	"kwil/internal/pkg/transport"
	"kwil/pkg/log"

	"google.golang.org/grpc"
)

type Config struct {
	Addr string `mapstructure:"addr"`
}

type Client struct {
	accountClt accountspb.AccountServiceClient
	txClt      txpb.TxServiceClient
	pricingClt pricingpb.PricingServiceClient
	cfgClt     cfgpb.ConfigServiceClient

	conn *grpc.ClientConn
	cfg  *Config
}

// @yaiba TODO: manually declare dependencies
func NewClient(ctx context.Context, cfg *Config, log log.Logger, conn grpc.ClientConnInterface,
	accountClt accountspb.AccountServiceClient, txClt txpb.TxServiceClient,
	pricingClt pricingpb.PricingServiceClient, cfgClt cfgpb.ConfigServiceClient) GrpcClient {
	return &Client{
		accountClt: accountClt,
		txClt:      txClt,
		pricingClt: pricingClt,
		cfgClt:     cfgClt,
		conn:       conn.(*grpc.ClientConn),
		cfg:        cfg,
	}
}

func New(ctx context.Context, cfg *Config) (*Client, error) {
	conn, err := transport.Dial(ctx, cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server %s: %w", cfg.Addr, err)
	}
	return &Client{
		accountClt: accountspb.NewAccountServiceClient(conn),
		txClt:      txpb.NewTxServiceClient(conn),
		pricingClt: pricingpb.NewPricingServiceClient(conn),
		cfgClt:     cfgpb.NewConfigServiceClient(conn),
		cfg:        cfg,
		conn:       conn,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}