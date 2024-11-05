package init_system_contract

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/kwilteam/kwil-db/core/gatewayclient"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"time"

	"github.com/kwilteam/kwil-db/core/crypto"
	"github.com/kwilteam/kwil-db/core/crypto/auth"
	clientType "github.com/kwilteam/kwil-db/core/types/client"
	"github.com/kwilteam/kwil-db/parse"
)

type InitSystemContractOptions struct {
	// PrivateKey is the private key of the account that will deploy the system contract. i.e., the TSN wallet
	PrivateKey string
	// ProviderUrl we're using the gateway client to interact with the TSN, so it should be the gateway URL
	ProviderUrl           string
	SystemContractContent string
	// RetryTimeout is the maximum time to wait for the TSN to start
	RetryTimeout time.Duration
}

func InitSystemContract(ctx context.Context, options InitSystemContractOptions) error {
	// use ctx to cancel long running operations

	zap.L().Info("Initializing system contract...")
	zap.L().Info("System contract content", zap.String("content", options.SystemContractContent))

	pk, err := crypto.Secp256k1PrivateKeyFromHex(options.PrivateKey)
	if err != nil {
		return errors.Wrap(err, "failed to parse private key")
	}

	signer := &auth.EthPersonalSigner{Key: *pk}

	var kwilClient clientType.Client

	// Make sure the TSN is running. We expect to receive pong. On this step, we retry for the max timeout
	err = backoff.RetryNotify(func() error {
		kwilClient, err = gatewayclient.NewClient(ctx, options.ProviderUrl, &gatewayclient.GatewayOptions{
			Options: clientType.Options{
				Signer: signer,
			},
		})
		if err != nil {
			return errors.Wrap(err, "failed to create kwil client")
		}

		zap.L().Info("Pinging the network...")
		res, err := kwilClient.Ping(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to ping the network")
		}

		if res != "pong" {
			return errors.New(fmt.Sprintf("expected pong, received: %s", res))
		}

		return nil
	}, backoff.NewExponentialBackOff(
		backoff.WithMaxInterval(15*time.Second),
		backoff.WithMaxElapsedTime(options.RetryTimeout),
	), func(err error, duration time.Duration) {
		zap.L().Warn("Error while waiting for TSN to start", zap.Error(err), zap.String("retry_in", duration.String()))
	})

	if err != nil {
		return errors.Wrap(err, "timed out while waiting for TSN to start")
	}

	schema, err := parse.Parse([]byte(options.SystemContractContent))
	if err != nil {
		return errors.Wrap(err, "failed to parse system contract")
	}

	zap.L().Info("Deploying system contract...")
	// Deploy the system contract
	txHash, err := kwilClient.DeployDatabase(ctx, schema, clientType.WithSyncBroadcast(true))
	if err != nil {
		return errors.Wrap(err, "failed to deploy system contract")
	}

	zap.L().Info("System contract deployed", zap.String("tx_hash", txHash.Hex()))

	return nil
}
