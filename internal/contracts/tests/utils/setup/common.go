package setup

import (
	"context"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/truflation/tsn-sdk/core/util"
)

type InitializeContractInput struct {
	Platform *kwilTesting.Platform
	Deployer util.EthereumAddress
	Dbid     string
	Height   int64
}

func initializeContract(ctx context.Context, input InitializeContractInput) error {
	_, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "init",
		Dataset:   input.Dbid,
		Args:      []any{},
		TransactionData: common.TransactionData{
			Signer: input.Deployer.Bytes(),
			Caller: input.Deployer.Address(),
			TxID:   input.Platform.Txid(),
			Height: input.Height,
		},
	})
	return err
}
