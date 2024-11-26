package setup

import (
	"context"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/trufnetwork/sdk-go/core/util"
)

type InitializeContractInput struct {
	Platform *kwilTesting.Platform
	Deployer util.EthereumAddress
	Dbid     string
	Height   int64
}

func initializeContract(ctx context.Context, input InitializeContractInput) error {
	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: input.Height,
		},
		TxID:   input.Platform.Txid(),
		Signer: input.Deployer.Bytes(),
		Caller: input.Deployer.Address(),
	}

	_, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
		Procedure: "init",
		Dataset:   input.Dbid,
		Args:      []any{},
	})
	return err
}
