package setup

import (
	"context"
	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
)

func initializeContract(ctx context.Context, platform *kwilTesting.Platform, dbid string) error {
	_, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "init",
		Dataset:   dbid,
		Args:      []any{},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 1,
		},
	})
	return err
}
