package setup

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/trufnetwork/sdk-go/core/util"
)

type ContractType string

const (
	ContractTypePrimitive ContractType = "primitive"
	ContractTypeComposed  ContractType = "composed"
)

type ContractInfo struct {
	Name     string
	StreamID util.StreamId
	Deployer util.EthereumAddress
	Content  []byte
	Type     ContractType
}

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

// SetupAndInitializeContract sets up and initializes a contract for testing.
func SetupAndInitializeContract(ctx context.Context, platform *kwilTesting.Platform, contractInfo ContractInfo) error {
	if err := setupContract(ctx, platform, contractInfo); err != nil {
		return err
	}
	dbid := GetDBID(contractInfo)
	return initializeContract(ctx, InitializeContractInput{
		Platform: platform,
		Deployer: contractInfo.Deployer,
		Dbid:     dbid,
		Height:   0,
	})
}

// setupContract parses and creates the dataset for a contract
func setupContract(ctx context.Context, platform *kwilTesting.Platform, contractInfo ContractInfo) error {
	schema, err := parse.Parse(contractInfo.Content)
	if err != nil {
		return errors.Wrapf(err, "Failed to parse contract %s", contractInfo.Name)
	}
	schema.Name = contractInfo.StreamID.String()

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       contractInfo.Deployer.Bytes(),
		Caller:       contractInfo.Deployer.Address(),
		TxID:         platform.Txid(),
	}

	return platform.Engine.CreateDataset(txContext, platform.DB, schema)
}

// GetDBID generates a DBID from contract info
func GetDBID(contractInfo ContractInfo) string {
	return utils.GenerateDBID(contractInfo.StreamID.String(), contractInfo.Deployer.Bytes())
}
