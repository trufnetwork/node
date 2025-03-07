package setup

import (
	"context"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/trufnetwork/sdk-go/core/types"
)

type ContractType string

const (
	ContractTypePrimitive ContractType = "primitive"
	ContractTypeComposed  ContractType = "composed"
)

type StreamInfo struct {
	Locator types.StreamLocator
	Type    ContractType
}

func (contractType ContractType) String() string {
	return string(contractType)
}

// CreateStream parses and creates the dataset for a contract
func CreateStream(ctx context.Context, platform *kwilTesting.Platform, contractInfo StreamInfo) (*common.CallResult, error) {

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       contractInfo.Locator.DataProvider.Bytes(),
		Caller:       contractInfo.Locator.DataProvider.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	return platform.Engine.Call(engineContext,
		platform.DB,
		"",
		"create_stream",
		[]any{contractInfo.Locator.StreamId.String(), contractInfo.Type.String()},
		func(row *common.Row) error {
			return nil
		},
	)
}

func DeleteStream(ctx context.Context, platform *kwilTesting.Platform, streamLocator types.StreamLocator) (*common.CallResult, error) {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       streamLocator.DataProvider.Bytes(),
		Caller:       streamLocator.DataProvider.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	return platform.Engine.Call(engineContext,
		platform.DB,
		"",
		"delete_stream",
		[]any{streamLocator.StreamId.String()},
		func(row *common.Row) error {
			return nil
		},
	)
}
