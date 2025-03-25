package setup

import (
	"context"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
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
func CreateStream(ctx context.Context, platform *kwilTesting.Platform, contractInfo StreamInfo) error {
	return UntypedCreateStream(ctx, platform, contractInfo.Locator.StreamId.String(), contractInfo.Locator.DataProvider.Address(), string(contractInfo.Type))
}

func UntypedCreateStream(ctx context.Context, platform *kwilTesting.Platform, streamId string, dataProvider string, contractType string) error {
	// Convert hex string to bytes for the signer
	var signerBytes []byte
	if len(dataProvider) > 2 {
		// Remove 0x prefix if present
		if dataProvider[:2] == "0x" {
			signerBytes = []byte(dataProvider[2:])
		} else {
			signerBytes = []byte(dataProvider)
		}
	}
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       signerBytes,
		Caller:       dataProvider,
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	r, err := platform.Engine.Call(engineContext,
		platform.DB,
		"",
		"create_stream",
		[]any{streamId, contractType},
		func(row *common.Row) error {
			return nil
		},
	)
	if err != nil {
		return errors.Wrap(err, "error in createStream")
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in createStream")
	}

	return nil
}

func CreateStreams(ctx context.Context, platform *kwilTesting.Platform, streamInfos []StreamInfo) error {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating composed dataset")
	}
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	streamIds := make([]string, len(streamInfos))
	streamTypes := make([]string, len(streamInfos))
	for i, streamInfo := range streamInfos {
		streamIds[i] = streamInfo.Locator.StreamId.String()
		streamTypes[i] = string(streamInfo.Type)
	}

	// execute create streams call instead of creating one by one
	r, err := platform.Engine.Call(engineContext,
		platform.DB,
		"",
		"create_streams",
		[]any{streamIds, streamTypes},
		func(row *common.Row) error {
			return nil
		},
	)
	if err != nil {
		return errors.Wrap(err, "error in createStreams")
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in createStreams")
	}

	return nil
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
		[]any{
			streamLocator.DataProvider.Address(),
			streamLocator.StreamId.String(),
		},
		func(row *common.Row) error {
			return nil
		},
	)
}
