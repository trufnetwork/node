package setup

import (
	"context"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
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
	return CreateStreamWithOptions(ctx, platform, contractInfo, CreateStreamOptions{})
}

// CreateStreamOptions provides configuration for stream creation
type CreateStreamOptions struct {
	SkipAutoRoleGrant bool // Skip automatic network_writer role granting for permission testing
}

// CreateStreamWithOptions parses and creates the dataset for a contract with configurable options
func CreateStreamWithOptions(ctx context.Context, platform *kwilTesting.Platform, contractInfo StreamInfo, opts CreateStreamOptions) error {
	if !opts.SkipAutoRoleGrant {
		// Auto-enable the data provider to create streams (high ROI: zero test changes needed)
		addr, err := util.NewEthereumAddressFromString(contractInfo.Locator.DataProvider.Address())
		if err != nil {
			return errors.Wrap(err, "invalid data provider address")
		}

		// Grant the data provider the network_writer role
		err = AddMemberToRoleBypass(ctx, platform, "system", "network_writer", addr.Address())
		if err != nil {
			return errors.Wrap(err, "failed to enable stream deployer")
		}
	}

	return UntypedCreateStream(ctx, platform, contractInfo.Locator.StreamId.String(), contractInfo.Locator.DataProvider.Address(), string(contractInfo.Type))
}

func UntypedCreateStream(ctx context.Context, platform *kwilTesting.Platform, streamId string, dataProvider string, contractType string) error {
	// Convert address string to proper bytes using util function
	addr, err := util.NewEthereumAddressFromString(dataProvider)
	if err != nil {
		return errors.Wrap(err, "invalid data provider address")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       addr.Bytes(),
		Caller:       addr.Address(),
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
	return CreateStreamsWithOptions(ctx, platform, streamInfos, CreateStreamsOptions{})
}

// CreateStreamsOptions provides configuration for batch stream creation
type CreateStreamsOptions struct {
	SkipAutoRoleGrant bool // Skip automatic network_writer role granting for permission testing
}

// CreateStreamsWithOptions creates multiple streams with configurable options
func CreateStreamsWithOptions(ctx context.Context, platform *kwilTesting.Platform, streamInfos []StreamInfo, opts CreateStreamsOptions) error {
	if !opts.SkipAutoRoleGrant {
		// Auto-enable the platform deployer to create streams (high ROI: zero test changes needed)
		deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
		if err != nil {
			return errors.Wrap(err, "error creating composed dataset")
		}

		// Grant the platform deployer the network_writer role
		err = AddMemberToRoleBypass(ctx, platform, "system", "network_writer", deployer.Address())
		if err != nil {
			return errors.Wrap(err, "failed to enable platform stream deployer")
		}
	}

	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating composed dataset")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
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
		BlockContext: &common.BlockContext{Height: 1},
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
