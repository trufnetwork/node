package setup

import (
	"context"
	"strings"

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

func CreateDataProvider(ctx context.Context, platform *kwilTesting.Platform, address string) error {
	addr, err := util.NewEthereumAddressFromString(address)
	if err != nil {
		return errors.Wrap(err, "invalid data provider address")
	}

	// Grant the data provider the network_writer role
	err = AddMemberToRoleBypass(ctx, platform, "system", "network_writer", addr.Address())
	if err != nil {
		return errors.Wrap(err, "failed to enable stream deployer")
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
		"create_data_provider",
		[]any{addr.Address()},
		func(row *common.Row) error {
			return nil
		},
	)
	if err != nil {
		return errors.Wrap(err, "error in createDataProvider")
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in createDataProvider")
	}

	return nil
}

// CreateDataProviderWithoutRole registers a data provider WITHOUT granting the network_writer role.
// This is useful for testing fee collection scenarios where the data provider should pay fees.
//
// Note: This function:
// 1. Temporarily grants network_writer role to register the provider (required by create_data_provider action)
// 2. Immediately revokes the role after registration
// 3. Leaves the data provider registered but non-whitelisted (will pay fees)
func CreateDataProviderWithoutRole(ctx context.Context, platform *kwilTesting.Platform, address string) error {
	addr, err := util.NewEthereumAddressFromString(address)
	if err != nil {
		return errors.Wrap(err, "invalid data provider address")
	}

	// First, grant role temporarily (required to call create_data_provider)
	err = AddMemberToRoleBypass(ctx, platform, "system", "network_writer", addr.Address())
	if err != nil {
		return errors.Wrap(err, "failed to grant temporary network_writer role")
	}

	// Register the data provider
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
		"create_data_provider",
		[]any{addr.Address()},
		func(row *common.Row) error {
			return nil
		},
	)
	if err != nil {
		// Clean up: revoke the temporary role before returning error
		_ = removeMemberFromRoleBypass(ctx, platform, "system", "network_writer", addr.Address())
		return errors.Wrap(err, "error in create_data_provider")
	}
	if r.Error != nil {
		// Clean up: revoke the temporary role before returning error
		_ = removeMemberFromRoleBypass(ctx, platform, "system", "network_writer", addr.Address())
		return errors.Wrap(r.Error, "error in create_data_provider")
	}

	// Now revoke the role so they have to pay fees
	err = removeMemberFromRoleBypass(ctx, platform, "system", "network_writer", addr.Address())
	if err != nil {
		return errors.Wrap(err, "failed to revoke network_writer role")
	}

	return nil
}

// removeMemberFromRoleBypass revokes a role using direct SQL with OverrideAuthz.
// This mirrors AddMemberToRoleBypass and uses direct SQL instead of calling revoke_roles
// action because:
// 1. Calling revoke_roles requires authorization (caller must be role owner or manager)
// 2. Test setup doesn't guarantee proper authorization context
// 3. This is a test utility following the same pattern as AddMemberToRoleBypass
// 4. Using OverrideAuthz is the standard pattern for test role management
func removeMemberFromRoleBypass(ctx context.Context, platform *kwilTesting.Platform, owner, roleName, wallet string) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		TxID:         platform.Txid(),
		Signer:       []byte("system"),
		Caller:       "0x0000000000000000000000000000000000000000",
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true, // Skip authorization checks - this is a test utility
	}

	// Direct SQL DELETE is idempotent - deleting a non-existent role membership is a no-op
	sql := `DELETE FROM role_members WHERE owner = $owner AND role_name = $role_name AND wallet = $wallet`

	// Normalize to lowercase to match AddMemberToRoleBypass behavior
	// (role_members table stores lowercase values, checksummed addresses won't match otherwise)
	err := platform.Engine.Execute(engineContext, platform.DB, sql, map[string]any{
		"$owner":     strings.ToLower(owner),
		"$role_name": strings.ToLower(roleName),
		"$wallet":    strings.ToLower(wallet),
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to remove role member")
	}

	return nil
}

// GetStreamId resolves a stream reference using the data provider address and stream ID
// This uses the get_stream_id action from the database to dynamically resolve stream refs
// instead of hardcoding them as "streamRef := 1"
func GetStreamId(ctx context.Context, platform *kwilTesting.Platform, dataProviderAddress string, streamId string) (int, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return 0, errors.Wrap(err, "error creating ethereum address")
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

	var streamRef int
	r, err := platform.Engine.Call(engineContext, platform.DB, "", "get_stream_id", []any{
		dataProviderAddress,
		streamId,
	}, func(row *common.Row) error {
		if len(row.Values) != 1 {
			return errors.Errorf("expected 1 column, got %d", len(row.Values))
		}

		if row.Values[0] == nil {
			return errors.New("stream not found")
		}

		streamRefInt, ok := row.Values[0].(int64)
		if !ok {
			return errors.New("stream_ref is not int64")
		}

		streamRef = int(streamRefInt)
		return nil
	})

	if err != nil {
		return 0, err
	}
	if r.Error != nil {
		return 0, errors.Wrap(r.Error, "get_stream_id failed")
	}

	return streamRef, nil
}

// GetStreamIdForDeployer is a convenience function that resolves stream ref for the test deployer
func GetStreamIdForDeployer(ctx context.Context, platform *kwilTesting.Platform, streamName string) (int, error) {
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return 0, errors.Wrap(err, "error creating ethereum address")
	}

	streamId := util.GenerateStreamId(streamName)

	return GetStreamId(ctx, platform, deployer.Address(), streamId.String())
}
