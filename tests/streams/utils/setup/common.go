package setup

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/crypto"
	coreauth "github.com/trufnetwork/kwil-db/core/crypto/auth"
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

var (
	defaultLeaderOnce sync.Once
	defaultLeaderPub  *crypto.Secp256k1PublicKey
)

func defaultLeaderPublicKey() *crypto.Secp256k1PublicKey {
	defaultLeaderOnce.Do(func() {
		_, pubGeneric, err := crypto.GenerateSecp256k1Key(nil)
		if err != nil {
			panic(fmt.Sprintf("failed to generate default leader key: %v", err))
		}
		var ok bool
		defaultLeaderPub, ok = pubGeneric.(*crypto.Secp256k1PublicKey)
		if !ok {
			panic("default leader public key is not secp256k1")
		}
	})
	return defaultLeaderPub
}

func newTxContextWithLeader(ctx context.Context, platform *kwilTesting.Platform, signer []byte, caller string, authenticator string, height int64) *common.TxContext {
	if height <= 0 {
		height = 1
	}
	return &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height:   height,
			Proposer: defaultLeaderPublicKey(),
		},
		Signer:        signer,
		Caller:        caller,
		TxID:          platform.Txid(),
		Authenticator: authenticator,
	}
}

func newEthEngineContext(ctx context.Context, platform *kwilTesting.Platform, addr util.EthereumAddress, height int64) *common.EngineContext {
	txContext := newTxContextWithLeader(ctx, platform, addr.Bytes(), addr.Address(), coreauth.EthPersonalSignAuth, height)
	return &common.EngineContext{TxContext: txContext}
}

// NewEngineContext returns an engine context configured with the provided signer and a deterministic leader.
func NewEngineContext(ctx context.Context, platform *kwilTesting.Platform, addr util.EthereumAddress, height int64) *common.EngineContext {
	return newEthEngineContext(ctx, platform, addr, height)
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

	engineContext := newEthEngineContext(ctx, platform, addr, 1)

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

	engineContext := newEthEngineContext(ctx, platform, deployer, 1)

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
	engineContext := newEthEngineContext(ctx, platform, streamLocator.DataProvider, 1)

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

	engineContext := newEthEngineContext(ctx, platform, addr, 1)

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
	engineContext := newEthEngineContext(ctx, platform, addr, 1)

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
	engineContext := &common.EngineContext{
		TxContext: newTxContextWithLeader(
			ctx,
			platform,
			[]byte("system"),
			"0x0000000000000000000000000000000000000000",
			"",
			1,
		),
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

	engineContext := newEthEngineContext(ctx, platform, deployer, 1)

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
