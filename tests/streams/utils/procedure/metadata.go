package procedure

import (
	"context"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/types"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	trufTypes "github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

type CheckReadPermissionsInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Wallet   string
}

// CheckReadPermissions checks if a wallet is allowed to read from a contract
func CheckReadPermissions(ctx context.Context, input CheckReadPermissionsInput) (bool, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return false, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var allowed bool
	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_allowed_to_read_all", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.Wallet,
		nil, // active_from, nil means no restriction
		nil, // active_to, nil means no restriction
	}, func(row *common.Row) error {
		if len(row.Values) > 0 {
			if val, ok := row.Values[0].(bool); ok {
				allowed = val
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return allowed, nil
}

type CheckWritePermissionsInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Wallet   string
}

// CheckWritePermissions checks if a wallet is allowed to write to a contract
func CheckWritePermissions(ctx context.Context, input CheckWritePermissionsInput) (bool, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return false, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var allowed bool
	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_allowed_to_write_all", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.Wallet,
	}, func(row *common.Row) error {
		if len(row.Values) > 0 {
			if val, ok := row.Values[0].(bool); ok {
				allowed = val
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return allowed, nil
}

type CheckComposePermissionsInput struct {
	Platform      *kwilTesting.Platform
	Locator       trufTypes.StreamLocator
	ForeignCaller string
}

// CheckComposePermissions checks if a stream is allowed to compose from another stream
func CheckComposePermissions(ctx context.Context, input CheckComposePermissionsInput) (bool, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return false, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var allowed bool
	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_stream_allowed_to_compose", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.ForeignCaller,
	}, func(row *common.Row) error {
		if len(row.Values) > 0 {
			if val, ok := row.Values[0].(bool); ok {
				allowed = val
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return allowed, nil
}

type InsertMetadataInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Key      string
	Value    string
	ValType  string
}

// InsertMetadata inserts metadata into a contract
func InsertMetadata(ctx context.Context, input InsertMetadataInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "insert_metadata", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.Key,
		input.Value,
		input.ValType,
	}, func(row *common.Row) error {
		return nil
	})
	return err
}

type TransferStreamOwnershipInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	NewOwner string
}

// TransferStreamOwnership transfers ownership of a stream to a new owner
func TransferStreamOwnership(ctx context.Context, input TransferStreamOwnershipInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "transfer_stream_ownership", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.NewOwner,
	}, func(row *common.Row) error {
		return nil
	})
	return err
}

type GetMetadataInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Key      string
}

// GetMetadata retrieves metadata from a contract
func GetMetadata(ctx context.Context, input GetMetadataInput) ([]any, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var results []any
	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_metadata", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.Key,
		true,
		nil,
		100,
		0,
		"created_at DESC",
	}, func(row *common.Row) error {
		results = append(results, row.Values...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

type DisableMetadataInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	RowID    *types.UUID
}

// DisableMetadata disables metadata in a contract
func DisableMetadata(ctx context.Context, input DisableMetadataInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "disable_metadata", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.RowID,
	}, func(row *common.Row) error {
		return nil
	})
	return err
}
