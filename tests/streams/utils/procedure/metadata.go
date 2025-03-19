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

type CheckReadAllPermissionsInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Wallet   string
	Height   int64
}

// CheckReadAllPermissions checks if a wallet is allowed to read from all substreams of a stream
func CheckReadAllPermissions(ctx context.Context, input CheckReadAllPermissionsInput) (bool, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return false, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var allowed bool
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_allowed_to_read_all", []any{
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
	if r.Error != nil {
		return false, errors.Wrap(r.Error, "error in is_allowed_to_read_all")
	}

	return allowed, nil
}

type CheckComposeAllPermissionsInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Height   int64
}

// CheckComposeAllPermissions checks if a wallet is allowed to compose from all substreams of a stream
func CheckComposeAllPermissions(ctx context.Context, input CheckComposeAllPermissionsInput) (bool, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return false, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var allowed bool
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_allowed_to_compose_all", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
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
	if r.Error != nil {
		return false, errors.Wrap(r.Error, "error in is_allowed_to_compose")
	}

	return allowed, nil
}

type CheckReadPermissionsInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Wallet   string
	Height   int64
}

// CheckReadPermissions checks if a wallet is allowed to read from a specific stream
func CheckReadPermissions(ctx context.Context, input CheckReadPermissionsInput) (bool, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return false, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var allowed bool
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_allowed_to_read", []any{
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
	if r.Error != nil {
		return false, errors.Wrap(r.Error, "error in is_allowed_to_read")
	}

	return allowed, nil
}

type CheckWritePermissionsInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Wallet   string
	Height   int64
}

// CheckWritePermissions checks if a wallet is allowed to write to a contract
func CheckWritePermissions(ctx context.Context, input CheckWritePermissionsInput) (bool, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return false, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var allowed bool
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_allowed_to_write_all", []any{
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
	if r.Error != nil {
		return false, errors.Wrap(r.Error, "error in is_allowed_to_write_all")
	}

	return allowed, nil
}

type CheckComposePermissionsInput struct {
	Platform          *kwilTesting.Platform
	Locator           trufTypes.StreamLocator
	ComposingStreamId string
	Height            int64
}

// CheckComposePermissions checks if a stream is allowed to compose from another stream
func CheckComposePermissions(ctx context.Context, input CheckComposePermissionsInput) (bool, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return false, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var allowed bool
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_allowed_to_compose", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.ComposingStreamId,
		nil,
		nil,
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
	if r.Error != nil {
		return false, errors.Wrap(r.Error, "error in is_allowed_to_compose")
	}

	return allowed, nil
}

type InsertMetadataInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Key      string
	Value    string
	ValType  string
	Height   int64
}

// InsertMetadata inserts metadata into a contract
func InsertMetadata(ctx context.Context, input InsertMetadataInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "insert_metadata", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.Key,
		input.Value,
		input.ValType,
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return err
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in insert_metadata")
	}

	return nil
}

type TransferStreamOwnershipInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	NewOwner string
	Height   int64
}

// TransferStreamOwnership transfers ownership of a stream to a new owner
func TransferStreamOwnership(ctx context.Context, input TransferStreamOwnershipInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "transfer_stream_ownership", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.NewOwner,
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return err
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in transfer_stream_ownership")
	}

	return nil
}

type GetMetadataInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Key      string
	Height   int64
}

// GetMetadata retrieves metadata from a contract
func GetMetadata(ctx context.Context, input GetMetadataInput) ([]any, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var results []any
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_metadata", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.Key,
		nil,
		1, // get only latest row
		0,
		"created_at DESC",
	}, func(row *common.Row) error {
		results = append(results, row.Values...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in get_metadata")
	}

	return results, nil
}

type DisableMetadataInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	RowID    *types.UUID
	Height   int64
}

// DisableMetadata disables metadata in a contract
func DisableMetadata(ctx context.Context, input DisableMetadataInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
	}

	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "disable_metadata", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.RowID,
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return err
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in disable_metadata")
	}

	return nil
}
