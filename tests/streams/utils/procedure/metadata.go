package procedure

import (
	"context"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
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
	Locators []trufTypes.StreamLocator
	Wallet   string
	Height   int64
}

type CheckWritePermissionsOutput struct {
	DataProvider string
	StreamID     string
	IsAllowed    bool
}

// CheckWritePermissions checks if a wallet is allowed to write to a contract
func CheckWritePermissions(ctx context.Context, input CheckWritePermissionsInput) ([]CheckWritePermissionsOutput, error) {
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

	dataProviders := make([]string, len(input.Locators))
	streamIDs := make([]string, len(input.Locators))
	for i, locator := range input.Locators {
		dataProviders[i] = locator.DataProvider.Address()
		streamIDs[i] = locator.StreamId.String()
	}

	var results []CheckWritePermissionsOutput
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "is_wallet_allowed_to_write_batch", []any{
		dataProviders,
		streamIDs,
		input.Wallet,
	}, func(row *common.Row) error {
		if len(row.Values) > 0 {
			if val, ok := row.Values[2].(bool); ok {
				results = append(results, CheckWritePermissionsOutput{
					DataProvider: row.Values[0].(string),
					StreamID:     row.Values[1].(string),
					IsAllowed:    val, // is_allowed
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in is_wallet_allowed_to_write_batch")
	}

	return results, nil
}

type CheckComposePermissionsInput struct {
	Platform         *kwilTesting.Platform
	Locator          trufTypes.StreamLocator
	ComposingLocator trufTypes.StreamLocator
	Height           int64
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
		input.ComposingLocator.DataProvider.Address(),
		input.ComposingLocator.StreamId.String(),
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

type GetMetadataOutput struct {
	RowID     *types.UUID
	ValueI    *int64
	ValueF    *float64
	ValueB    *bool
	ValueS    *string
	ValueR    *string
	CreatedAt int64
}

// GetMetadata retrieves metadata from a contract
func GetMetadata(ctx context.Context, input GetMetadataInput) ([]GetMetadataOutput, error) {
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

	var results []GetMetadataOutput
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_metadata", []any{
		input.Locator.DataProvider.Address(),
		input.Locator.StreamId.String(),
		input.Key,
		nil,
		1, // get only latest row
		0,
		"created_at DESC",
	}, func(row *common.Row) error {
		results = append(results, GetMetadataOutput{
			RowID:     safe(row.Values[0], nil, uuidConverter),
			ValueI:    safe(row.Values[1], nil, int64PtrConverter),
			ValueF:    safe(row.Values[2], nil, float64PtrConverter),
			ValueB:    safe(row.Values[3], nil, boolPtrConverter),
			ValueS:    safe(row.Values[4], nil, stringPtrConverter),
			ValueR:    safe(row.Values[5], nil, stringPtrConverter),
			CreatedAt: safe(row.Values[6], int64(0), int64ValueConverter),
		})
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

// Replace all the safe* functions with a generic approach
func safe[T any](v any, defaultVal T, converter func(any) (T, bool)) T {
	if v == nil {
		return defaultVal
	}
	if result, ok := converter(v); ok {
		return result
	}
	return defaultVal
}

// Type conversion functions
func uuidConverter(v any) (*types.UUID, bool) {
	result, ok := v.(*types.UUID)
	return result, ok
}

func int64PtrConverter(v any) (*int64, bool) {
	result, ok := v.(int64)
	return &result, ok
}

func float64PtrConverter(v any) (*float64, bool) {
	result, ok := v.(float64)
	return &result, ok
}

func boolPtrConverter(v any) (*bool, bool) {
	result, ok := v.(bool)
	return &result, ok
}

func stringPtrConverter(v any) (*string, bool) {
	result, ok := v.(string)
	return &result, ok
}

func int64ValueConverter(v any) (int64, bool) {
	result, ok := v.(int64)
	return result, ok
}
