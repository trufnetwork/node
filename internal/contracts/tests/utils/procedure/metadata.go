package procedure

import (
	"context"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/types"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/util"
)

type CheckReadPermissionsInput struct {
	Platform *kwilTesting.Platform
	Deployer util.EthereumAddress
	DBID     string
	Wallet   string
}

// CheckReadPermissions checks if a wallet is allowed to read from a contract
func CheckReadPermissions(ctx context.Context, input CheckReadPermissionsInput) (bool, error) {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Deployer.Bytes(),
		Caller:       input.Deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	result, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
		Procedure: "is_wallet_allowed_to_read",
		Dataset:   input.DBID,
		Args:      []any{input.Wallet},
	})
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, errors.New("No result returned")
	}
	return result.Rows[0][0].(bool), nil
}

type CheckWritePermissionsInput struct {
	Platform *kwilTesting.Platform
	Deployer util.EthereumAddress
	DBID     string
	Wallet   string
}

// CheckWritePermissions checks if a wallet is allowed to write to a contract
func CheckWritePermissions(ctx context.Context, input CheckWritePermissionsInput) (bool, error) {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Deployer.Bytes(),
		Caller:       input.Deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	result, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
		Procedure: "is_wallet_allowed_to_write",
		Dataset:   input.DBID,
		Args:      []any{input.Wallet},
	})
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, errors.New("No result returned")
	}
	return result.Rows[0][0].(bool), nil
}

type CheckComposePermissionsInput struct {
	Platform      *kwilTesting.Platform
	DBID          string
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
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	result, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
		Procedure: "is_stream_allowed_to_compose",
		Dataset:   input.DBID,
		Args:      []any{input.ForeignCaller},
	})
	if err != nil {
		return false, err
	}
	if len(result.Rows) == 0 {
		return false, errors.New("No result returned")
	}
	return result.Rows[0][0].(bool), nil
}

type InsertMetadataInput struct {
	Platform *kwilTesting.Platform
	Deployer util.EthereumAddress
	DBID     string
	Key      string
	Value    string
	ValType  string
}

// InsertMetadata inserts metadata into a contract
func InsertMetadata(ctx context.Context, input InsertMetadataInput) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Deployer.Bytes(),
		Caller:       input.Deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	_, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
		Procedure: "insert_metadata",
		Dataset:   input.DBID,
		Args:      []any{input.Key, input.Value, input.ValType},
	})
	return err
}

type TransferStreamOwnershipInput struct {
	Platform *kwilTesting.Platform
	Deployer util.EthereumAddress
	DBID     string
	NewOwner string
}

// TransferStreamOwnership transfers ownership of a stream to a new owner
func TransferStreamOwnership(ctx context.Context, input TransferStreamOwnershipInput) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Deployer.Bytes(),
		Caller:       input.Deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	_, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
		Procedure: "transfer_stream_ownership",
		Dataset:   input.DBID,
		Args:      []any{input.NewOwner},
	})
	return err
}

type GetMetadataInput struct {
	Platform *kwilTesting.Platform
	Deployer util.EthereumAddress
	DBID     string
	Key      string
}

// GetMetadata retrieves metadata from a contract
func GetMetadata(ctx context.Context, input GetMetadataInput) ([]any, error) {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Deployer.Bytes(),
		Caller:       input.Deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	result, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
		Procedure: "get_metadata",
		Dataset:   input.DBID,
		Args:      []any{input.Key, true, nil},
	})
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, errors.New("No metadata found")
	}
	return result.Rows[0], nil
}

type DisableMetadataInput struct {
	Platform *kwilTesting.Platform
	Deployer util.EthereumAddress
	DBID     string
	RowID    *types.UUID
}

// DisableMetadata disables metadata in a contract
func DisableMetadata(ctx context.Context, input DisableMetadataInput) error {
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Deployer.Bytes(),
		Caller:       input.Deployer.Address(),
		TxID:         input.Platform.Txid(),
	}

	_, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
		Procedure: "disable_metadata",
		Dataset:   input.DBID,
		Args:      []any{input.RowID.String()},
	})
	return err
}
