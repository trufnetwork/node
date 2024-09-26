package procedure

import (
	"context"
	"fmt"

	"github.com/truflation/tsn-sdk/core/util"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
)

func GetRecord(ctx context.Context, input GetRecordInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in getRecord")
	}

	result, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "get_record",
		Dataset:   input.DBID,
		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
			Caller: deployer.Address(),
			TxID:   input.Platform.Txid(),
			Height: input.Height,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getRecord")
	}

	return processResultRows(result.Rows)
}

func GetIndex(ctx context.Context, input GetIndexInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndex")
	}

	result, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "get_index",
		Dataset:   input.DBID,
		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt, input.BaseDate},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
			Caller: deployer.Address(),
			TxID:   input.Platform.Txid(),
			Height: input.Height,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndex")
	}

	return processResultRows(result.Rows)
}

func GetIndexChange(ctx context.Context, input GetIndexChangeInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndexChange")
	}

	result, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "get_index_change",
		Dataset:   input.DBID,
		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt, input.BaseDate, input.Interval},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
			Caller: deployer.Address(),
			TxID:   input.Platform.Txid(),
			Height: input.Height,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndexChange")
	}

	return processResultRows(result.Rows)
}

func GetFirstRecord(ctx context.Context, input GetFirstRecordInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in getFirstRecord")
	}

	result, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "get_first_record",
		Dataset:   input.DBID,
		Args:      []any{input.AfterDate, input.FrozenAt},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
			Caller: deployer.Address(),
			TxID:   input.Platform.Txid(),
			Height: input.Height,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getFirstRecord")
	}

	return processResultRows(result.Rows)
}

func processResultRows(rows [][]any) ([]ResultRow, error) {
	resultRows := make([]ResultRow, len(rows))
	for i, row := range rows {
		resultRow := ResultRow{}
		for _, value := range row {
			resultRow = append(resultRow, fmt.Sprintf("%v", value))
		}
		resultRows[i] = resultRow
	}

	return resultRows, nil
}

// WithSigner returns a new platform with the given signer, but doesn't mutate the original platform
func WithSigner(platform *kwilTesting.Platform, signer []byte) *kwilTesting.Platform {
	newPlatform := *platform // create a copy of the original platform
	newPlatform.Deployer = signer
	return &newPlatform
}
