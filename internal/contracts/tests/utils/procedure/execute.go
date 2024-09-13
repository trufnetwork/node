package procedure

import (
	"context"
	"fmt"
	"github.com/pkg/errors"

	"github.com/kwilteam/kwil-db/common"
)

func GetRecord(ctx context.Context, input GetRecordOrIndexInput) ([]ResultRow, error) {
	return getX(ctx, "get_record", input)
}

func GetIndex(ctx context.Context, input GetRecordOrIndexInput) ([]ResultRow, error) {
	return getX(ctx, "get_index", input)
}

func getX(ctx context.Context, procedure string, input GetRecordOrIndexInput) ([]ResultRow, error) {
	result, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: procedure,
		Dataset:   input.DBID,
		Args:      []any{input.DateFrom, input.DateTo, input.Height},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
			TxID:   input.Platform.Txid(),
			Height: input.Height,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getX")
	}

	var resultRows []ResultRow
	for _, row := range result.Rows {
		resultRow := ResultRow{}
		for _, value := range row {
			resultRow = append(resultRow, fmt.Sprintf("%v", value))
		}
		resultRows = append(resultRows, resultRow)
	}

	return resultRows, nil
}

func GetIndexChange(ctx context.Context, input GetIndexChangeInput) ([]ResultRow, error) {
	result, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "get_index_change",
		Dataset:   input.DBID,
		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt, input.Interval},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
			TxID:   input.Platform.Txid(),
			Height: input.Height,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndexChange")
	}

	var resultRows []ResultRow
	for _, row := range result.Rows {
		resultRow := ResultRow{}
		for _, value := range row {
			resultRow = append(resultRow, fmt.Sprintf("%v", value))
		}
		resultRows = append(resultRows, resultRow)
	}

	return resultRows, nil
}
