package procedure

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kwilteam/kwil-db/common"
)

func GetRecord(ctx context.Context, input GetRecordInput) ([]ResultRow, error) {
	result, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "get_record",
		Dataset:   input.DBID,
		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
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
	result, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "get_index",
		Dataset:   input.DBID,
		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt, input.BaseDate},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
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
	result, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "get_index_change",
		Dataset:   input.DBID,
		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt, input.BaseDate, input.Interval},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
			TxID:   input.Platform.Txid(),
			Height: input.Height,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndexChange")
	}

	return processResultRows(result.Rows)
}

func processResultRows(rows [][]any) ([]ResultRow, error) {
	if len(rows) == 0 {
		return nil, errors.New("no rows returned from the procedure")
	}

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
