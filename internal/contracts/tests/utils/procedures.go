package testutils

import (
	"context"
	"fmt"
	"testing"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/stretchr/testify/assert"

	"github.com/pkg/errors"

	"github.com/kwilteam/kwil-db/common"
)

type GetRecordOrIndexInput struct {
	Platform *kwilTesting.Platform
	DBID     string
	DateFrom string
	DateTo   string
	Height   int64
}

type ResultRow []string

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

type GetIndexChangeInput struct {
	Platform *kwilTesting.Platform
	DBID     string
	DateFrom string
	DateTo   string
	Interval int
	FrozenAt *string
	Height   int64
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

func AssertResultRowsEqualMarkdownTable(t *testing.T, actual []ResultRow, markdownTable string) {
	expectedTable, err := TableFromMarkdown(markdownTable)
	if err != nil {
		t.Fatalf("error parsing expected markdown table: %v", err)
	}

	// clear empty rows, because we won't get those from answer, but
	// tests might include it just to be explicit about what is being tested
	expected := [][]string{}
	for _, row := range expectedTable.Rows {
		if row[1] != "" {
			expected = append(expected, row)
		}
	}

	actualInStrings := [][]string{}
	for _, row := range actual {
		actualInStrings = append(actualInStrings, row)
	}

	assert.Equal(t, expected, actualInStrings, "Result rows do not match expected markdown table")
}
