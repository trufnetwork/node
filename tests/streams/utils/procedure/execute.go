package procedure

import (
	"context"
	"fmt"

	"github.com/trufnetwork/sdk-go/core/util"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
)

func GetRecord(ctx context.Context, input GetRecordInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in getRecord")
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: input.Height,
		},
		TxID:   input.Platform.Txid(),
		Signer: input.Platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var resultRows [][]any
	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_record", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.FromTime,
		input.ToTime,
		input.FrozenAt,
	}, func(row *common.Row) error {
		// Convert the row values to []any
		values := make([]any, len(row.Values))
		for i, v := range row.Values {
			values[i] = v
		}
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getRecord")
	}

	return processResultRows(resultRows)
}

func GetIndex(ctx context.Context, input GetIndexInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndex")
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: input.Height,
		},
		TxID:   input.Platform.Txid(),
		Signer: input.Platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var resultRows [][]any
	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_index", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.FromTime,
		input.ToTime,
		input.FrozenAt,
		input.BaseTime,
	}, func(row *common.Row) error {
		// Convert the row values to []any
		values := make([]any, len(row.Values))
		for i, v := range row.Values {
			values[i] = v
		}
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndex")
	}

	return processResultRows(resultRows)
}

func GetIndexChange(ctx context.Context, input GetIndexChangeInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndexChange")
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: input.Height,
		},
		TxID:   input.Platform.Txid(),
		Signer: input.Platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var resultRows [][]any
	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_index_change", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.FromTime,
		input.ToTime,
		input.FrozenAt,
		input.BaseTime,
		input.Interval,
	}, func(row *common.Row) error {
		// Convert the row values to []any
		values := make([]any, len(row.Values))
		for i, v := range row.Values {
			values[i] = v
		}
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndexChange")
	}

	return processResultRows(resultRows)
}

func GetFirstRecord(ctx context.Context, input GetFirstRecordInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in getFirstRecord")
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: input.Height,
		},
		TxID:   input.Platform.Txid(),
		Signer: input.Platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var resultRows [][]any
	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_first_record", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.AfterTime,
		input.FrozenAt,
	}, func(row *common.Row) error {
		// Convert the row values to []any
		values := make([]any, len(row.Values))
		for i, v := range row.Values {
			values[i] = v
		}
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getFirstRecord")
	}

	return processResultRows(resultRows)
}

func SetMetadata(ctx context.Context, input SetMetadataInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error in setMetadata")
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: input.Height,
		},
		TxID:   input.Platform.Txid(),
		Signer: input.Platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "set_metadata", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.Key,
		input.Value,
		input.ValType,
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error in setMetadata")
	}

	return nil
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

type DescribeTaxonomiesInput struct {
	Platform      *kwilTesting.Platform
	StreamId      string
	DataProvider  string
	LatestVersion bool
}

// DescribeTaxonomies is a helper function to describe taxonomies of a composed stream
func DescribeTaxonomies(ctx context.Context, input DescribeTaxonomiesInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in DescribeTaxonomies.NewEthereumAddressFromBytes")
	}

	txContext := &common.TxContext{
		BlockContext: &common.BlockContext{Height: 0},
		Signer:       input.Platform.Deployer,
		Caller:       deployer.Address(),
		TxID:         input.Platform.Txid(),
		Ctx:          ctx,
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var resultRows [][]any
	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "describe_taxonomies", []any{
		input.DataProvider,
		input.StreamId,
		input.LatestVersion,
	}, func(row *common.Row) error {
		// Convert the row values to []any
		values := make([]any, len(row.Values))
		for i, v := range row.Values {
			values[i] = v
		}
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in DescribeTaxonomies.Procedure")
	}

	return processResultRows(resultRows)
}

// SetTaxonomy sets the taxonomy for a composed stream with optional start date
func SetTaxonomy(ctx context.Context, input SetTaxonomyInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error in SetTaxonomy")
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

	_, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "set_taxonomy", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.DataProviders,
		input.StreamIds,
		input.Weights,
		input.StartTime,
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error in SetTaxonomy")
	}

	return nil
}
