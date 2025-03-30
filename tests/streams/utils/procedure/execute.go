package procedure

import (
	"context"
	"fmt"

	"github.com/trufnetwork/sdk-go/core/util"

	"github.com/kwilteam/kwil-db/common"
	kwilTypes "github.com/kwilteam/kwil-db/core/types"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/types"
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
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_record", []any{
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
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in getRecord")
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
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_index", []any{
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
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in getIndex")
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
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_index_change", []any{
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
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in getIndexChange")
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
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_first_record", []any{
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
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in getFirstRecord")
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

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "set_metadata", []any{
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
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in setMetadata")
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
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "describe_taxonomies", []any{
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
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in DescribeTaxonomies.Procedure")
	}

	return processResultRows(resultRows)
}

// SetTaxonomy sets the taxonomy for a composed stream with optional start date
func SetTaxonomy(ctx context.Context, input SetTaxonomyInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating composed dataset")
	}

	primitiveStreamStrings := []string{}
	dataProviderStrings := []string{}
	var weightDecimals []*kwilTypes.Decimal
	for i, item := range input.StreamIds {
		primitiveStreamStrings = append(primitiveStreamStrings, item)
		dataProviderStrings = append(dataProviderStrings, input.DataProviders[i])
		// should be formatted as 0.000000000000000000 (18 decimal places)
		valueDecimal, err := kwilTypes.ParseDecimalExplicit(input.Weights[i], 36, 18)
		if err != nil {
			return errors.Wrap(err, "error parsing weight")
		}
		weightDecimals = append(weightDecimals, valueDecimal)
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

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "insert_taxonomy", []any{
		input.StreamLocator.DataProvider.Address(), // parent data provider
		input.StreamLocator.StreamId.String(),      // parent stream id
		dataProviderStrings,                        // child data providers
		primitiveStreamStrings,                     // child stream ids
		weightDecimals,
		input.StartTime,
	}, func(row *common.Row) error {
		return nil
	})
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in insert_taxonomy")
	}
	return err
}

func GetCategoryStreams(ctx context.Context, input GetCategoryStreamsInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in getCategoryStreams")
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 0,
		},
		TxID:   input.Platform.Txid(),
		Signer: input.Platform.Deployer,
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	var resultRows [][]any
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_category_streams", []any{
		input.DataProvider,
		input.StreamId,
		input.ActiveFrom,
		input.ActiveTo,
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
		return nil, errors.Wrap(err, "error in getCategoryStreams")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in getCategoryStreams")
	}

	return processResultRows(resultRows)
}

// FilterStreamsByExistence filters streams based on existence, returning either existing or non-existing streams
// based on the ReturnExisting flag in the input
func FilterStreamsByExistence(ctx context.Context, input FilterStreamsByExistenceInput) ([]types.StreamLocator, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in FilterStreamsByExistence")
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

	dataProviders := []string{}
	streamIds := []string{}
	for _, streamLocator := range input.StreamLocators {
		dataProviders = append(dataProviders, streamLocator.DataProvider.Address())
		streamIds = append(streamIds, streamLocator.StreamId.String())
	}

	var resultRows []types.StreamLocator
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "filter_streams", []any{
		dataProviders,
		streamIds,
		input.ExistingOnly,
	}, func(row *common.Row) error {
		// return [dataprovider, streamid][]
		streamLocator := types.StreamLocator{}
		streamLocator.DataProvider, err = util.NewEthereumAddressFromString(row.Values[0].(string))
		if err != nil {
			return errors.Wrap(err, "error in FilterStreamsByExistence")
		}
		streamId, err := util.NewStreamId(row.Values[1].(string))
		if err != nil {
			return errors.Wrap(err, "error in FilterStreamsByExistence")
		}
		streamLocator.StreamId = *streamId
		resultRows = append(resultRows, streamLocator)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in FilterStreamsByExistence")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in FilterStreamsByExistence")
	}

	return resultRows, nil
}

func DisableTaxonomy(ctx context.Context, input DisableTaxonomyInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error in DisableTaxonomy")
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

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "disable_taxonomy", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.GroupSequence,
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error in DisableTaxonomy")
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in DisableTaxonomy")
	}

	return nil
}

type ListStreamsInput struct {
	Platform     *kwilTesting.Platform
	Height       int64
	DataProvider string
	Limit        int
	Offset       int
	OrderBy      string
}

func ListStreams(ctx context.Context, input ListStreamsInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in ListStreams")
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
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "list_streams", []any{
		input.DataProvider,
		input.Limit,
		input.Offset,
		input.OrderBy,
	}, func(row *common.Row) error {
		values := make([]any, len(row.Values))
		for i, v := range row.Values {
			values[i] = v
		}
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in ListStreams")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in ListStreams")
	}

	return processResultRows(resultRows)
}
