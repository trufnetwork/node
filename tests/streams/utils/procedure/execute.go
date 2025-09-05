package procedure

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	trufTypes "github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/sdk-go/core/types"
)

// GetDataResult contains the full result of a GetRecord call
type GetDataResult struct {
	Rows     []ResultRow
	Logs     []string
	CacheHit bool   // Whether the result came from cache
	CachedAt *int64 // Timestamp when data was cached (only set on cache hit)
}

// parseCacheInfoFromLogs parses cache hit information from procedure logs
// Returns true if any log shows cache_hit=true
func parseCacheInfoFromLogs(logs []string) (cacheHit bool, cachedAt *int64) {
	for _, log := range logs {
		if strings.Contains(log, "cache_hit") {
			var logData map[string]interface{}
			if err := json.Unmarshal([]byte(log), &logData); err == nil {
				if hit, ok := logData["cache_hit"].(bool); ok && hit {
					cacheHit = true
					if logData["cached_at"] != nil {
						if timestamp, ok := logData["cached_at"].(float64); ok {
							timestampInt := int64(timestamp)
							cachedAt = &timestampInt
						}
					}
					// Don't break - continue checking other logs in case there are multiple
				}
			}
		}
	}
	return
}

// GetRecordWithLogs executes get_record and returns full result including logs
func GetRecordWithLogs(ctx context.Context, input GetRecordInput) (*GetDataResult, error) {
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

	prefix := ""
	if input.Prefix != nil {
		prefix = *input.Prefix
	}

	var resultRows [][]any
	var r *common.CallResult

	if input.UseCache == nil {
		// Call with 5 parameters (omit use_cache entirely)
		r, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", prefix+"get_record", []any{
			input.StreamLocator.DataProvider.Address(),
			input.StreamLocator.StreamId.String(),
			input.FromTime,
			input.ToTime,
			input.FrozenAt,
		}, func(row *common.Row) error {
			values := make([]any, len(row.Values))
			copy(values, row.Values)
			resultRows = append(resultRows, values)
			return nil
		})
	} else {
		// Call with 6 parameters (include use_cache)
		r, err = input.Platform.Engine.Call(engineContext, input.Platform.DB, "", prefix+"get_record", []any{
			input.StreamLocator.DataProvider.Address(),
			input.StreamLocator.StreamId.String(),
			input.FromTime,
			input.ToTime,
			input.FrozenAt,
			*input.UseCache,
		}, func(row *common.Row) error {
			values := make([]any, len(row.Values))
			copy(values, row.Values)
			resultRows = append(resultRows, values)
			return nil
		})
	}

	if err != nil {
		return nil, errors.Wrap(err, "error in getRecord")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in getRecord")
	}

	processedRows, err := processResultRows(resultRows)
	if err != nil {
		return nil, err
	}

	// Parse cache information from logs
	cacheHit, cachedAt := parseCacheInfoFromLogs(r.Logs)

	return &GetDataResult{
		Rows:     processedRows,
		Logs:     r.Logs,
		CacheHit: cacheHit,
		CachedAt: cachedAt,
	}, nil
}

// GetRecord executes get_record and returns results
func GetRecord(ctx context.Context, input GetRecordInput) ([]ResultRow, error) {
	result, err := GetRecordWithLogs(ctx, input)
	if err != nil {
		return nil, err
	}

	// Print logs if requested
	if input.PrintLogs != nil && *input.PrintLogs {
		fmt.Println("getRecord logs:")
		for _, log := range result.Logs {
			fmt.Println(log)
		}
	}

	return result.Rows, nil
}

// GetIndexWithLogs executes get_index and returns full result including logs
func GetIndexWithLogs(ctx context.Context, input GetIndexInput) (*GetDataResult, error) {
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

	prefix := ""
	if input.Prefix != nil {
		prefix = *input.Prefix
	}

	// Set default use_cache to false if not specified
	useCache := false
	if input.UseCache != nil {
		useCache = *input.UseCache
	}

	var resultRows [][]any
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", prefix+"get_index", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.FromTime,
		input.ToTime,
		input.FrozenAt,
		input.BaseTime,
		useCache,
	}, func(row *common.Row) error {
		// Convert the row values to []any
		values := make([]any, len(row.Values))
		copy(values, row.Values)
		resultRows = append(resultRows, values)
		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "error in getIndex")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in getIndex")
	}

	processedRows, err := processResultRows(resultRows)
	if err != nil {
		return nil, err
	}

	// Parse cache information from logs
	cacheHit, cachedAt := parseCacheInfoFromLogs(r.Logs)

	return &GetDataResult{
		Rows:     processedRows,
		Logs:     r.Logs,
		CacheHit: cacheHit,
		CachedAt: cachedAt,
	}, nil
}

func GetIndex(ctx context.Context, input GetIndexInput) ([]ResultRow, error) {
	result, err := GetIndexWithLogs(ctx, input)
	if err != nil {
		return nil, err
	}

	return result.Rows, nil
}

// GetIndexChangeWithLogs executes get_index_change and returns full result including logs
func GetIndexChangeWithLogs(ctx context.Context, input GetIndexChangeInput) (*GetDataResult, error) {
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

	// Set default use_cache to false if not specified
	useCache := false
	if input.UseCache != nil {
		useCache = *input.UseCache
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
		useCache,
	}, func(row *common.Row) error {
		// Convert the row values to []any
		values := make([]any, len(row.Values))
		copy(values, row.Values)
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getIndexChange")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in getIndexChange")
	}

	processedRows, err := processResultRows(resultRows)
	if err != nil {
		return nil, err
	}

	// Parse cache information from logs
	cacheHit, cachedAt := parseCacheInfoFromLogs(r.Logs)

	return &GetDataResult{
		Rows:     processedRows,
		Logs:     r.Logs,
		CacheHit: cacheHit,
		CachedAt: cachedAt,
	}, nil
}

func GetIndexChange(ctx context.Context, input GetIndexChangeInput) ([]ResultRow, error) {
	result, err := GetIndexChangeWithLogs(ctx, input)
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

// GetFirstRecordWithLogs executes get_first_record and returns full result including logs
func GetFirstRecordWithLogs(ctx context.Context, input GetFirstRecordInput) (*GetDataResult, error) {
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

	// Set default use_cache to false if not specified
	useCache := false
	if input.UseCache != nil {
		useCache = *input.UseCache
	}

	var resultRows [][]any
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_first_record", []any{
		input.StreamLocator.DataProvider.Address(),
		input.StreamLocator.StreamId.String(),
		input.AfterTime,
		input.FrozenAt,
		useCache,
	}, func(row *common.Row) error {
		// Convert the row values to []any
		values := make([]any, len(row.Values))
		copy(values, row.Values)
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in getFirstRecord")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in getFirstRecord")
	}

	processedRows, err := processResultRows(resultRows)
	if err != nil {
		return nil, err
	}

	// Parse cache information from logs
	cacheHit, cachedAt := parseCacheInfoFromLogs(r.Logs)

	return &GetDataResult{
		Rows:     processedRows,
		Logs:     r.Logs,
		CacheHit: cacheHit,
		CachedAt: cachedAt,
	}, nil
}

func GetFirstRecord(ctx context.Context, input GetFirstRecordInput) ([]ResultRow, error) {
	result, err := GetFirstRecordWithLogs(ctx, input)
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
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
		copy(values, row.Values)
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
		copy(values, row.Values)
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
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "filter_streams_by_existence", []any{
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
	BlockHeight  int
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
		input.BlockHeight,
	}, func(row *common.Row) error {
		values := make([]any, len(row.Values))
		copy(values, row.Values)
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

type GetDatabaseSizeInput struct {
	Platform *kwilTesting.Platform
	Locator  trufTypes.StreamLocator
	Height   int64
}

func GetDatabaseSize(ctx context.Context, input GetDatabaseSizeInput) (int64, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return 0, errors.Wrap(err, "failed to create Ethereum address from deployer bytes")
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
	var databaseSize *int64
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_database_size", []any{}, func(row *common.Row) error {
		databaseSize = safe(row.Values[0], nil, int64PtrConverter)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if r.Error != nil {
		return 0, errors.Wrap(r.Error, "error in get_database_size")
	}

	return *databaseSize, nil
}

// ListTaxonomiesByHeight executes list_taxonomies_by_height action
func ListTaxonomiesByHeight(ctx context.Context, input ListTaxonomiesByHeightInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in ListTaxonomiesByHeight.NewEthereumAddressFromBytes")
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

	var resultRows [][]any
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "list_taxonomies_by_height", []any{
		input.FromHeight,
		input.ToHeight,
		input.Limit,
		input.Offset,
		input.LatestOnly,
	}, func(row *common.Row) error {
		values := make([]any, len(row.Values))
		copy(values, row.Values)
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in ListTaxonomiesByHeight.Call")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in ListTaxonomiesByHeight.Call")
	}

	return processResultRows(resultRows)
}

// GetTaxonomiesForStreams executes get_taxonomies_for_streams action
func GetTaxonomiesForStreams(ctx context.Context, input GetTaxonomiesForStreamsInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in GetTaxonomiesForStreams.NewEthereumAddressFromBytes")
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

	var resultRows [][]any
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "get_taxonomies_for_streams", []any{
		input.DataProviders,
		input.StreamIds,
		input.LatestOnly,
	}, func(row *common.Row) error {
		values := make([]any, len(row.Values))
		copy(values, row.Values)
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in GetTaxonomiesForStreams.Call")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in GetTaxonomiesForStreams.Call")
	}

	return processResultRows(resultRows)
}

// ListMetadataByHeight executes list_metadata_by_height action
func ListMetadataByHeight(ctx context.Context, input ListMetadataByHeightInput) ([]ResultRow, error) {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return nil, errors.Wrap(err, "error in ListMetadataByHeight.NewEthereumAddressFromBytes")
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

	var resultRows [][]any
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "list_metadata_by_height", []any{
		input.Key,
		input.Value,
		input.FromHeight,
		input.ToHeight,
		input.Limit,
		input.Offset,
	}, func(row *common.Row) error {
		values := make([]any, len(row.Values))
		copy(values, row.Values)
		resultRows = append(resultRows, values)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error in ListMetadataByHeight.Call")
	}
	if r.Error != nil {
		return nil, errors.Wrap(r.Error, "error in ListMetadataByHeight.Call")
	}

	return processResultRows(resultRows)
}
