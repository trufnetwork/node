package procedure

// import (
// 	"context"
// 	"fmt"

// 	"github.com/trufnetwork/sdk-go/core/util"

// 	"github.com/kwilteam/kwil-db/common"
// 	kwilTesting "github.com/kwilteam/kwil-db/testing"
// 	"github.com/pkg/errors"
// )

// func GetRecord(ctx context.Context, input GetRecordInput) ([]ResultRow, error) {
// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in getRecord")
// 	}

// 	txContext := &common.TxContext{
// 		Ctx: ctx,
// 		BlockContext: &common.BlockContext{
// 			Height: input.Height,
// 		},
// 		TxID:   input.Platform.Txid(),
// 		Signer: input.Platform.Deployer,
// 		Caller: deployer.Address(),
// 	}

// 	result, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
// 		Procedure: "get_record",
// 		Dataset:   input.DBID,
// 		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt},
// 	})
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in getRecord")
// 	}

// 	return processResultRows(result.Rows)
// }

// func GetIndex(ctx context.Context, input GetIndexInput) ([]ResultRow, error) {
// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in getIndex")
// 	}

// 	txContext := &common.TxContext{
// 		Ctx: ctx,
// 		BlockContext: &common.BlockContext{
// 			Height: input.Height,
// 		},
// 		TxID:   input.Platform.Txid(),
// 		Signer: input.Platform.Deployer,
// 		Caller: deployer.Address(),
// 	}

// 	result, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
// 		Procedure: "get_index",
// 		Dataset:   input.DBID,
// 		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt, input.BaseDate},
// 	})
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in getIndex")
// 	}

// 	return processResultRows(result.Rows)
// }

// func GetIndexChange(ctx context.Context, input GetIndexChangeInput) ([]ResultRow, error) {
// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in getIndexChange")
// 	}

// 	txContext := &common.TxContext{
// 		Ctx: ctx,
// 		BlockContext: &common.BlockContext{
// 			Height: input.Height,
// 		},
// 		TxID:   input.Platform.Txid(),
// 		Signer: input.Platform.Deployer,
// 		Caller: deployer.Address(),
// 	}

// 	result, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
// 		Procedure: "get_index_change",
// 		Dataset:   input.DBID,
// 		Args:      []any{input.DateFrom, input.DateTo, input.FrozenAt, input.BaseDate, input.Interval},
// 	})
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in getIndexChange")
// 	}

// 	return processResultRows(result.Rows)
// }

// func GetFirstRecord(ctx context.Context, input GetFirstRecordInput) ([]ResultRow, error) {
// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in getFirstRecord")
// 	}

// 	txContext := &common.TxContext{
// 		Ctx: ctx,
// 		BlockContext: &common.BlockContext{
// 			Height: input.Height,
// 		},
// 		TxID:   input.Platform.Txid(),
// 		Signer: input.Platform.Deployer,
// 		Caller: deployer.Address(),
// 	}

// 	result, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
// 		Procedure: "get_first_record",
// 		Dataset:   input.DBID,
// 		Args:      []any{input.AfterDate, input.FrozenAt},
// 	})
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in getFirstRecord")
// 	}

// 	return processResultRows(result.Rows)
// }

// func SetMetadata(ctx context.Context, input SetMetadataInput) error {
// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return errors.Wrap(err, "error in setMetadata")
// 	}

// 	txContext := &common.TxContext{
// 		Ctx: ctx,
// 		BlockContext: &common.BlockContext{
// 			Height: input.Height,
// 		},
// 		TxID:   input.Platform.Txid(),
// 		Signer: input.Platform.Deployer,
// 		Caller: deployer.Address(),
// 	}

// 	_, err = input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
// 		Procedure: "insert_metadata",
// 		Dataset:   input.DBID,
// 		Args:      []any{input.Key, input.Value, input.ValType},
// 	})
// 	if err != nil {
// 		return errors.Wrap(err, "error in setMetadata")
// 	}

// 	return nil
// }

// func processResultRows(rows [][]any) ([]ResultRow, error) {
// 	resultRows := make([]ResultRow, len(rows))
// 	for i, row := range rows {
// 		resultRow := ResultRow{}
// 		for _, value := range row {
// 			resultRow = append(resultRow, fmt.Sprintf("%v", value))
// 		}
// 		resultRows[i] = resultRow
// 	}

// 	return resultRows, nil
// }

// // WithSigner returns a new platform with the given signer, but doesn't mutate the original platform
// func WithSigner(platform *kwilTesting.Platform, signer []byte) *kwilTesting.Platform {
// 	newPlatform := *platform // create a copy of the original platform
// 	newPlatform.Deployer = signer
// 	return &newPlatform
// }

// type DescribeTaxonomiesInput struct {
// 	Platform      *kwilTesting.Platform
// 	DBID          string
// 	LatestVersion bool
// }

// // DescribeTaxonomies is a helper function to describe taxonomies of a composed stream
// func DescribeTaxonomies(ctx context.Context, input DescribeTaxonomiesInput) ([]ResultRow, error) {
// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in DescribeTaxonomies.NewEthereumAddressFromBytes")
// 	}

// 	txContext := &common.TxContext{
// 		BlockContext: &common.BlockContext{Height: 0},
// 		Signer:       input.Platform.Deployer,
// 		Caller:       deployer.Address(),
// 		TxID:         input.Platform.Txid(),
// 		Ctx:          ctx,
// 	}

// 	result, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
// 		Procedure: "describe_taxonomies",
// 		Dataset:   input.DBID,
// 		Args:      []any{input.LatestVersion},
// 	})
// 	if err != nil {
// 		return nil, errors.Wrap(err, "error in DescribeTaxonomies.Procedure")
// 	}

// 	return processResultRows(result.Rows)
// }

// // SetTaxonomy sets the taxonomy for a composed stream with optional start date
// func SetTaxonomy(ctx context.Context, input SetTaxonomyInput) error {
// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return errors.Wrap(err, "error in SetTaxonomy")
// 	}

// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 0},
// 		Signer:       input.Platform.Deployer,
// 		Caller:       deployer.Address(),
// 		TxID:         input.Platform.Txid(),
// 	}

// 	_, err = input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
// 		Procedure: "set_taxonomy",
// 		Dataset:   input.DBID,
// 		Args:      []any{input.DataProviders, input.StreamIds, input.Weights, input.StartDate},
// 	})
// 	if err != nil {
// 		return errors.Wrap(err, "error in SetTaxonomy")
// 	}

// 	return nil
// }
