package setup

// import (
// 	"context"
// 	"fmt"
// 	"strconv"

// 	"github.com/kwilteam/kwil-db/common"
// 	"github.com/kwilteam/kwil-db/core/utils"
// 	"github.com/kwilteam/kwil-db/parse"
// 	kwilTesting "github.com/kwilteam/kwil-db/testing"
// 	"github.com/pkg/errors"
// 	testdate "github.com/trufnetwork/node/tests/streams/tests/utils/date"
// 	testtable "github.com/trufnetwork/node/tests/streams/tests/utils/table"
// 	"github.com/trufnetwork/sdk-go/core/types"
// 	"github.com/trufnetwork/sdk-go/core/util"
// )

// type ComposedStreamDefinition struct {
// 	StreamLocator       types.StreamLocator
// 	TaxonomyDefinitions types.Taxonomy
// }

// type SetupComposedAndPrimitivesInput struct {
// 	ComposedStreamDefinition ComposedStreamDefinition
// 	PrimitiveStreamsWithData []PrimitiveStreamWithData
// 	Platform                 *kwilTesting.Platform
// 	Height                   int64
// }

// func setupComposedAndPrimitives(ctx context.Context, input SetupComposedAndPrimitivesInput) error {
// 	// Create composed stream
// 	composedDBID := utils.GenerateDBID(
// 		input.ComposedStreamDefinition.StreamLocator.StreamId.String(),
// 		input.ComposedStreamDefinition.StreamLocator.DataProvider.Bytes(),
// 	)
// 	composedSchema, err := parse.Parse(contracts.ComposedStreamContent)
// 	if err != nil {
// 		return errors.Wrap(err, "error parsing composed stream content")
// 	}
// 	composedSchema.Name = input.ComposedStreamDefinition.StreamLocator.StreamId.String()

// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: input.Height},
// 		Signer:       input.ComposedStreamDefinition.StreamLocator.DataProvider.Bytes(),
// 		Caller:       input.ComposedStreamDefinition.StreamLocator.DataProvider.Address(),
// 		TxID:         input.Platform.Txid(),
// 	}

// 	if err := input.Platform.Engine.CreateDataset(txContext, input.Platform.DB, composedSchema); err != nil {
// 		return errors.Wrap(err, "error creating composed dataset")
// 	}

// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return errors.Wrap(err, "error creating composed dataset")
// 	}

// 	if err := initializeContract(ctx, InitializeContractInput{
// 		Platform: input.Platform,
// 		Deployer: deployer,
// 		Dbid:     composedDBID,
// 		Height:   input.Height,
// 	}); err != nil {
// 		return errors.Wrap(err, "error initializing composed stream")
// 	}

// 	// Set taxonomy for composed stream
// 	if err := setTaxonomy(ctx, SetTaxonomyInput{
// 		Platform:       input.Platform,
// 		composedStream: input.ComposedStreamDefinition,
// 	}); err != nil {
// 		return errors.Wrap(err, "error setting taxonomy for composed stream")
// 	}

// 	// Deploy and initialize primitive streams
// 	for _, primitiveStream := range input.PrimitiveStreamsWithData {
// 		if err := setupPrimitive(ctx, SetupPrimitiveInput{
// 			Platform:                input.Platform,
// 			Height:                  input.Height,
// 			PrimitiveStreamWithData: primitiveStream,
// 		}); err != nil {
// 			return errors.Wrap(err, "error setting up primitive stream")
// 		}
// 	}

// 	return nil
// }

// type MarkdownComposedSetupInput struct {
// 	Platform     *kwilTesting.Platform
// 	StreamId     string
// 	MarkdownData string
// 	// optional. If not provided, each will have a weight of 1
// 	Weights []string
// 	Height  int64
// }

// // we expect to parse tables such as:
// // markdownData:
// // | date       | stream 1 | stream 2 | stream 3 |
// // | ---------- | -------- | -------- | -------- |
// // | 2024-08-29 | 1        | 2        |          |
// // | 2024-08-30 |          |          |          |
// // | 2024-08-31 | 3        | 4        | 5        |
// func parseComposedMarkdownSetup(input MarkdownComposedSetupInput) (SetupComposedAndPrimitivesInput, error) {
// 	table, err := testtable.TableFromMarkdown(input.MarkdownData)
// 	if err != nil {
// 		return SetupComposedAndPrimitivesInput{}, err
// 	}

// 	// check if the first header is "date"
// 	if table.Headers[0] != "date" {
// 		return SetupComposedAndPrimitivesInput{}, fmt.Errorf("first header is not date")
// 	}

// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return SetupComposedAndPrimitivesInput{}, err
// 	}

// 	composedStreamLocator := types.StreamLocator{
// 		StreamId:     util.GenerateStreamId(input.StreamId),
// 		DataProvider: deployer,
// 	}

// 	primitiveStreams := []PrimitiveStreamWithData{}
// 	for _, header := range table.Headers {
// 		if header == "date" {
// 			continue
// 		}
// 		streamId := util.GenerateStreamId(header)
// 		primitiveStreams = append(primitiveStreams, PrimitiveStreamWithData{
// 			PrimitiveStreamDefinition: PrimitiveStreamDefinition{
// 				StreamLocator: types.StreamLocator{
// 					StreamId:     streamId,
// 					DataProvider: deployer,
// 				},
// 			},
// 			Data: []InsertRecordInput{},
// 		})
// 	}

// 	for _, row := range table.Rows {
// 		date := row[0]
// 		for i, primitive := range row[1:] {
// 			if primitive == "" {
// 				continue
// 			}
// 			primitiveStreams[i].Data = append(primitiveStreams[i].Data, InsertRecordInput{
// 				DateValue: testdate.MustParseDate(date),
// 				Value:     primitive,
// 			})
// 		}
// 	}

// 	composedStream := ComposedStreamDefinition{
// 		StreamLocator:       composedStreamLocator,
// 		TaxonomyDefinitions: types.Taxonomy{},
// 	}

// 	var weights []string
// 	if input.Weights != nil {
// 		weights = input.Weights
// 	} else {
// 		weights = make([]string, len(primitiveStreams))
// 		for i := range weights {
// 			weights[i] = "1"
// 		}
// 	}

// 	for i, primitiveStream := range primitiveStreams {
// 		weight, err := strconv.ParseFloat(weights[i], 64)
// 		if err != nil {
// 			return SetupComposedAndPrimitivesInput{}, err
// 		}
// 		composedStream.TaxonomyDefinitions.TaxonomyItems = append(composedStream.TaxonomyDefinitions.TaxonomyItems, types.TaxonomyItem{
// 			ChildStream: types.StreamLocator{
// 				StreamId:     primitiveStream.StreamLocator.StreamId,
// 				DataProvider: primitiveStream.StreamLocator.DataProvider,
// 			},
// 			Weight: weight,
// 		})
// 	}

// 	return SetupComposedAndPrimitivesInput{
// 		ComposedStreamDefinition: composedStream,
// 		PrimitiveStreamsWithData: primitiveStreams,
// 		Height:                   input.Height,
// 		Platform:                 input.Platform,
// 	}, nil
// }

// func SetupComposedFromMarkdown(ctx context.Context, input MarkdownComposedSetupInput) error {
// 	setup, err := parseComposedMarkdownSetup(input)
// 	if err != nil {
// 		return err
// 	}
// 	return setupComposedAndPrimitives(ctx, setup)
// }

// type SetTaxonomyInput struct {
// 	Platform       *kwilTesting.Platform
// 	composedStream ComposedStreamDefinition
// }

// func setTaxonomy(ctx context.Context, input SetTaxonomyInput) error {
// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return errors.Wrap(err, "error creating composed dataset")
// 	}

// 	primitiveStreamStrings := []string{}
// 	dataProviderStrings := []string{}
// 	weightStrings := []string{}
// 	for _, item := range input.composedStream.TaxonomyDefinitions.TaxonomyItems {
// 		primitiveStreamStrings = append(primitiveStreamStrings, item.ChildStream.StreamId.String())
// 		dataProviderStrings = append(dataProviderStrings, item.ChildStream.DataProvider.Address())
// 		// should be formatted as 0.000000000000000000 (18 decimal places)
// 		weightStrings = append(weightStrings, fmt.Sprintf("%.18f", item.Weight))
// 	}

// 	var startDate string
// 	if input.composedStream.TaxonomyDefinitions.StartDate != nil {
// 		startDate = input.composedStream.TaxonomyDefinitions.StartDate.String()
// 	}

// 	dbid := utils.GenerateDBID(input.composedStream.StreamLocator.StreamId.String(), input.composedStream.StreamLocator.DataProvider.Bytes())

// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: 0},
// 		Signer:       input.Platform.Deployer,
// 		Caller:       deployer.Address(),
// 		TxID:         input.Platform.Txid(),
// 	}

// 	_, err = input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
// 		Procedure: "set_taxonomy",
// 		Dataset:   dbid,
// 		Args: []any{
// 			dataProviderStrings,
// 			primitiveStreamStrings,
// 			weightStrings,
// 			startDate,
// 		},
// 	})
// 	return err
// }

// type SetupComposedStreamInput struct {
// 	Platform *kwilTesting.Platform
// 	StreamId util.StreamId
// 	Height   int64
// }

// // SetupComposedStream sets up a composed stream
// func SetupComposedStream(ctx context.Context, input SetupComposedStreamInput) error {
// 	composedSchema, err := parse.Parse(contracts.ComposedStreamContent)
// 	if err != nil {
// 		return errors.Wrap(err, "error parsing composed stream content")
// 	}
// 	composedSchema.Name = input.StreamId.String()

// 	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
// 	if err != nil {
// 		return errors.Wrap(err, "error creating ethereum address from bytes")
// 	}

// 	txContext := &common.TxContext{
// 		Ctx:          ctx,
// 		BlockContext: &common.BlockContext{Height: input.Height},
// 		Signer:       input.Platform.Deployer,
// 		Caller:       deployer.Address(),
// 		TxID:         input.Platform.Txid(),
// 	}

// 	if err := input.Platform.Engine.CreateDataset(txContext, input.Platform.DB, composedSchema); err != nil {
// 		return errors.Wrap(err, "error creating composed dataset")
// 	}

// 	composedDBID := utils.GenerateDBID(input.StreamId.String(), input.Platform.Deployer)

// 	if err := initializeContract(ctx, InitializeContractInput{
// 		Platform: input.Platform,
// 		Deployer: deployer,
// 		Dbid:     composedDBID,
// 		Height:   input.Height,
// 	}); err != nil {
// 		return errors.Wrap(err, "error initializing composed stream")
// 	}

// 	return nil
// }
