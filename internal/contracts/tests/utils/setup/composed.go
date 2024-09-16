package setup

import (
	"context"
	"fmt"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/truflation/tsn-db/internal/contracts"
	testdate "github.com/truflation/tsn-db/internal/contracts/tests/utils/date"
	testtable "github.com/truflation/tsn-db/internal/contracts/tests/utils/table"
	"github.com/truflation/tsn-sdk/core/types"
	"github.com/truflation/tsn-sdk/core/util"
	"strconv"
)

type ComposedStreamDefinition struct {
	StreamId            util.StreamId
	TaxonomyDefinitions []types.TaxonomyItem
}

type SetupComposedAndPrimitivesInput struct {
	ComposedStreamDefinition ComposedStreamDefinition
	PrimitiveStreamsWithData []PrimitiveStreamWithData
	Platform                 *kwilTesting.Platform
	Height                   int64
}

func setupComposedAndPrimitives(ctx context.Context, input SetupComposedAndPrimitivesInput) error {
	// Create composed stream
	composedDBID := utils.GenerateDBID(input.ComposedStreamDefinition.StreamId.String(), input.Platform.Deployer)
	composedSchema, err := parse.Parse(contracts.ComposedStreamContent)
	if err != nil {
		return errors.Wrap(err, "error parsing composed stream content")
	}
	composedSchema.Name = input.ComposedStreamDefinition.StreamId.String()

	if err := input.Platform.Engine.CreateDataset(ctx, input.Platform.DB, composedSchema, &common.TransactionData{
		Signer: input.Platform.Deployer,
		TxID:   input.Platform.Txid(),
		Height: input.Height,
	}); err != nil {
		return errors.Wrap(err, "error creating composed dataset")
	}

	if err := initializeContract(ctx, input.Platform, composedDBID); err != nil {
		return errors.Wrap(err, "error initializing composed stream")
	}

	// Set taxonomy for composed stream
	if err := setTaxonomy(ctx, SetTaxonomyInput{
		Platform:       input.Platform,
		composedStream: input.ComposedStreamDefinition,
	}); err != nil {
		return errors.Wrap(err, "error setting taxonomy for composed stream")
	}

	// Deploy and initialize primitive streams
	for _, primitiveStream := range input.PrimitiveStreamsWithData {
		if err := setupPrimitive(ctx, SetupPrimitiveInput{
			Platform:                input.Platform,
			Height:                  input.Height,
			PrimitiveStreamWithData: primitiveStream,
		}); err != nil {
			return errors.Wrap(err, "error setting up primitive stream")
		}
	}

	return nil
}

type MarkdownComposedSetupInput struct {
	Platform           *kwilTesting.Platform
	ComposedStreamName string
	MarkdownData       string
	// optional. If not provided, each will have a weight of 1
	Weights []string
	Height  int64
}

// we expect to parse tables such as:
// markdownData:
// | date       | stream 1 | stream 2 | stream 3 |
// | ---------- | -------- | -------- | -------- |
// | 2024-08-29 | 1        | 2        |          |
// | 2024-08-30 |          |          |          |
// | 2024-08-31 | 3        | 4        | 5        |
func parseComposedMarkdownSetup(input MarkdownComposedSetupInput) (SetupComposedAndPrimitivesInput, error) {
	table, err := testtable.TableFromMarkdown(input.MarkdownData)
	if err != nil {
		return SetupComposedAndPrimitivesInput{}, err
	}

	// check if the first header is "date"
	if table.Headers[0] != "date" {
		return SetupComposedAndPrimitivesInput{}, fmt.Errorf("first header is not date")
	}

	primitiveStreams := []PrimitiveStreamWithData{}
	for _, header := range table.Headers {
		if header == "date" {
			continue
		}
		streamId := util.GenerateStreamId(header)
		primitiveStreams = append(primitiveStreams, PrimitiveStreamWithData{
			PrimitiveStreamDefinition: PrimitiveStreamDefinition{
				StreamId: streamId,
			},
			Data: []InsertRecordInput{},
		})
	}

	for _, row := range table.Rows {
		date := row[0]
		for i, primitive := range row[1:] {
			if primitive == "" {
				continue
			}
			primitiveStreams[i].Data = append(primitiveStreams[i].Data, InsertRecordInput{
				DateValue: testdate.MustParseDate(date),
				Value:     primitive,
			})
		}
	}

	composedStream := ComposedStreamDefinition{
		StreamId:            util.GenerateStreamId(input.ComposedStreamName),
		TaxonomyDefinitions: []types.TaxonomyItem{},
	}

	dataProvider, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return SetupComposedAndPrimitivesInput{}, err
	}

	var weights []string
	if input.Weights != nil {
		weights = input.Weights
	} else {
		weights = make([]string, len(primitiveStreams))
		for i := range weights {
			weights[i] = "1"
		}
	}

	for i, primitiveStream := range primitiveStreams {
		weight, err := strconv.ParseFloat(weights[i], 64)
		if err != nil {
			return SetupComposedAndPrimitivesInput{}, err
		}
		composedStream.TaxonomyDefinitions = append(composedStream.TaxonomyDefinitions, types.TaxonomyItem{
			ChildStream: types.StreamLocator{
				StreamId:     primitiveStream.StreamId,
				DataProvider: dataProvider,
			},
			Weight: weight,
		})
	}

	return SetupComposedAndPrimitivesInput{
		ComposedStreamDefinition: composedStream,
		PrimitiveStreamsWithData: primitiveStreams,
		Height:                   input.Height,
		Platform:                 input.Platform,
	}, nil
}

func SetupComposedFromMarkdown(ctx context.Context, input MarkdownComposedSetupInput) error {
	setup, err := parseComposedMarkdownSetup(input)
	if err != nil {
		return err
	}
	return setupComposedAndPrimitives(ctx, setup)
}

type SetTaxonomyInput struct {
	Platform       *kwilTesting.Platform
	composedStream ComposedStreamDefinition
}

func setTaxonomy(ctx context.Context, input SetTaxonomyInput) error {
	primitiveStreamStrings := []string{}
	dataProviderStrings := []string{}
	weightStrings := []string{}
	for _, item := range input.composedStream.TaxonomyDefinitions {
		primitiveStreamStrings = append(primitiveStreamStrings, item.ChildStream.StreamId.String())
		dataProviderStrings = append(dataProviderStrings, item.ChildStream.DataProvider.Address())
		// should be formatted as 0.000 (3 decimal places)
		weightStrings = append(weightStrings, fmt.Sprintf("%.3f", item.Weight))
	}

	dbid := utils.GenerateDBID(input.composedStream.StreamId.String(), input.Platform.Deployer)

	_, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
		Procedure: "set_taxonomy",
		Dataset:   dbid,
		Args: []any{
			dataProviderStrings,
			primitiveStreamStrings,
			weightStrings,
		},
		TransactionData: common.TransactionData{
			Signer: input.Platform.Deployer,
			TxID:   input.Platform.Txid(),
			Height: 0,
		},
	})
	return err
}