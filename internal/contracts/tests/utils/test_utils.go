package testutils

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-sql/civil"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/truflation/tsn-db/internal/contracts"
	"github.com/truflation/tsn-sdk/core/types"
	"github.com/truflation/tsn-sdk/core/util"
)

type InsertRecordInput struct {
	DateValue civil.Date
	Value     string
}

type ComposedStreamDefinition struct {
	StreamId            util.StreamId
	TaxonomyDefinitions []types.TaxonomyItem
}

type PrimitiveStreamDefinition struct {
	StreamId util.StreamId
}

type PrimitiveStreamWithData struct {
	PrimitiveStreamDefinition
	Data []InsertRecordInput
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

type SetupPrimitiveInput struct {
	Platform                *kwilTesting.Platform
	Height                  int64
	PrimitiveStreamWithData PrimitiveStreamWithData
}

func setupPrimitive(ctx context.Context, setupInput SetupPrimitiveInput) error {
	primitiveSchema, err := parse.Parse(contracts.PrimitiveStreamContent)
	if err != nil {
		return errors.Wrap(err, "error parsing primitive stream content")
	}
	primitiveSchema.Name = setupInput.PrimitiveStreamWithData.StreamId.String()

	if err := setupInput.Platform.Engine.CreateDataset(ctx, setupInput.Platform.DB, primitiveSchema, &common.TransactionData{
		Signer: setupInput.Platform.Deployer,
		TxID:   setupInput.Platform.Txid(),
		Height: setupInput.Height,
	}); err != nil {
		return errors.Wrap(err, "error creating primitive dataset")
	}

	dbid := utils.GenerateDBID(setupInput.PrimitiveStreamWithData.StreamId.String(), setupInput.Platform.Deployer)
	if err := initializeContract(ctx, setupInput.Platform, dbid); err != nil {
		return errors.Wrap(err, "error initializing primitive stream")
	}

	if err := insertPrimitiveData(ctx, InsertPrimitiveDataInput{
		Platform:        setupInput.Platform,
		primitiveStream: setupInput.PrimitiveStreamWithData,
		height:          setupInput.Height,
	}); err != nil {
		return errors.Wrap(err, "error inserting primitive data")
	}

	return nil
}

func initializeContract(ctx context.Context, platform *kwilTesting.Platform, dbid string) error {
	_, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "init",
		Dataset:   dbid,
		Args:      []any{},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 1,
		},
	})
	return err
}

func insertTestData(ctx context.Context, platform *kwilTesting.Platform, dbid string, testData []struct {
	date  string
	value string
}) error {
	for _, data := range testData {
		if _, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
			Procedure: "insert_record",
			Dataset:   dbid,
			Args:      []any{data.date, data.value},
			TransactionData: common.TransactionData{
				Signer: platform.Deployer,
				TxID:   platform.Txid(),
				Height: 0,
			},
		}); err != nil {
			return err
		}
	}
	return nil
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

type InsertPrimitiveDataInput struct {
	Platform        *kwilTesting.Platform
	primitiveStream PrimitiveStreamWithData
	height          int64
}

func insertPrimitiveData(ctx context.Context, input InsertPrimitiveDataInput) error {

	args := [][]any{}
	for _, data := range input.primitiveStream.Data {
		args = append(args, []any{data.DateValue, data.Value})
	}

	dbid := utils.GenerateDBID(input.primitiveStream.StreamId.String(), input.Platform.Deployer)

	txid := input.Platform.Txid()

	for _, arg := range args {
		_, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
			Procedure: "insert_record",
			Dataset:   dbid,
			Args:      arg,
			TransactionData: common.TransactionData{
				Signer: input.Platform.Deployer,
				TxID:   txid,
				Height: input.height,
			},
		})
		if err != nil {
			return err
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
	table, err := TableFromMarkdown(input.MarkdownData)
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
				DateValue: MustParseDate(date),
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

type MarkdownPrimitiveSetupInput struct {
	Platform            *kwilTesting.Platform
	Height              int64
	PrimitiveStreamName string
	MarkdownData        string
}

// we expect to parse tables such as:
// markdownData:
// | date       | value |
// | ---------- | ----- |
// | 2024-08-29 | 1     |
// | 2024-08-30 | 2     |
// | 2024-08-31 | 3     |
func parsePrimitiveMarkdownSetup(input MarkdownPrimitiveSetupInput) (SetupPrimitiveInput, error) {
	table, err := TableFromMarkdown(input.MarkdownData)
	if err != nil {
		return SetupPrimitiveInput{}, err
	}

	primitiveStream := PrimitiveStreamWithData{
		PrimitiveStreamDefinition: PrimitiveStreamDefinition{
			StreamId: util.GenerateStreamId(input.PrimitiveStreamName),
		},
		Data: []InsertRecordInput{},
	}

	for _, row := range table.Rows {
		date := row[0]
		value := row[1]
		// if value is empty, we don't insert it
		if value == "" {
			continue
		}
		primitiveStream.Data = append(primitiveStream.Data, InsertRecordInput{
			DateValue: MustParseDate(date),
			Value:     value,
		})
	}

	return SetupPrimitiveInput{
		Platform:                input.Platform,
		Height:                  input.Height,
		PrimitiveStreamWithData: primitiveStream,
	}, nil
}

func SetupComposedFromMarkdown(ctx context.Context, input MarkdownComposedSetupInput) error {
	setup, err := parseComposedMarkdownSetup(input)
	if err != nil {
		return err
	}
	return setupComposedAndPrimitives(ctx, setup)
}

func SetupPrimitiveFromMarkdown(ctx context.Context, input MarkdownPrimitiveSetupInput) error {
	setup, err := parsePrimitiveMarkdownSetup(input)
	if err != nil {
		return err
	}
	return setupPrimitive(ctx, setup)
}

func MustParseDate(date string) civil.Date {
	parsed, err := time.Parse("2006-01-02", date)
	if err != nil {
		panic(err)
	}
	return civil.DateOf(parsed)
}
