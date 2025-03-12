package setup

import (
	"context"
	"fmt"
	"strconv"

	"github.com/kwilteam/kwil-db/common"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	testtable "github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

type ComposedStreamDefinition struct {
	StreamLocator       types.StreamLocator
	TaxonomyDefinitions types.Taxonomy
}

type SetupComposedAndPrimitivesInput struct {
	ComposedStreamDefinition ComposedStreamDefinition
	PrimitiveStreamsWithData []PrimitiveStreamWithData
	Platform                 *kwilTesting.Platform
	Height                   int64
}

func setupComposedAndPrimitives(ctx context.Context, input SetupComposedAndPrimitivesInput) error {
	// Create composed stream
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: input.Height},
		Signer:       input.ComposedStreamDefinition.StreamLocator.DataProvider.Bytes(),
		Caller:       input.ComposedStreamDefinition.StreamLocator.DataProvider.Address(),
		TxID:         input.Platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	// Create the composed stream using create_stream action
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "create_stream", []any{
		input.ComposedStreamDefinition.StreamLocator.StreamId.String(),
		"composed",
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error creating composed stream")
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error creating composed stream")
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

	dataProviders := []string{}
	streamIds := []string{}
	weights := []string{}
	for _, item := range input.ComposedStreamDefinition.TaxonomyDefinitions.TaxonomyItems {
		dataProviders = append(dataProviders, item.ChildStream.DataProvider.Address())
		streamIds = append(streamIds, item.ChildStream.StreamId.String())
		weights = append(weights, strconv.FormatFloat(item.Weight, 'f', -1, 64))
	}

	// Set taxonomy for composed stream
	if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
		Platform:      input.Platform,
		StreamLocator: input.ComposedStreamDefinition.StreamLocator,
		DataProviders: dataProviders,
		StreamIds:     streamIds,
		Weights:       weights,
		StartTime:     0,
	}); err != nil {
		return errors.Wrap(err, "error setting taxonomy for composed stream")
	}

	return nil
}

type MarkdownComposedSetupInput struct {
	Platform     *kwilTesting.Platform
	StreamId     util.StreamId
	MarkdownData string
	// optional. If not provided, each will have a weight of 1
	Weights []string
	Height  int64
}

// we expect to parse tables such as:
// markdownData:
// | event_time | stream 1 | stream 2 | stream 3 |
// | ---------- | -------- | -------- | -------- |
// | 1          | 1        | 2        |          |
// | 2          |          |          |          |
// | 3          | 3        | 4        | 5        |
func parseComposedMarkdownSetup(input MarkdownComposedSetupInput) (SetupComposedAndPrimitivesInput, error) {
	table, err := testtable.TableFromMarkdown(input.MarkdownData)
	if err != nil {
		return SetupComposedAndPrimitivesInput{}, err
	}

	// check if the first header is "event_time"
	if table.Headers[0] != "event_time" {
		return SetupComposedAndPrimitivesInput{}, fmt.Errorf("first header is not event_time")
	}

	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return SetupComposedAndPrimitivesInput{}, err
	}

	composedStreamLocator := types.StreamLocator{
		StreamId:     util.GenerateStreamId(input.StreamId.String()),
		DataProvider: deployer,
	}

	primitiveStreams := []PrimitiveStreamWithData{}
	for _, header := range table.Headers {
		if header == "event_time" {
			continue
		}
		streamId := util.GenerateStreamId(header)
		primitiveStreams = append(primitiveStreams, PrimitiveStreamWithData{
			PrimitiveStreamDefinition: PrimitiveStreamDefinition{
				StreamLocator: types.StreamLocator{
					StreamId:     streamId,
					DataProvider: deployer,
				},
			},
			Data: []InsertRecordInput{},
		})
	}

	for _, row := range table.Rows {
		eventTime := row[0]
		eventTimeInt, err := strconv.ParseInt(eventTime, 10, 64)
		if err != nil {
			return SetupComposedAndPrimitivesInput{}, err
		}
		for i, primitive := range row[1:] {

			if primitive == "" {
				continue
			}
			primitiveFloat, err := strconv.ParseFloat(primitive, 64)
			if err != nil {
				return SetupComposedAndPrimitivesInput{}, err
			}
			primitiveStreams[i].Data = append(primitiveStreams[i].Data, InsertRecordInput{
				EventTime: eventTimeInt,
				Value:     primitiveFloat,
			})
		}
	}

	composedStream := ComposedStreamDefinition{
		StreamLocator:       composedStreamLocator,
		TaxonomyDefinitions: types.Taxonomy{},
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
		composedStream.TaxonomyDefinitions.TaxonomyItems = append(composedStream.TaxonomyDefinitions.TaxonomyItems, types.TaxonomyItem{
			ChildStream: types.StreamLocator{
				StreamId:     primitiveStream.StreamLocator.StreamId,
				DataProvider: primitiveStream.StreamLocator.DataProvider,
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

type SetupComposedStreamInput struct {
	Platform *kwilTesting.Platform
	StreamId util.StreamId
	Height   int64
}

// SetupComposedStream sets up a composed stream
func SetupComposedStream(ctx context.Context, input SetupComposedStreamInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating ethereum address from bytes")
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

	// Create the composed stream using create_stream action
	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "create_stream", []any{
		input.StreamId.String(),
		"composed",
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error creating composed stream")
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error creating composed stream")
	}

	return nil
}
