package setup

import (
	"context"
	"strconv"

	"github.com/pkg/errors"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	testtable "github.com/trufnetwork/node/tests/streams/utils/table"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

type InsertRecordInput struct {
	EventTime int64   `json:"event_time"`
	Value     float64 `json:"value"`
}

type PrimitiveStreamDefinition struct {
	StreamLocator types.StreamLocator
}

type PrimitiveStreamWithData struct {
	PrimitiveStreamDefinition
	Data []InsertRecordInput
}

type MarkdownPrimitiveSetupInput struct {
	Platform     *kwilTesting.Platform
	StreamId     util.StreamId
	Height       int64
	MarkdownData string
}

type SetupPrimitiveInput struct {
	Platform                *kwilTesting.Platform
	Height                  int64
	PrimitiveStreamWithData PrimitiveStreamWithData
}

func SetupPrimitive(ctx context.Context, setupInput SetupPrimitiveInput) error {
	// Create the stream using our helper function that handles role management
	err := CreateStream(ctx, setupInput.Platform, StreamInfo{
		Locator: setupInput.PrimitiveStreamWithData.StreamLocator,
		Type:    ContractTypePrimitive,
	})
	if err != nil {
		return errors.Wrap(err, "error in setupPrimitive.CreateStream")
	}

	// Insert the data
	if err := insertPrimitiveData(ctx, InsertPrimitiveDataInput{
		Platform:        setupInput.Platform,
		PrimitiveStream: setupInput.PrimitiveStreamWithData,
		Height:          setupInput.Height,
	}); err != nil {
		return errors.Wrap(err, "error inserting primitive data")
	}

	return nil
}

// we expect to parse tables such as:
// markdownData:
// | date       | value |
// | ---------- | ----- |
// | 2024-08-29 | 1     |
// | 2024-08-30 | 2     |
// | 2024-08-31 | 3     |
func parsePrimitiveMarkdownSetup(input MarkdownPrimitiveSetupInput) (SetupPrimitiveInput, error) {
	table, err := testtable.TableFromMarkdown(input.MarkdownData)
	if err != nil {
		return SetupPrimitiveInput{}, err
	}

	deployer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return SetupPrimitiveInput{}, errors.Wrap(err, "error in parsePrimitiveMarkdownSetup")
	}

	primitiveStream := PrimitiveStreamWithData{
		PrimitiveStreamDefinition: PrimitiveStreamDefinition{
			StreamLocator: types.StreamLocator{
				StreamId:     input.StreamId,
				DataProvider: deployer,
			},
		},
		Data: []InsertRecordInput{},
	}

	for _, row := range table.Rows {
		eventTime := row[0]
		value := row[1]
		// if value is empty, we don't insert it
		if value == "" {
			continue
		}
		eventTimeInt, err := strconv.ParseInt(eventTime, 10, 64)
		if err != nil {
			return SetupPrimitiveInput{}, err
		}
		valueFloat, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return SetupPrimitiveInput{}, err
		}
		primitiveStream.Data = append(primitiveStream.Data, InsertRecordInput{
			EventTime: eventTimeInt,
			Value:     valueFloat,
		})
	}

	return SetupPrimitiveInput{
		Platform:                input.Platform,
		Height:                  input.Height,
		PrimitiveStreamWithData: primitiveStream,
	}, nil
}

func SetupPrimitiveFromMarkdown(ctx context.Context, input MarkdownPrimitiveSetupInput) error {
	setup, err := parsePrimitiveMarkdownSetup(input)
	if err != nil {
		return err
	}
	return SetupPrimitive(ctx, setup)
}

type InsertMarkdownDataInput struct {
	Platform *kwilTesting.Platform
	Height   int64
	// we use locator instead because it could be a third party data provider
	StreamLocator types.StreamLocator
	MarkdownData  string
}

// InsertMarkdownPrimitiveData inserts data from a markdown table into a primitive stream
func InsertMarkdownPrimitiveData(ctx context.Context, input InsertMarkdownDataInput) error {
	table, err := testtable.TableFromMarkdown(input.MarkdownData)
	if err != nil {
		return err
	}

	txid := input.Platform.Txid()

	signer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error in InsertMarkdownPrimitiveData")
	}

	for _, row := range table.Rows {
		eventTime := row[0]
		value := row[1]
		if value == "" {
			continue
		}

		engineContext := newEthEngineContext(ctx, input.Platform, signer, input.Height)
		engineContext.TxContext.TxID = txid

		r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "insert_record", []any{
			input.StreamLocator.DataProvider.Address(),
			input.StreamLocator.StreamId.String(),
			eventTime,
			value,
		}, func(row *common.Row) error {
			return nil
		})
		if err != nil {
			return err
		}
		if r.Error != nil {
			return errors.Wrap(r.Error, "error in InsertMarkdownPrimitiveData")
		}
	}
	return nil
}

type InsertPrimitiveDataInput struct {
	Platform        *kwilTesting.Platform
	PrimitiveStream PrimitiveStreamWithData
	Height          int64
}

func insertPrimitiveData(ctx context.Context, input InsertPrimitiveDataInput) error {
	args := [][]any{}
	for _, data := range input.PrimitiveStream.Data {
		valueDecimal, err := kwilTypes.ParseDecimalExplicit(strconv.FormatFloat(data.Value, 'f', -1, 64), 36, 18)
		if err != nil {
			return errors.Wrap(err, "error in insertPrimitiveData")
		}
		args = append(args, []any{
			input.PrimitiveStream.StreamLocator.DataProvider.Address(),
			input.PrimitiveStream.StreamLocator.StreamId.String(),
			data.EventTime,
			valueDecimal,
		})
	}

	txid := input.Platform.Txid()

	deployer, err := util.NewEthereumAddressFromBytes(input.PrimitiveStream.StreamLocator.DataProvider.Bytes())
	if err != nil {
		return errors.Wrap(err, "error in insertPrimitiveData")
	}

	for _, arg := range args {
		engineContext := newEthEngineContext(ctx, input.Platform, deployer, input.Height)
		engineContext.TxContext.TxID = txid

		r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "insert_record", arg, func(row *common.Row) error {
			return nil
		})
		if err != nil {
			return err
		}
		if r.Error != nil {
			return errors.Wrap(r.Error, "error in insertPrimitiveData")
		}
	}
	return nil
}

// ExecuteInsertRecord executes the create_stream procedure
func ExecuteInsertRecord(ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, input InsertRecordInput, height int64) error {
	insertPrimitiveDataInput := InsertPrimitiveDataInput{
		Platform: platform,
		PrimitiveStream: PrimitiveStreamWithData{
			PrimitiveStreamDefinition: PrimitiveStreamDefinition{
				StreamLocator: locator,
			},
			Data: []InsertRecordInput{input},
		},
		Height: height,
	}

	return insertPrimitiveData(ctx, insertPrimitiveDataInput)
}

// InsertPrimitiveDataBatch calls the batch insertion action "insert_records" with arrays of parameters.
// This is now a wrapper around InsertPrimitiveDataMultiBatch for backward compatibility.
func InsertPrimitiveDataBatch(ctx context.Context, input InsertPrimitiveDataInput) error {
	// Convert single stream input to multi-stream format
	multiInput := InsertMultiPrimitiveDataInput{
		Platform: input.Platform,
		Height:   input.Height,
		Streams:  []PrimitiveStreamWithData{input.PrimitiveStream},
	}

	// Delegate to the multi-stream implementation
	return InsertPrimitiveDataMultiBatch(ctx, multiInput)
}

// InsertMultiPrimitiveDataInput allows batching records for multiple streams in one or few engine calls
type InsertMultiPrimitiveDataInput struct {
	Platform *kwilTesting.Platform
	Height   int64
	Streams  []PrimitiveStreamWithData
}

// InsertPrimitiveDataMultiBatch inserts records for multiple streams using the insert_records action.
// It groups records by data provider to minimize calls and ensure correct signing.
func InsertPrimitiveDataMultiBatch(ctx context.Context, input InsertMultiPrimitiveDataInput) error {
	if len(input.Streams) == 0 {
		return nil
	}

	// Group records by provider address
	type grouped struct {
		dataProviders []string
		streamIds     []string
		eventTimes    []int64
		values        []*kwilTypes.Decimal
	}

	byProvider := map[string]*grouped{}

	for _, ps := range input.Streams {
		provider := ps.StreamLocator.DataProvider.Address()
		g, ok := byProvider[provider]
		if !ok {
			g = &grouped{}
			byProvider[provider] = g
		}
		for _, rec := range ps.Data {
			dec, err := kwilTypes.ParseDecimalExplicit(strconv.FormatFloat(rec.Value, 'f', -1, 64), 36, 18)
			if err != nil {
				return errors.Wrap(err, "parse decimal in InsertPrimitiveDataMultiBatch")
			}
			g.dataProviders = append(g.dataProviders, provider)
			g.streamIds = append(g.streamIds, ps.StreamLocator.StreamId.String())
			g.eventTimes = append(g.eventTimes, rec.EventTime)
			g.values = append(g.values, dec)
		}
	}

	txid := input.Platform.Txid()

	// Execute one call per provider group with correct signer
	for provider, g := range byProvider {
		if len(g.dataProviders) != len(g.streamIds) || len(g.streamIds) != len(g.eventTimes) || len(g.eventTimes) != len(g.values) {
			return errors.Errorf("array length mismatch for provider %s: dp=%d sid=%d ts=%d val=%d",
				provider, len(g.dataProviders), len(g.streamIds), len(g.eventTimes), len(g.values))
		}
		signerAddr := util.Unsafe_NewEthereumAddressFromString(provider)

		engineContext := newEthEngineContext(ctx, input.Platform, signerAddr, input.Height)
		engineContext.TxContext.TxID = txid

		args := []any{g.dataProviders, g.streamIds, g.eventTimes, g.values}
		r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "insert_records", args, func(row *common.Row) error { return nil })
		if err != nil {
			return errors.Wrapf(err, "insert_records call failed for provider %s", provider)
		}
		if r.Error != nil {
			return errors.Wrapf(r.Error, "insert_records result failed for provider %s", provider)
		}
	}

	return nil
}

type InsertTruflationRecordInput struct {
	EventTime           int64   `json:"event_time"`
	Value               float64 `json:"value"`
	TruflationCreatedAt string  `json:"truflation_created_at"`
}

type TruflationStreamWithData struct {
	PrimitiveStreamDefinition
	Data []InsertTruflationRecordInput
}

type InsertTruflationDataInput struct {
	Platform        *kwilTesting.Platform
	PrimitiveStream TruflationStreamWithData
	Height          int64
}

// InsertTruflationDataBatch calls the batch insertion action "truflation_insert_records" with arrays of parameters.
func InsertTruflationDataBatch(ctx context.Context, input InsertTruflationDataInput) error {
	dataProviders := []string{}
	streamIds := []string{}
	eventTimes := []int64{}
	values := []*kwilTypes.Decimal{}
	truflationCreatedAts := []string{}

	for _, data := range input.PrimitiveStream.Data {
		// For each record, add the same provider and stream id (they come from the stream locator)
		dataProviders = append(dataProviders, input.PrimitiveStream.StreamLocator.DataProvider.Address())
		streamIds = append(streamIds, input.PrimitiveStream.StreamLocator.StreamId.String())
		eventTimes = append(eventTimes, data.EventTime)
		truflationCreatedAts = append(truflationCreatedAts, data.TruflationCreatedAt)
		valueDecimal, err := kwilTypes.ParseDecimalExplicit(strconv.FormatFloat(data.Value, 'f', -1, 64), 36, 18)
		if err != nil {
			return errors.Wrap(err, "error in InsertTruflationDataBatch")
		}
		values = append(values, valueDecimal)
	}

	args := []any{
		dataProviders,
		streamIds,
		eventTimes,
		values,
		truflationCreatedAts,
	}

	txid := input.Platform.Txid()

	deployer, err := util.NewEthereumAddressFromBytes(input.PrimitiveStream.StreamLocator.DataProvider.Bytes())
	if err != nil {
		return errors.Wrap(err, "error in InsertTruflationDataBatch")
	}

	engineContext := newEthEngineContext(ctx, input.Platform, deployer, input.Height)
	engineContext.TxContext.TxID = txid

	r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "truflation_insert_records", args, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return err
	}
	if r.Error != nil {
		return errors.Wrap(r.Error, "error in InsertTruflationDataBatch")
	}
	return nil
}
