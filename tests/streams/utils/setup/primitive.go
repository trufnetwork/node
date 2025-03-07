package setup

import (
	"context"
	"strconv"

	"github.com/trufnetwork/sdk-go/core/types"

	"github.com/kwilteam/kwil-db/common"
	kwilTypes "github.com/kwilteam/kwil-db/core/types"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	testtable "github.com/trufnetwork/node/tests/streams/utils/table"
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

func setupPrimitive(ctx context.Context, setupInput SetupPrimitiveInput) error {
	deployer, err := util.NewEthereumAddressFromBytes(setupInput.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error in setupPrimitive")
	}

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: setupInput.Height,
		},
		TxID:   setupInput.Platform.Txid(),
		Signer: deployer.Bytes(),
		Caller: deployer.Address(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	// Create the stream using create_stream action
	_, err = setupInput.Platform.Engine.Call(engineContext, setupInput.Platform.DB, "", "create_stream", []any{
		setupInput.PrimitiveStreamWithData.StreamLocator.StreamId.String(),
		"primitive",
	}, func(row *common.Row) error {
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error creating primitive stream")
	}

	// Insert the data
	if err := insertPrimitiveData(ctx, InsertPrimitiveDataInput{
		Platform:        setupInput.Platform,
		primitiveStream: setupInput.PrimitiveStreamWithData,
		height:          setupInput.Height,
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
	return setupPrimitive(ctx, setup)
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

		txContext := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: input.Height,
			},
			TxID:   txid,
			Signer: signer.Bytes(),
			Caller: signer.Address(),
		}

		engineContext := &common.EngineContext{
			TxContext: txContext,
		}

		_, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "insert_record", []any{
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
	}
	return nil
}

type InsertPrimitiveDataInput struct {
	Platform        *kwilTesting.Platform
	primitiveStream PrimitiveStreamWithData
	height          int64
}

func insertPrimitiveData(ctx context.Context, input InsertPrimitiveDataInput) error {
	args := [][]any{}
	for _, data := range input.primitiveStream.Data {
		valueDecimal, err := kwilTypes.ParseDecimalExplicit(strconv.FormatFloat(data.Value, 'f', -1, 64), 36, 18)
		if err != nil {
			return errors.Wrap(err, "error in insertPrimitiveData")
		}
		args = append(args, []any{
			input.primitiveStream.StreamLocator.DataProvider.Address(),
			input.primitiveStream.StreamLocator.StreamId.String(),
			data.EventTime,
			valueDecimal,
		})
	}

	txid := input.Platform.Txid()

	deployer, err := util.NewEthereumAddressFromBytes(input.primitiveStream.StreamLocator.DataProvider.Bytes())
	if err != nil {
		return errors.Wrap(err, "error in insertPrimitiveData")
	}

	for _, arg := range args {
		txContext := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: input.height,
			},
			TxID:   txid,
			Signer: deployer.Bytes(),
			Caller: deployer.Address(),
		}

		engineContext := &common.EngineContext{
			TxContext: txContext,
		}

		_, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "insert_record", arg, func(row *common.Row) error {
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// ExecuteInsertRecord executes the create_stream procedure
func ExecuteInsertRecord(ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, input InsertRecordInput, height int64) error {
	insertPrimitiveDataInput := InsertPrimitiveDataInput{
		Platform: platform,
		primitiveStream: PrimitiveStreamWithData{
			PrimitiveStreamDefinition: PrimitiveStreamDefinition{
				StreamLocator: locator,
			},
			Data: []InsertRecordInput{input},
		},
		height: height,
	}

	return insertPrimitiveData(ctx, insertPrimitiveDataInput)
}
