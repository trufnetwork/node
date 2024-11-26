package setup

import (
	"context"

	"github.com/trufnetwork/sdk-go/core/types"

	"github.com/golang-sql/civil"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/node/internal/contracts"
	testdate "github.com/trufnetwork/node/internal/contracts/tests/utils/date"
	testtable "github.com/trufnetwork/node/internal/contracts/tests/utils/table"
	"github.com/trufnetwork/sdk-go/core/util"
)

type PrimitiveStreamDefinition struct {
	StreamLocator types.StreamLocator
}

type InsertRecordInput struct {
	DateValue civil.Date
	Value     string
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
	primitiveSchema, err := parse.Parse(contracts.PrimitiveStreamContent)
	if err != nil {
		return errors.Wrap(err, "error parsing primitive stream content")
	}
	primitiveSchema.Name = setupInput.PrimitiveStreamWithData.StreamLocator.StreamId.String()

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

	if err := setupInput.Platform.Engine.CreateDataset(txContext, setupInput.Platform.DB, primitiveSchema); err != nil {
		return errors.Wrap(err, "error creating primitive dataset")
	}

	dbid := utils.GenerateDBID(setupInput.PrimitiveStreamWithData.StreamLocator.StreamId.String(), deployer.Bytes())
	if err := initializeContract(ctx, InitializeContractInput{
		Platform: setupInput.Platform,
		Deployer: deployer,
		Dbid:     dbid,
		Height:   setupInput.Height,
	}); err != nil {
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
		date := row[0]
		value := row[1]
		// if value is empty, we don't insert it
		if value == "" {
			continue
		}
		primitiveStream.Data = append(primitiveStream.Data, InsertRecordInput{
			DateValue: testdate.MustParseDate(date),
			Value:     value,
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

	dbid := utils.GenerateDBID(input.StreamLocator.StreamId.String(), input.StreamLocator.DataProvider.Bytes())

	txid := input.Platform.Txid()

	signer, err := util.NewEthereumAddressFromBytes(input.Platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error in InsertMarkdownPrimitiveData")
	}

	for _, row := range table.Rows {
		date := row[0]
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

		_, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
			Procedure: "insert_record",
			Dataset:   dbid,
			Args:      []any{testdate.MustParseDate(date), value},
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
		args = append(args, []any{data.DateValue, data.Value})
	}

	dbid := utils.GenerateDBID(
		input.primitiveStream.StreamLocator.StreamId.String(),
		input.primitiveStream.StreamLocator.DataProvider.Bytes(),
	)

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

		_, err := input.Platform.Engine.Procedure(txContext, input.Platform.DB, &common.ExecutionData{
			Procedure: "insert_record",
			Dataset:   dbid,
			Args:      arg,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
