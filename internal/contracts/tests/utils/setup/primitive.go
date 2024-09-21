package setup

import (
	"context"

	"github.com/golang-sql/civil"
	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/truflation/tsn-db/internal/contracts"
	testdate "github.com/truflation/tsn-db/internal/contracts/tests/utils/date"
	testtable "github.com/truflation/tsn-db/internal/contracts/tests/utils/table"
	"github.com/truflation/tsn-sdk/core/util"
)

type PrimitiveStreamDefinition struct {
	StreamId util.StreamId
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
	Platform            *kwilTesting.Platform
	Deployer            util.EthereumAddress
	Height              int64
	PrimitiveStreamName string
	MarkdownData        string
}

type SetupPrimitiveInput struct {
	Platform                *kwilTesting.Platform
	Deployer                util.EthereumAddress
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
		Signer: setupInput.Deployer.Bytes(),
		TxID:   setupInput.Platform.Txid(),
		Height: setupInput.Height,
	}); err != nil {
		return errors.Wrap(err, "error creating primitive dataset")
	}

	dbid := utils.GenerateDBID(setupInput.PrimitiveStreamWithData.StreamId.String(), setupInput.Deployer.Bytes())
	if err := initializeContract(ctx, InitializeContractInput{
		Platform: setupInput.Platform,
		Deployer: setupInput.Deployer,
		Dbid:     dbid,
		Height:   setupInput.Height,
	}); err != nil {
		return errors.Wrap(err, "error initializing primitive stream")
	}

	if err := insertPrimitiveData(ctx, InsertPrimitiveDataInput{
		Platform:        setupInput.Platform,
		Deployer:        setupInput.Deployer,
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
			DateValue: testdate.MustParseDate(date),
			Value:     value,
		})
	}

	return SetupPrimitiveInput{
		Platform:                input.Platform,
		Height:                  input.Height,
		PrimitiveStreamWithData: primitiveStream,
		Deployer:                input.Deployer,
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
	Platform            *kwilTesting.Platform
	Height              int64
	PrimitiveStreamName string
	MarkdownData        string
}

// InsertMarkdownPrimitiveData inserts data from a markdown table into a primitive stream
func InsertMarkdownPrimitiveData(ctx context.Context, input InsertMarkdownDataInput) error {
	table, err := testtable.TableFromMarkdown(input.MarkdownData)
	if err != nil {
		return err
	}

	primitiveStreamId := util.GenerateStreamId(input.PrimitiveStreamName)
	dbid := utils.GenerateDBID(primitiveStreamId.String(), input.Platform.Deployer)

	txid := input.Platform.Txid()

	for _, row := range table.Rows {
		date := row[0]
		value := row[1]
		if value == "" {
			continue
		}
		_, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
			Procedure: "insert_record",
			Dataset:   dbid,
			Args:      []any{testdate.MustParseDate(date), value},
			TransactionData: common.TransactionData{
				Signer: input.Platform.Deployer,
				TxID:   txid,
				Height: input.Height,
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

type InsertPrimitiveDataInput struct {
	Platform        *kwilTesting.Platform
	Deployer        util.EthereumAddress
	primitiveStream PrimitiveStreamWithData
	height          int64
}

func insertPrimitiveData(ctx context.Context, input InsertPrimitiveDataInput) error {

	args := [][]any{}
	for _, data := range input.primitiveStream.Data {
		args = append(args, []any{data.DateValue, data.Value})
	}

	dbid := utils.GenerateDBID(input.primitiveStream.StreamId.String(), input.Deployer.Bytes())

	txid := input.Platform.Txid()

	for _, arg := range args {
		_, err := input.Platform.Engine.Procedure(ctx, input.Platform.DB, &common.ExecutionData{
			Procedure: "insert_record",
			Dataset:   dbid,
			Args:      arg,
			TransactionData: common.TransactionData{
				Signer: input.Deployer.Bytes(),
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
