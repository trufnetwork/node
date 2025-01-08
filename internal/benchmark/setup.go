package benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	mathrand "math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/google/uuid"
	"github.com/kwilteam/kwil-db/common"
	kwiltypes "github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/node/internal/benchmark/trees"
	"github.com/trufnetwork/node/internal/contracts"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
	"golang.org/x/sync/errgroup"
)

type SetupSchemasInput struct {
	BenchmarkCase BenchmarkCase
	Tree          trees.Tree
}

// Schema setup functions
func setupSchemas(
	ctx context.Context,
	platform *kwilTesting.Platform,
	input SetupSchemasInput,
) error {
	deployerAddress := MustNewEthereumAddressFromBytes(platform.Deployer)

	type schemaAndNode struct {
		Schema *kwiltypes.Schema
		Node   trees.TreeNode
	}

	// Create an unbuffered channel to process one schema at a time
	schemaChan := make(chan schemaAndNode)

	eg, grpCtx := errgroup.WithContext(ctx)

	// Producer goroutine to generate schemas
	// it can be trying to create scheams asap, but consumer will block if it's not finished
	eg.Go(func() error {
		defer close(schemaChan)
		for _, node := range input.Tree.Nodes {
			select {
			case <-grpCtx.Done():
				return grpCtx.Err()
			default:
				var schema *kwiltypes.Schema
				var err error
				if node.IsLeaf {
					if input.BenchmarkCase.UnixOnly {
						schema, err = parse.Parse(contracts.PrimitiveStreamUnixContent)
					} else {
						schema, err = parse.Parse(contracts.PrimitiveStreamContent)
					}
				} else {
					if input.BenchmarkCase.UnixOnly {
						schema, err = parse.Parse(contracts.ComposedStreamUnixContent)
					} else {
						schema, err = parse.Parse(contracts.ComposedStreamContent)
					}
				}
				if err != nil {
					return errors.Wrap(err, "failed to parse stream")
				}

				schema.Name = getStreamId(node.Index).String()
				select {
				case schemaChan <- schemaAndNode{Schema: schema, Node: node}:
				case <-grpCtx.Done():
					return grpCtx.Err()
				}
			}
		}
		return nil
	})

	// Consumer goroutine to process schemas
	// it can't be parallel because uses postgres pool under the hood
	eg.Go(func() error {
		for schema := range schemaChan {
			if err := createAndInitializeSchema(grpCtx, platform, schema.Schema); err != nil {
				return errors.Wrap(err, "failed to create and initialize schema")
			}

			if err := setupSchema(grpCtx, platform, schema.Schema, setupSchemaInput{
				visibility:  input.BenchmarkCase.Visibility,
				treeNode:    schema.Node,
				rangeParams: getMaxRangeParams(input.BenchmarkCase.DataPointsSet, input.BenchmarkCase.UnixOnly),
				owner:       deployerAddress,
				unixOnly:    input.BenchmarkCase.UnixOnly,
			}); err != nil {
				return errors.Wrap(err, "failed to setup schema")
			}
		}
		return nil
	})

	// Wait for all goroutines to complete
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func createAndInitializeSchema(ctx context.Context, platform *kwilTesting.Platform, schema *kwiltypes.Schema) error {
	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 0,
		},
		TxID:   platform.Txid(),
		Signer: platform.Deployer,
	}

	if err := platform.Engine.CreateDataset(txContext, platform.DB, schema); err != nil {
		return errors.Wrap(err, "failed to create dataset")
	}

	txContext2 := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 1,
		},
		TxID:   platform.Txid(),
		Signer: platform.Deployer,
		Caller: MustEthereumAddressFromBytes(platform.Deployer).Address(),
	}
	_, err := platform.Engine.Procedure(txContext2, platform.DB, &common.ExecutionData{
		Procedure: "init",
		Dataset:   utils.GenerateDBID(schema.Name, platform.Deployer),
		Args:      []any{},
	})
	if err != nil {
		return errors.Wrap(err, "failed to initialize schema")
	}
	return nil
}

type setupSchemaInput struct {
	visibility  util.VisibilityEnum
	owner       util.EthereumAddress
	treeNode    trees.TreeNode
	rangeParams RangeParameters
	unixOnly    bool
}

func setupSchema(ctx context.Context, platform *kwilTesting.Platform, schema *kwiltypes.Schema, input setupSchemaInput) error {
	dbid := utils.GenerateDBID(schema.Name, input.owner.Bytes())

	if input.visibility == util.PrivateVisibility {
		if err := setVisibilityAndWhitelist(ctx, platform, dbid, input.treeNode); err != nil {
			return errors.Wrap(err, "failed to set visibility and whitelist")
		}
	}

	// if it's a leaf, then it's a primitive stream
	if input.treeNode.IsLeaf {
		if err := insertRecordsForPrimitive(ctx, platform, dbid, input.rangeParams, input.unixOnly); err != nil {
			return errors.Wrap(err, "failed to insert records for primitive")
		}
	} else {
		if err := setTaxonomyForComposed(ctx, platform, dbid, input); err != nil {
			return errors.Wrap(err, "failed to set taxonomy for composed")
		}
	}
	return nil
}

func setVisibilityAndWhitelist(ctx context.Context, platform *kwilTesting.Platform, dbid string, treeNode trees.TreeNode) error {
	parentStreamId := getStreamId(treeNode.Parent)
	parentDbid := utils.GenerateDBID(parentStreamId.String(), platform.Deployer)
	metadataToInsert := []struct {
		key     string
		val     string
		valType string
	}{
		{string(types.ComposeVisibilityKey), strconv.Itoa(int(util.PrivateVisibility)), string(types.ComposeVisibilityKey.GetType())},
		{string(types.AllowComposeStreamKey), parentDbid, string(types.AllowComposeStreamKey.GetType())},
		{string(types.ReadVisibilityKey), strconv.Itoa(int(util.PrivateVisibility)), string(types.ReadVisibilityKey.GetType())},
		{string(types.AllowReadWalletKey), readerAddress.Address(), string(types.AllowReadWalletKey.GetType())},
	}

	// generate more wallets and stream ids, to make a little more realistic result
	// they shoudln't be influencing too much, if our indexing is correct
	for _, wallet := range getMockReadWallets(1000) {
		metadataToInsert = append(metadataToInsert, struct {
			key     string
			val     string
			valType string
		}{string(types.AllowReadWalletKey), wallet.Address(), string(types.AllowReadWalletKey.GetType())})
	}

	for _, streamId := range getMockStreamIds(1000) {
		metadataToInsert = append(metadataToInsert, struct {
			key     string
			val     string
			valType string
		}{string(types.AllowComposeStreamKey), streamId.String(), string(types.AllowComposeStreamKey.GetType())})
	}

	if err := batchInsertMetadata(ctx, platform, dbid, 1, metadataToInsert); err != nil {
		return errors.Wrap(err, "failed to insert metadata")
	}
	return nil
}

// getMockReadWallets generates and returns a slice of Ethereum addresses.
// The number of addresses generated is determined by the parameter `n`.
func getMockReadWallets(n int) []util.EthereumAddress {
	wallets := make([]util.EthereumAddress, 0, n)
	for i := 0; i < n; i++ {
		// Generate a 20-byte address
		addrBytes := make([]byte, 20)
		_, err := rand.Read(addrBytes)
		if err != nil {
			panic(fmt.Sprintf("failed to generate random address: %v", err))
		}

		// Convert to EthereumAddress
		addr, err := util.NewEthereumAddressFromBytes(addrBytes)
		if err != nil {
			panic(fmt.Errorf("failed to create Ethereum address: %w", err))
		}

		wallets = append(wallets, addr)
	}
	return wallets
}

// getMockStreamIds generates and returns a slice of util.StreamId.
// The number of streamIds generated is determined by the parameter `n`.
func getMockStreamIds(n int) []util.StreamId {
	var streamIds []util.StreamId
	for i := 0; i < n; i++ {
		streamIds = append(streamIds, util.GenerateStreamId(fmt.Sprintf("stream-%d", i)))
	}
	return streamIds
}

func batchInsertMetadata(ctx context.Context, platform *kwilTesting.Platform, dbid string, height int, metadata []struct {
	key     string
	val     string
	valType string
}) error {
	// Prepare the SQL statement for bulk insert
	sqlStmt := "INSERT INTO metadata (row_id, metadata_key, value_i, value_f, value_b, value_s, value_ref, created_at) VALUES "
	var values []string

	for _, m := range metadata {
		uuidVal := fmt.Sprintf("'%s'::uuid", uuid.New().String())
		valueI, valueF, valueB, valueS, valueRef := "NULL", "NULL", "NULL", "NULL", "NULL"

		switch m.valType {
		case "int":
			valueI = m.val
		case "float":
			valueF = m.val
		case "bool":
			valueB = m.val
		case "string":
			valueS = fmt.Sprintf("'%s'", m.val)
		case "ref":
			valueRef = fmt.Sprintf("LOWER('%s')", m.val)
		}

		values = append(values, fmt.Sprintf("(%s, '%s', %s, %s, %s, %s, %s, %d)",
			uuidVal, m.key, valueI, valueF, valueB, valueS, valueRef, height))
	}

	sqlStmt += strings.Join(values, ", ")

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 1,
		},
	}

	// Execute the bulk insert
	_, err := platform.Engine.Execute(txContext, platform.DB, dbid, sqlStmt, nil)
	if err != nil {
		return errors.Wrap(err, "failed to execute bulk insert for metadata")
	}
	return nil
}

// insertRecordsForPrimitive inserts records for a primitive stream.
// - it generates records for the given number of days
// - it generates a random value for each record
// - it inserts the records into the stream
// - we use a bulk insert to speed up the process
func insertRecordsForPrimitive(ctx context.Context, platform *kwilTesting.Platform, dbid string, rangeParams RangeParameters, unixOnly bool) error {
	records := generateRecords(rangeParams, unixOnly)

	// Prepare the SQL statement for bulk insert
	sqlStmt := "INSERT INTO primitive_events (date_value, value, created_at) VALUES "
	var values []string

	if unixOnly {
		for _, record := range records {
			values = append(values, fmt.Sprintf("(%d, %s::decimal(36,18), 0)", record[0], record[1]))
		}
	} else {
		for _, record := range records {
			values = append(values, fmt.Sprintf("('%s', %s::decimal(36,18), 0)", record[0], record[1]))
		}
	}

	sqlStmt += strings.Join(values, ", ")

	txContext := &common.TxContext{
		Ctx: ctx,
		BlockContext: &common.BlockContext{
			Height: 1,
		},
	}

	// Execute the bulk insert
	_, err := platform.Engine.Execute(txContext, platform.DB, dbid, sqlStmt, nil)
	if err != nil {
		return errors.Wrap(err, "failed to execute bulk insert")
	}
	return nil
}

type RangeParameters struct {
	DataPoints int
	FromDate   time.Time
	ToDate     time.Time
}

// setTaxonomyForComposed sets the taxonomy for a composed stream.
// - it creates a new taxonomy item for each child stream
func setTaxonomyForComposed(ctx context.Context, platform *kwilTesting.Platform, dbid string, input setupSchemaInput) error {
	// Calculate parent and child stream IDs based on the new structure
	var taxonomy []types.TaxonomyItem
	for _, childIndex := range input.treeNode.Children {
		childStreamId := getStreamId(childIndex)
		randWeight, _ := apd.New(mathrand.Int63n(10), 0).Float64() // can't be so big, otherwise it overflows when multiplying values
		taxonomy = append(taxonomy, types.TaxonomyItem{
			Weight: randWeight,
			ChildStream: types.StreamLocator{
				DataProvider: input.owner,
				StreamId:     *childStreamId,
			},
		})
	}

	var dataProvidersArg []string
	var streamIdsArg []string
	var weightsArg []int
	var startDateArg string

	for _, t := range taxonomy {
		dataProvidersArg = append(dataProvidersArg, t.ChildStream.DataProvider.Address())
		streamIdsArg = append(streamIdsArg, t.ChildStream.StreamId.String())
		weightsArg = append(weightsArg, int(t.Weight))
	}

	if input.unixOnly {
		startDateArg = strconv.Itoa(int(input.rangeParams.FromDate.Unix()))
	} else {
		startDateArg = input.rangeParams.FromDate.Format(time.DateOnly)
	}

	return executeStreamProcedure(ctx, platform, dbid, "set_taxonomy",
		[]any{dataProvidersArg, streamIdsArg, weightsArg, startDateArg}, platform.Deployer)
}

func randDate(minDate, maxDate time.Time) time.Time {
	delta := maxDate.Unix() - minDate.Unix()

	sec := mathrand.Int63n(delta) + minDate.Unix()
	return time.Unix(sec, 0)
}
