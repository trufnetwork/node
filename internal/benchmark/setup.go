package benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/kwilteam/kwil-db/common"
	kwiltypes "github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/core/utils"
	"github.com/kwilteam/kwil-db/parse"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/truflation/tsn-db/internal/benchmark/trees"
	"github.com/truflation/tsn-db/internal/contracts"
	"github.com/truflation/tsn-sdk/core/types"
	"github.com/truflation/tsn-sdk/core/util"
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

	// we make schemas parsing separate from the rest because it takes too much time and can be done in parallel
	var schemasAndNodes []schemaAndNode

	eg, grpCtx := errgroup.WithContext(ctx)

	// Create a channel to safely collect results
	resultChan := make(chan schemaAndNode, len(input.Tree.Nodes))

	for _, node := range input.Tree.Nodes {
		node := node // Create a new variable to avoid closure issues
		eg.Go(func() error {
			var schema *kwiltypes.Schema
			var err error
			if node.IsLeaf {
				schema, err = parse.Parse(contracts.PrimitiveStreamContent)
				if err != nil {
					return errors.Wrap(err, "failed to parse primitive stream")
				}
			} else {
				schema, err = parse.Parse(contracts.ComposedStreamContent)
				if err != nil {
					return errors.Wrap(err, "failed to parse composed stream")
				}
			}

			schema.Name = getStreamId(node.Index).String()
			select {
			case <-grpCtx.Done():
				return grpCtx.Err()
			case resultChan <- schemaAndNode{
				Schema: schema,
				Node:   node,
			}:
			}
			return nil
		})
	}

	// Wait for all goroutines to complete
	if err := eg.Wait(); err != nil {
		return err
	}

	// Collect results from the channel
	close(resultChan)
	schemasAndNodes = make([]schemaAndNode, 0, len(input.Tree.Nodes))
	for result := range resultChan {
		schemasAndNodes = append(schemasAndNodes, result)
	}

	for _, schema := range schemasAndNodes {
		if err := createAndInitializeSchema(ctx, platform, schema.Schema); err != nil {
			return errors.Wrap(err, "failed to create and initialize schema")
		}

		if err := setupSchema(ctx, platform, schema.Schema, setupSchemaInput{
			visibility: input.BenchmarkCase.Visibility,
			treeNode:   schema.Node,
			days:       380, // to be sure we have more days to calculate change index
			owner:      deployerAddress,
		}); err != nil {
			return errors.Wrap(err, "failed to setup schema")
		}
	}
	return nil
}

func createAndInitializeSchema(ctx context.Context, platform *kwilTesting.Platform, schema *kwiltypes.Schema) error {
	if err := platform.Engine.CreateDataset(ctx, platform.DB, schema, &common.TransactionData{
		Signer: platform.Deployer,
		TxID:   platform.Txid(),
		Height: 0,
	}); err != nil {
		return errors.Wrap(err, "failed to create dataset")
	}

	_, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "init",
		Dataset:   utils.GenerateDBID(schema.Name, platform.Deployer),
		Args:      []any{},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 1,
		},
	})
	if err != nil {
		return errors.Wrap(err, "failed to initialize schema")
	}
	return nil
}

type setupSchemaInput struct {
	visibility util.VisibilityEnum
	days       int
	owner      util.EthereumAddress
	treeNode   trees.TreeNode
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
		if err := insertRecordsForPrimitive(ctx, platform, dbid, input.days); err != nil {
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

	// Execute the bulk insert
	_, err := platform.Engine.Execute(ctx, platform.DB, dbid, sqlStmt, nil)
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
func insertRecordsForPrimitive(ctx context.Context, platform *kwilTesting.Platform, dbid string, days int) error {
	fromDate := fixedDate.AddDate(0, 0, -days)
	records := generateRecords(fromDate, fixedDate)

	// Prepare the SQL statement for bulk insert
	sqlStmt := "INSERT INTO primitive_events (date_value, value, created_at) VALUES "
	var values []string

	for _, record := range records {
		values = append(values, fmt.Sprintf("('%s', %s::decimal(21,3), 0)", record[0], record[1]))
	}

	sqlStmt += strings.Join(values, ", ")

	// Execute the bulk insert
	_, err := platform.Engine.Execute(ctx, platform.DB, dbid, sqlStmt, nil)
	if err != nil {
		return errors.Wrap(err, "failed to execute bulk insert")
	}
	return nil
}

// setTaxonomyForComposed sets the taxonomy for a composed stream.
// - it creates a new taxonomy item for each child stream
func setTaxonomyForComposed(ctx context.Context, platform *kwilTesting.Platform, dbid string, input setupSchemaInput) error {
	// Calculate parent and child stream IDs based on the new structure
	var taxonomy []types.TaxonomyItem
	for _, childIndex := range input.treeNode.Children {
		childStreamId := getStreamId(childIndex)
		taxonomy = append(taxonomy, types.TaxonomyItem{
			Weight: 1,
			ChildStream: types.StreamLocator{
				DataProvider: input.owner,
				StreamId:     *childStreamId,
			},
		})
	}

	var dataProvidersArg []string
	var streamIdsArg []string
	var weightsArg []int

	for _, t := range taxonomy {
		dataProvidersArg = append(dataProvidersArg, t.ChildStream.DataProvider.Address())
		streamIdsArg = append(streamIdsArg, t.ChildStream.StreamId.String())
		weightsArg = append(weightsArg, int(t.Weight))
	}

	return executeStreamProcedure(ctx, platform, dbid, "set_taxonomy",
		[]any{dataProvidersArg, streamIdsArg, weightsArg}, platform.Deployer)
}
