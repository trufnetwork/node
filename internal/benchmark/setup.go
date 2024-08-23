package benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/kwilteam/kwil-db/common"
	kwiltypes "github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/core/utils"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/truflation/tsn-sdk/core/types"
	"github.com/truflation/tsn-sdk/core/util"
	"strconv"
)

type setupSchemaInput struct {
	visibility util.VisibilityEnum
	depth      int
	days       int
	owner      util.EthereumAddress
}

// Schema setup functions
func setupSchemas(
	ctx context.Context,
	platform *kwilTesting.Platform,
	schemas []*kwiltypes.Schema,
	visibility util.VisibilityEnum,
) error {
	deployerAddress := MustNewEthereumAddressFromBytes(platform.Deployer)

	for i, schema := range schemas {
		if err := createAndInitializeSchema(ctx, platform, schema); err != nil {
			return err
		}

		if err := setupSchema(ctx, platform, schema, setupSchemaInput{
			visibility: visibility,
			depth:      i,
			days:       380, // to be sure we have more days to calculate change index
			owner:      deployerAddress,
		}); err != nil {
			return err
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
		return err
	}

	_, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "init",
		Dataset:   utils.GenerateDBID(schema.Name, platform.Deployer),
		Args:      []any{},
		TransactionData: *&common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 1,
		},
	})
	return err
}

func setupSchema(ctx context.Context, platform *kwilTesting.Platform, schema *kwiltypes.Schema, input setupSchemaInput) error {
	dbid := utils.GenerateDBID(schema.Name, input.owner.Bytes())
	readerStream := getStreamId(input.depth + 1)
	dbidReader := utils.GenerateDBID(schema.Name, []byte(readerStream.String()))

	if input.visibility == util.PrivateVisibility {
		if err := setVisibilityAndWhitelist(ctx, platform, dbid, dbidReader); err != nil {
			return err
		}
	}

	if input.depth == 0 {
		return insertRecordsForPrimitive(ctx, platform, dbid, input.days)
	}
	return setTaxonomyForComposed(ctx, platform, dbid, input)
}

func setVisibilityAndWhitelist(ctx context.Context, platform *kwilTesting.Platform, dbid string, readerDbid string) error {
	metadataToInsert := []struct {
		key     string
		val     string
		valType string
	}{
		{string(types.ComposeVisibilityKey), strconv.Itoa(int(util.PrivateVisibility)), string(types.ComposeVisibilityKey.GetType())},
		{string(types.AllowComposeStreamKey), readerDbid, string(types.AllowComposeStreamKey.GetType())},
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

	for _, m := range metadataToInsert {
		if err := insertMetadata(ctx, platform, dbid, m.key, m.val, m.valType); err != nil {
			return err
		}
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

func insertMetadata(ctx context.Context, platform *kwilTesting.Platform, dbid, key, val, valType string) error {
	_, err := platform.Engine.Procedure(ctx, platform.DB, &common.ExecutionData{
		Procedure: "insert_metadata",
		Dataset:   dbid,
		Args:      []any{key, val, valType},
		TransactionData: common.TransactionData{
			Signer: platform.Deployer,
			TxID:   platform.Txid(),
			Height: 0,
		},
	})
	return err
}

func insertRecordsForPrimitive(ctx context.Context, platform *kwilTesting.Platform, dbid string, days int) error {
	fromDate := fixedDate.AddDate(0, 0, -days)
	records := generateRecords(fromDate, fixedDate)

	for _, record := range records {
		if err := executeStreamProcedure(ctx, platform, dbid, "insert_record", record); err != nil {
			return err
		}
	}
	return nil
}

func setTaxonomyForComposed(ctx context.Context, platform *kwilTesting.Platform, dbid string, input setupSchemaInput) error {
	lastStreamId := getStreamId(input.depth - 1)
	taxonomy := []types.TaxonomyItem{{
		Weight: 1,
		ChildStream: types.StreamLocator{
			DataProvider: input.owner,
			StreamId:     *lastStreamId,
		},
	}}

	var dataProvidersArg []string
	var streamIdsArg []string
	var weightsArg []int

	for _, t := range taxonomy {
		dataProvidersArg = append(dataProvidersArg, t.ChildStream.DataProvider.Address())
		streamIdsArg = append(streamIdsArg, t.ChildStream.StreamId.String())
		weightsArg = append(weightsArg, int(t.Weight))
	}

	return executeStreamProcedure(ctx, platform, dbid, "set_taxonomy",
		[]any{dataProvidersArg, streamIdsArg, weightsArg})
}
