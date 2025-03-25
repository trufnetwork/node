package benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	mathrand "math/rand"
	"strconv"
	"time"

	"github.com/cockroachdb/apd/v3"
	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/pkg/errors"
	"github.com/trufnetwork/node/internal/benchmark/trees"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
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

	allStreamInfos := []setup.StreamInfo{}

	for _, node := range input.Tree.Nodes {
		streamId := getStreamId(node.Index)
		streamInfo := setup.StreamInfo{
			Locator: types.StreamLocator{
				DataProvider: deployerAddress,
				StreamId:     *streamId,
			},
		}

		if node.IsLeaf {
			streamInfo.Type = "primitive"
		} else {
			streamInfo.Type = "composed"
		}

		allStreamInfos = append(allStreamInfos, streamInfo)
	}

	if err := setup.CreateStreams(ctx, platform, allStreamInfos); err != nil {
		return errors.Wrap(err, "failed to create stream")
	}

	for i, streamInfo := range allStreamInfos {
		if err := setupSchema(ctx, platform, streamInfo, setupSchemaInput{
			visibility:  input.BenchmarkCase.Visibility,
			treeNode:    input.Tree.Nodes[i],
			rangeParams: getMaxRangeParams(input.BenchmarkCase.DataPointsSet),
			owner:       deployerAddress,
		}); err != nil {
			return errors.Wrap(err, "failed to setup schema")
		}
	}

	return nil
}

type setupSchemaInput struct {
	visibility  util.VisibilityEnum
	owner       util.EthereumAddress
	treeNode    trees.TreeNode
	rangeParams RangeParameters
}

func setupSchema(ctx context.Context, platform *kwilTesting.Platform, stream setup.StreamInfo, input setupSchemaInput) error {

	if input.visibility == util.PrivateVisibility {
		if err := setVisibilityAndWhitelist(ctx, platform, stream, input.treeNode); err != nil {
			return errors.Wrap(err, "failed to set visibility and whitelist")
		}
	}

	// if it's a leaf, then it's a primitive stream
	if input.treeNode.IsLeaf {
		if err := insertRecordsForPrimitive(ctx, platform, stream, input.rangeParams); err != nil {
			return errors.Wrap(err, "failed to insert records for primitive")
		}
	} else {
		if err := setTaxonomyForComposed(ctx, platform, stream, input); err != nil {
			return errors.Wrap(err, "failed to set taxonomy for composed")
		}
	}
	return nil
}

func setVisibilityAndWhitelist(ctx context.Context, platform *kwilTesting.Platform, stream setup.StreamInfo, treeNode trees.TreeNode) error {
	parentStreamId := getStreamId(treeNode.Parent)
	metadataToInsert := []procedure.InsertMetadataInput{
		{Key: string(types.ComposeVisibilityKey), Value: strconv.Itoa(int(util.PrivateVisibility)), ValType: string(types.ComposeVisibilityKey.GetType())},
		{Key: string(types.AllowComposeStreamKey), Value: parentStreamId.String(), ValType: string(types.AllowComposeStreamKey.GetType())},
		{Key: string(types.ReadVisibilityKey), Value: strconv.Itoa(int(util.PrivateVisibility)), ValType: string(types.ReadVisibilityKey.GetType())},
		{Key: string(types.AllowReadWalletKey), Value: readerAddress.Address(), ValType: string(types.AllowReadWalletKey.GetType())},
	}

	// generate more wallets and stream ids, to make a little more realistic result
	// they shoudln't be influencing too much, if our indexing is correct
	for _, wallet := range getMockReadWallets(1000) {
		metadataToInsert = append(metadataToInsert, procedure.InsertMetadataInput{
			Key:     string(types.AllowReadWalletKey),
			Value:   wallet.Address(),
			ValType: string(types.AllowReadWalletKey.GetType()),
		})
	}

	for _, streamId := range getMockStreamIds(1000) {
		metadataToInsert = append(metadataToInsert, procedure.InsertMetadataInput{
			Key:     string(types.AllowComposeStreamKey),
			Value:   streamId.String(),
			ValType: string(types.AllowComposeStreamKey.GetType()),
		})
	}

	// for all inputs, add the locator and height
	for i := range metadataToInsert {
		metadataToInsert[i].Locator = stream.Locator
		metadataToInsert[i].Height = 1
		metadataToInsert[i].Platform = platform
	}

	for _, input := range metadataToInsert {
		if err := procedure.InsertMetadata(ctx, input); err != nil {
			return errors.Wrap(err, "failed to insert metadata")
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

// insertRecordsForPrimitive inserts records for a primitive stream.
// - it generates records for the given number of days
// - it generates a random value for each record
// - it inserts the records into the stream
// - we use a bulk insert to speed up the process
func insertRecordsForPrimitive(ctx context.Context, platform *kwilTesting.Platform, stream setup.StreamInfo, rangeParams RangeParameters) error {
	records := generateRecords(rangeParams)

	input := setup.InsertPrimitiveDataInput{
		Platform: platform,
		Height:   1,
		PrimitiveStream: setup.PrimitiveStreamWithData{
			PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
				StreamLocator: stream.Locator,
			},
			Data: records,
		},
	}

	// Execute the bulk insert
	if err := setup.InsertPrimitiveDataBatch(ctx, input); err != nil {
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
func setTaxonomyForComposed(ctx context.Context, platform *kwilTesting.Platform, stream setup.StreamInfo, input setupSchemaInput) error {
	// Calculate parent and child stream IDs based on the new structure
	var dataProviders []string
	var streamIds []string
	var weights []string
	for _, childIndex := range input.treeNode.Children {
		childStreamId := getStreamId(childIndex)
		randWeight, _ := apd.New(mathrand.Int63n(10), 0).Float64() // can't be so big, otherwise it overflows when multiplying values
		dataProviders = append(dataProviders, stream.Locator.DataProvider.Address())
		streamIds = append(streamIds, childStreamId.String())
		weights = append(weights, strconv.FormatFloat(randWeight, 'f', -1, 64))
	}

	return procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
		Platform:      platform,
		StreamLocator: stream.Locator,
		DataProviders: dataProviders,
		StreamIds:     streamIds,
		Weights:       weights,
	})
}

func randDate(minDate, maxDate time.Time) time.Time {
	delta := maxDate.Unix() - minDate.Unix()

	sec := mathrand.Int63n(delta) + minDate.Unix()
	return time.Unix(sec, 0)
}
