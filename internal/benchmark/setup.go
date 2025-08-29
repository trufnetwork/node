package benchmark

import (
	"context"
	"crypto/rand"
	"fmt"
	mathrand "math/rand"
	"strconv"
	"time"

	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/pkg/errors"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/benchmark/trees"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
	"golang.org/x/sync/errgroup"
)

type SetupSchemasInput struct {
	BenchmarkCase BenchmarkCase
	Tree          trees.Tree
}

// setupSchemas creates streams and sets up their schemas for benchmarking.
func setupSchemas(
	ctx context.Context,
	platform *kwilTesting.Platform,
	logger *testing.T,
	input SetupSchemasInput,
) error {
	LogPhaseEnter(logger, "setupSchemas", "QtyStreams: %d, BranchingFactor: %d, Visibility: %s", input.BenchmarkCase.QtyStreams, input.BenchmarkCase.BranchingFactor, visibilityToString(input.BenchmarkCase.Visibility))
	defer LogPhaseExit(logger, time.Now(), "setupSchemas", "")

	deployerAddress := MustNewEthereumAddressFromBytes(platform.Deployer)

	// Register the deployer as a data provider
	err := setup.CreateDataProvider(ctx, platform, deployerAddress.Address())
	if err != nil {
		return errors.Wrap(err, "failed to create data provider")
	}

	allStreamInfos := []setup.StreamInfo{}

	LogInfo(logger, "Creating %d streams (will then setup schema for each)", len(input.Tree.Nodes))
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
		LogInfo(logger, "Failed to create streams: %v", err)
		return errors.Wrap(err, "failed to create stream")
	}

	// Setup streams in parallel for better performance
	LogPhaseEnter(logger, "parallelSetupSchemas", "Setting up %d streams in parallel", len(allStreamInfos))
	parallelStart := time.Now()

	// Separate primitive and composed streams for different handling
	var primitiveStreams []setup.StreamInfo
	var composedStreams []setup.StreamInfo
	var composedNodes []trees.TreeNode

	for i, streamInfo := range allStreamInfos {
		if input.Tree.Nodes[i].IsLeaf {
			primitiveStreams = append(primitiveStreams, streamInfo)
		} else {
			composedStreams = append(composedStreams, streamInfo)
			composedNodes = append(composedNodes, input.Tree.Nodes[i])
		}
	}

	// Batch all primitive data generation and insertion
	if len(primitiveStreams) > 0 {
		LogPhaseEnter(logger, "batchPrimitiveDataGeneration", "Generating data for %d primitive streams", len(primitiveStreams))
		batchStart := time.Now()

		rangeParams := getMaxRangeParams(input.BenchmarkCase.DataPointsSet)

		// Generate all primitive data in parallel
		type PrimitiveData struct {
			StreamInfo setup.StreamInfo
			Records    []setup.InsertRecordInput
		}

		primitiveDataChan := make(chan PrimitiveData, len(primitiveStreams))

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(50) // Limit concurrency to avoid overwhelming system

		for _, streamInfo := range primitiveStreams {
			streamInfo := streamInfo // capture loop variable
			g.Go(func() error {
				records := generateRecords(rangeParams)
				select {
				case primitiveDataChan <- PrimitiveData{StreamInfo: streamInfo, Records: records}:
					return nil
				case <-gCtx.Done():
					return gCtx.Err()
				}
			})
		}

		// Close channel when all goroutines complete
		go func() {
			g.Wait()
			close(primitiveDataChan)
		}()

		if err := g.Wait(); err != nil {
			return errors.Wrap(err, "failed to generate primitive data in parallel")
		}

		// Collect all generated data
		var allPrimitiveData []setup.InsertPrimitiveDataInput
		for data := range primitiveDataChan {
			allPrimitiveData = append(allPrimitiveData, setup.InsertPrimitiveDataInput{
				Platform: platform,
				Height:   1,
				PrimitiveStream: setup.PrimitiveStreamWithData{
					PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
						StreamLocator: data.StreamInfo.Locator,
					},
					Data: data.Records,
				},
			})
		}

		LogPhaseExit(logger, batchStart, "batchPrimitiveDataGeneration", "Generated data for %d streams", len(allPrimitiveData))

		// Single batch insert for all primitive data
		totalRecords := 0
		for _, data := range allPrimitiveData {
			totalRecords += len(data.PrimitiveStream.Data)
		}

		LogPhaseEnter(logger, "batchPrimitiveDataInsertion", "Batch inserting %d total records from %d primitive streams", totalRecords, len(allPrimitiveData))
		insertStart := time.Now()

		// Group records into batches of at least 10K records and use InsertPrimitiveDataMultiBatch for each batch
		if err := batchInsertUsingMultiBatchWithGroups(ctx, platform, allPrimitiveData); err != nil {
			return errors.Wrap(err, "failed to batch insert primitive data using multi-batch with groups")
		}

		LogPhaseExit(logger, insertStart, "batchPrimitiveDataInsertion", "Completed batch insert of %d records", totalRecords)
	}

	// Setup composed streams (taxonomy) in parallel
	if len(composedStreams) > 0 {
		LogPhaseEnter(logger, "parallelComposedSetup", "Setting up %d composed streams in parallel", len(composedStreams))
		composedStart := time.Now()

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(2) // Lower limit for composed streams as they're more DB-intensive

		for i, streamInfo := range composedStreams {
			i, streamInfo := i, streamInfo // capture loop variables
			g.Go(func() error {
				return setupComposedSchema(gCtx, platform, streamInfo, setupSchemaInput{
					visibility:  input.BenchmarkCase.Visibility,
					treeNode:    composedNodes[i],
					rangeParams: getMaxRangeParams(input.BenchmarkCase.DataPointsSet),
					owner:       deployerAddress,
				})
			})
		}

		if err := g.Wait(); err != nil {
			return errors.Wrap(err, "failed to setup composed streams in parallel")
		}

		LogPhaseExit(logger, composedStart, "parallelComposedSetup", "Completed composed stream setup")
	}

	LogPhaseExit(logger, parallelStart, "parallelSetupSchemas", "Completed parallel setup")

	return nil
}

// batchInsertUsingMultiBatchWithGroups groups records into batches of at least 10K records and uses InsertPrimitiveDataMultiBatch for each batch
func batchInsertUsingMultiBatchWithGroups(ctx context.Context, platform *kwilTesting.Platform, allData []setup.InsertPrimitiveDataInput) error {
	if len(allData) == 0 {
		return nil
	}

	const minBatchSize = 1000

	// Collect all records from all streams
	type record struct {
		streamLocator types.StreamLocator
		eventTime     int64
		value         float64
	}

	var allRecords []record
	for _, input := range allData {
		for _, data := range input.PrimitiveStream.Data {
			// Filter out zero values to reduce payload size
			if data.Value == 0.0 {
				continue
			}
			allRecords = append(allRecords, record{
				streamLocator: input.PrimitiveStream.StreamLocator,
				eventTime:     data.EventTime,
				value:         data.Value,
			})
		}
	}

	if len(allRecords) == 0 {
		return nil
	}

	// Calculate number of batches needed (each with at least minBatchSize records)
	numBatches := (len(allRecords) + minBatchSize - 1) / minBatchSize

	fmt.Printf("[MULTI_BATCH_GROUPS] Processing %d total records in %d batches of at least %d records each\n",
		len(allRecords), numBatches, minBatchSize)

	height := allData[0].Height

	// Process each batch
	for i := 0; i < len(allRecords); i += minBatchSize {
		end := i + minBatchSize
		if end > len(allRecords) {
			end = len(allRecords)
		}
		currentBatch := allRecords[i:end]

		if len(currentBatch) == 0 {
			continue
		}

		// Group records by stream for this batch
		type streamKey struct {
			provider string
			id       string
		}
		type streamData struct {
			locator types.StreamLocator
			records []setup.InsertRecordInput
		}
		streamRecords := make(map[streamKey]*streamData)
		for _, r := range currentBatch {
			k := streamKey{
				provider: r.streamLocator.DataProvider.Address(),
				id:       r.streamLocator.StreamId.String(),
			}
			if streamRecords[k] == nil {
				streamRecords[k] = &streamData{
					locator: r.streamLocator,
					records: make([]setup.InsertRecordInput, 0),
				}
			}
			streamRecords[k].records = append(streamRecords[k].records, setup.InsertRecordInput{
				EventTime: r.eventTime,
				Value:     r.value,
			})
		}

		// Create InsertMultiPrimitiveDataInput for this batch
		multiInput := setup.InsertMultiPrimitiveDataInput{
			Platform: platform,
			Height:   height,
			Streams:  make([]setup.PrimitiveStreamWithData, 0, len(streamRecords)),
		}

		// Convert map to slice
		for _, streamData := range streamRecords {
			multiInput.Streams = append(multiInput.Streams, setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: streamData.locator,
				},
				Data: streamData.records,
			})
		}

		// Use InsertPrimitiveDataMultiBatch for this batch
		if err := setup.InsertPrimitiveDataMultiBatch(ctx, multiInput); err != nil {
			return errors.Wrapf(err, "error in multi-batch insert for group %d with %d records", i/minBatchSize+1, len(currentBatch))
		}

		fmt.Printf("[MULTI_BATCH_GROUPS] Completed group %d/%d with %d records\n", i/minBatchSize+1, numBatches, len(currentBatch))
	}

	return nil
}

// setupComposedSchema handles the setup of composed streams including visibility and taxonomy.
func setupComposedSchema(ctx context.Context, platform *kwilTesting.Platform, stream setup.StreamInfo, input setupSchemaInput) error {
	if input.visibility == util.PrivateVisibility {
		if err := setVisibilityAndWhitelist(ctx, platform, stream, input.treeNode); err != nil {
			return errors.Wrap(err, "failed to set visibility and whitelist")
		}
	}

	return setTaxonomyForComposed(ctx, platform, stream, input)
}

type setupSchemaInput struct {
	visibility  util.VisibilityEnum
	owner       util.EthereumAddress
	treeNode    trees.TreeNode
	rangeParams RangeParameters
}

func setVisibilityAndWhitelist(ctx context.Context, platform *kwilTesting.Platform, stream setup.StreamInfo, treeNode trees.TreeNode) error {
	parentStreamId := getStreamId(treeNode.Parent)
	metadataToInsert := []procedure.InsertMetadataInput{
		{Key: string(types.ComposeVisibilityKey), Value: strconv.Itoa(int(util.PrivateVisibility)), ValType: string(types.ComposeVisibilityKey.GetType())},
		{Key: string(types.AllowComposeStreamKey), Value: parentStreamId.String(), ValType: string(types.AllowComposeStreamKey.GetType())},
		{Key: string(types.ReadVisibilityKey), Value: strconv.Itoa(int(util.PrivateVisibility)), ValType: string(types.ReadVisibilityKey.GetType())},
		{Key: string(types.AllowReadWalletKey), Value: readerAddress.Address(), ValType: string(types.AllowReadWalletKey.GetType())},
	}

	for _, wallet := range getMockReadWallets(10) {
		metadataToInsert = append(metadataToInsert, procedure.InsertMetadataInput{
			Key:     string(types.AllowReadWalletKey),
			Value:   wallet.Address(),
			ValType: string(types.AllowReadWalletKey.GetType()),
		})
	}

	for _, streamId := range getMockStreamIds(10) {
		metadataToInsert = append(metadataToInsert, procedure.InsertMetadataInput{
			Key:     string(types.AllowComposeStreamKey),
			Value:   streamId.String(),
			ValType: string(types.AllowComposeStreamKey.GetType()),
		})
	}

	// Add locator and height to all metadata inputs
	for i := range metadataToInsert {
		metadataToInsert[i].Locator = stream.Locator
		metadataToInsert[i].Height = 1
		metadataToInsert[i].Platform = platform
	}

	return batchInsertMetadata(ctx, metadataToInsert)
}

// batchInsertMetadata inserts metadata records in parallel for better performance.
func batchInsertMetadata(ctx context.Context, metadataList []procedure.InsertMetadataInput) error {
	if len(metadataList) == 0 {
		return nil
	}

	g := errgroup.Group{}
	g.SetLimit(5) // Limit concurrent metadata operations

	for _, metadata := range metadataList {
		metadata := metadata // capture loop variable
		g.Go(func() error {
			return procedure.InsertMetadata(ctx, metadata)
		})
	}

	return g.Wait()
}

// getMockReadWallets generates and returns a slice of Ethereum addresses.
func getMockReadWallets(n int) []util.EthereumAddress {
	wallets := make([]util.EthereumAddress, 0, n)
	for i := 0; i < n; i++ {
		addrBytes := make([]byte, 20)
		_, err := rand.Read(addrBytes)
		if err != nil {
			panic(fmt.Sprintf("failed to generate random address: %v", err))
		}

		addr, err := util.NewEthereumAddressFromBytes(addrBytes)
		if err != nil {
			panic(fmt.Errorf("failed to create Ethereum address: %w", err))
		}

		wallets = append(wallets, addr)
	}
	return wallets
}

// getMockStreamIds generates and returns a slice of util.StreamId.
func getMockStreamIds(n int) []util.StreamId {
	var streamIds []util.StreamId
	for i := 0; i < n; i++ {
		streamIds = append(streamIds, util.GenerateStreamId(fmt.Sprintf("stream-%d", i)))
	}
	return streamIds
}

type RangeParameters struct {
	DataPoints int
	FromDate   time.Time
	ToDate     time.Time
}

// setTaxonomyForComposed sets the taxonomy for a composed stream by defining child relationships.
func setTaxonomyForComposed(ctx context.Context, platform *kwilTesting.Platform, stream setup.StreamInfo, input setupSchemaInput) error {
	var dataProviders []string
	var streamIds []string
	var weights []string
	for _, childIndex := range input.treeNode.Children {
		childStreamId := getStreamId(childIndex)
		randWeight, _ := apd.New(mathrand.Int63n(10), 0).Float64()
		dataProviders = append(dataProviders, stream.Locator.DataProvider.Address())
		streamIds = append(streamIds, childStreamId.String())
		weights = append(weights, strconv.FormatFloat(randWeight, 'f', -1, 64))
	}

	err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
		Platform:      platform,
		StreamLocator: stream.Locator,
		DataProviders: dataProviders,
		StreamIds:     streamIds,
		Weights:       weights,
	})

	if err != nil {
		return errors.Wrap(err, "failed to set taxonomy")
	}

	return err
}
