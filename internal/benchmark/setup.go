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
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
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

		if err := batchInsertAllPrimitiveData(ctx, allPrimitiveData); err != nil {
			return errors.Wrap(err, "failed to batch insert primitive data")
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

// batchInsertAllPrimitiveData performs batch insertion of primitive data across multiple streams.
func batchInsertAllPrimitiveData(ctx context.Context, allData []setup.InsertPrimitiveDataInput) error {
	if len(allData) == 0 {
		return nil
	}

	type record struct {
		dataProvider string
		streamId     string
		eventTime    int64
		value        *kwilTypes.Decimal
	}

	var allRecords []record
	for _, input := range allData {
		for _, data := range input.PrimitiveStream.Data {
			valueDecimal, err := kwilTypes.ParseDecimalExplicit(strconv.FormatFloat(data.Value, 'f', -1, 64), 36, 18)
			if err != nil {
				return errors.Wrap(err, "error parsing decimal")
			}
			// Filter out zero values to reduce payload size
			if valueDecimal.IsZero() {
				continue
			}
			allRecords = append(allRecords, record{
				dataProvider: input.PrimitiveStream.StreamLocator.DataProvider.Address(),
				streamId:     input.PrimitiveStream.StreamLocator.StreamId.String(),
				eventTime:    data.EventTime,
				value:        valueDecimal,
			})
		}
	}

	if len(allRecords) == 0 {
		return nil
	}

	platform := allData[0].Platform
	height := allData[0].Height
	deployer, err := util.NewEthereumAddressFromBytes(platform.Deployer)
	if err != nil {
		return errors.Wrap(err, "error creating deployer address")
	}

	const recordBatchSize = 1000
	numBatches := (len(allRecords) + recordBatchSize - 1) / recordBatchSize // Ceiling division

	fmt.Printf("[BATCH_INSERT] Processing %d total records in %d batches of up to %d records each\n",
		len(allRecords), numBatches, recordBatchSize)

	// Process sequentially to avoid concurrent queries on the same tx/conn (prevents "conn busy").
	for i := 0; i < len(allRecords); i += recordBatchSize {
		end := i + recordBatchSize
		if end > len(allRecords) {
			end = len(allRecords)
		}
		currentBatch := allRecords[i:end]

		if len(currentBatch) == 0 {
			continue
		}

		dataProviders := make([]string, len(currentBatch))
		streamIds := make([]string, len(currentBatch))
		eventTimes := make([]int64, len(currentBatch))
		values := make([]*kwilTypes.Decimal, len(currentBatch))

		for i, r := range currentBatch {
			dataProviders[i] = r.dataProvider
			streamIds[i] = r.streamId
			eventTimes[i] = r.eventTime
			values[i] = r.value
		}

		txContext := &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: height,
			},
			TxID:   platform.Txid(),
			Signer: deployer.Bytes(),
			Caller: deployer.Address(),
		}
		engineContext := &common.EngineContext{
			TxContext: txContext,
		}

		args := []any{
			dataProviders,
			streamIds,
			eventTimes,
			values,
		}

		r, err := platform.Engine.Call(engineContext, platform.DB, "", "insert_records", args, func(row *common.Row) error {
			return nil
		})
		if err != nil {
			return errors.Wrapf(err, "error in batch insert for a chunk of %d records", len(currentBatch))
		}
		if r.Error != nil {
			return errors.Wrapf(r.Error, "procedure error in batch insert for a chunk of %d records", len(currentBatch))
		}
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

	return procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
		Platform:      platform,
		StreamLocator: stream.Locator,
		DataProviders: dataProviders,
		StreamIds:     streamIds,
		Weights:       weights,
	})
}
