package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	kwilcrypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	kwiltypes "github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/sdk-go/core/tnclient"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

const (
	kwildEndpoint = "http://localhost:8484"
	// Using the same private key as in the server fixture
	deployerPrivateKey = "0000000000000000000000000000000000000000000000000000000000000001"
)

// TestCacheMetrics runs a comprehensive test to generate all types of cache metrics
func TestCacheMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

	defer cancel()

	// Parse deployer private key
	deployerWallet, err := kwilcrypto.Secp256k1PrivateKeyFromHex(deployerPrivateKey)
	require.NoError(t, err, "Failed to parse deployer private key")

	// Create TN client
	tnClient, err := tnclient.NewClient(ctx, kwildEndpoint, tnclient.WithSigner(auth.GetUserSigner(deployerWallet)))

	if err != nil {
		t.Skipf("Failed to create TN client (is the test environment running?): %v", err)
		return
	}

	// Setup cleanup to run after tests
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()

		t.Log("Cleaning up test streams...")
		if err := cleanupTestStreams(cleanupCtx, t, tnClient); err != nil {
			t.Logf("Warning: Failed to cleanup test streams: %v", err)
		}
	})

	// Grant network_writer role to deployer
	t.Log("Granting network_writer role to deployer...")
	err = grantNetworkWriterRole(ctx, t, deployerWallet)
	require.NoError(t, err, "Failed to grant network_writer role")

	// Deploy test streams and data
	t.Log("Deploying test streams...")
	err = deployTestStreams(ctx, t, tnClient)
	require.NoError(t, err, "Failed to deploy test streams")

	// Wait for cache scheduler to run at least once
	// The shortest refresh interval is 10 seconds, so wait at least that long
	t.Log("Waiting for cache refresh cycles (minimum 10 seconds for first refresh)...")
	time.Sleep(12 * time.Second)

	// Test 1: Generate cache hits
	t.Log("Testing cache hits...")
	err = testCacheHits(ctx, t, tnClient)
	require.NoError(t, err, "Failed to test cache hits")

	// Test 1.5: Generate index cache hits
	t.Log("Testing index cache hits...")
	err = testIndexCacheHits(ctx, t, tnClient)
	require.NoError(t, err, "Failed to test index cache hits")

	// Test 2: Generate cache misses

	t.Log("Testing cache misses...")
	err = testCacheMisses(ctx, t, tnClient)
	require.NoError(t, err, "Failed to test cache misses")

	// Test 2.5: Generate index cache misses
	t.Log("Testing index cache misses...")
	err = testIndexCacheMisses(ctx, t, tnClient)
	require.NoError(t, err, "Failed to test index cache misses")

	// Test 3: Wait for refresh cycles
	t.Log("Waiting for cache refresh cycles...")
	time.Sleep(30 * time.Second)

	// Test 4: Simulate refresh errors
	t.Log("Testing refresh errors...")
	err = testRefreshErrors(ctx, t, tnClient)
	require.NoError(t, err, "Failed to test refresh errors")

	t.Log("All tests completed successfully!")
	t.Log("Metrics should now be visible in Grafana at http://localhost:3000")
}

// deployTestStreams deploys composed streams with child primitive streams
func deployTestStreams(ctx context.Context, t *testing.T, tnClient *tnclient.Client) error {
	signerAddress := tnClient.Address()

	// Use fixed stream IDs in the proper format for cache configuration
	// Stream IDs must be exactly 32 characters long and start with "st"
	composedStreamId1Ptr, err := util.NewStreamId("stcomposed1234567890123456789001")
	if err != nil {
		return fmt.Errorf("failed to create composed stream ID 1: %w", err)
	}
	composedStreamId2Ptr, err := util.NewStreamId("stcomposed1234567890123456789002")
	if err != nil {
		return fmt.Errorf("failed to create composed stream ID 2: %w", err)
	}
	composedStreamId3Ptr, err := util.NewStreamId("stcomposed1234567890123456789003")
	if err != nil {
		return fmt.Errorf("failed to create composed stream ID 3: %w", err)
	}

	composedStreamId1 := *composedStreamId1Ptr
	composedStreamId2 := *composedStreamId2Ptr
	composedStreamId3 := *composedStreamId3Ptr

	// Stream IDs for primitive child streams (exactly 32 chars)
	childStreamId1Ptr, err := util.NewStreamId("stchild1234567890123456789child1")
	if err != nil {
		return fmt.Errorf("failed to create child stream ID 1: %w", err)
	}
	childStreamId2Ptr, err := util.NewStreamId("stchild1234567890123456789child2")
	if err != nil {
		return fmt.Errorf("failed to create child stream ID 2: %w", err)
	}

	childStreamId1 := *childStreamId1Ptr
	childStreamId2 := *childStreamId2Ptr

	// Deploy primitive streams first
	primitiveStreamDefs := []types.StreamDefinition{
		{StreamId: childStreamId1, StreamType: types.StreamTypePrimitive},
		{StreamId: childStreamId2, StreamType: types.StreamTypePrimitive},
	}

	batchDeployTxHash, err := tnClient.BatchDeployStreams(ctx, primitiveStreamDefs)
	if err != nil {
		return fmt.Errorf("failed to deploy primitive streams: %w", err)
	}
	waitDeployTx(t, ctx, tnClient, batchDeployTxHash)

	// Load primitive actions and insert data into child streams
	primitiveActions, err := tnClient.LoadPrimitiveActions()
	if err != nil {
		return fmt.Errorf("failed to load primitive actions: %w", err)
	}

	// Insert test data into child streams
	testData := []types.InsertRecordInput{
		// Child stream 1 data
		{DataProvider: signerAddress.Address(), StreamId: childStreamId1.String(), Value: 100.5, EventTime: 1609459200},
		{DataProvider: signerAddress.Address(), StreamId: childStreamId1.String(), Value: 101.2, EventTime: 1609459260},
		{DataProvider: signerAddress.Address(), StreamId: childStreamId1.String(), Value: 99.8, EventTime: 1609459320},
		// Child stream 2 data
		{DataProvider: signerAddress.Address(), StreamId: childStreamId2.String(), Value: 200.0, EventTime: 1609459200},
		{DataProvider: signerAddress.Address(), StreamId: childStreamId2.String(), Value: 201.5, EventTime: 1609459260},
		{DataProvider: signerAddress.Address(), StreamId: childStreamId2.String(), Value: 199.2, EventTime: 1609459320},
	}

	txHashInsert, err := primitiveActions.InsertRecords(ctx, testData)
	if err != nil {
		return fmt.Errorf("failed to insert records: %w", err)
	}
	waitTxToBeMinedWithSuccess(t, ctx, tnClient, txHashInsert)

	// Deploy composed streams
	composedStreamDefs := []types.StreamDefinition{
		{StreamId: composedStreamId1, StreamType: types.StreamTypeComposed},
		{StreamId: composedStreamId2, StreamType: types.StreamTypeComposed},
		{StreamId: composedStreamId3, StreamType: types.StreamTypeComposed},
	}

	batchDeployComposedTxHash, err := tnClient.BatchDeployStreams(ctx, composedStreamDefs)
	if err != nil {
		return fmt.Errorf("failed to deploy composed streams: %w", err)
	}
	waitDeployTx(t, ctx, tnClient, batchDeployComposedTxHash)

	// Load composed actions and set taxonomies
	composedActions, err := tnClient.LoadComposedActions()
	if err != nil {
		return fmt.Errorf("failed to load composed actions: %w", err)
	}

	// Set taxonomies for each composed stream
	composedStreams := []struct {
		streamId util.StreamId
		name     string
	}{
		{composedStreamId1, "composed-1"},
		{composedStreamId2, "composed-2"},
		{composedStreamId3, "composed-3"},
	}

	for _, composed := range composedStreams {
		startDate := 1609459200
		taxonomy := types.Taxonomy{
			ParentStream: types.StreamLocator{
				StreamId:     composed.streamId,
				DataProvider: signerAddress,
			},
			TaxonomyItems: []types.TaxonomyItem{
				{
					ChildStream: types.StreamLocator{
						StreamId:     childStreamId1,
						DataProvider: signerAddress,
					},
					Weight: 1,
				},
				{
					ChildStream: types.StreamLocator{
						StreamId:     childStreamId2,
						DataProvider: signerAddress,
					},
					Weight: 2,
				},
			},
			StartDate: &startDate,
		}

		txHashTax, err := composedActions.InsertTaxonomy(ctx, taxonomy)
		if err != nil {
			return fmt.Errorf("failed to set taxonomy for %s: %w", composed.name, err)
		}
		waitTxToBeMinedWithSuccess(t, ctx, tnClient, txHashTax)
	}

	t.Logf("Deployed composed streams: %s, %s, %s with data provider: %s",
		composedStreamId1, composedStreamId2, composedStreamId3, signerAddress.Address())
	t.Logf("Child streams: %s, %s", childStreamId1, childStreamId2)

	// Store the composed stream IDs for use in tests
	t.Setenv("COMPOSED_STREAM_1", composedStreamId1.String())
	t.Setenv("COMPOSED_STREAM_2", composedStreamId2.String())
	t.Setenv("COMPOSED_STREAM_3", composedStreamId3.String())

	return nil
}

// testCacheHits generates cache hit metrics by calling get_record on cached streams
func testCacheHits(ctx context.Context, t *testing.T, tnClient *tnclient.Client) error {
	// Get the raw Kwil client from TN client to make direct calls
	kwilClient := tnClient.GetKwilClient()
	signerAddress := tnClient.Address()

	// Get the composed stream ID from environment
	composedStreamId := os.Getenv("COMPOSED_STREAM_1")
	if composedStreamId == "" {
		return fmt.Errorf("COMPOSED_STREAM_1 not set")
	}

	// The cache works transparently - when we call get_record on a stream
	// that has been cached by the scheduler, it will internally check the cache
	// and generate hit/miss metrics accordingly

	// Query the composed stream multiple times - these should generate cache hits
	// because the scheduler has already refreshed this stream
	for i := 0; i < 10; i++ {
		args := []any{
			signerAddress.Address(), // data_provider
			composedStreamId,        // stream_id
			int64(1609459200),       // from_time
			int64(1609459400),       // to_time
			nil,                     // frozen_at
			true,                    // use_cache
		}

		// Call get_record - it will use cache since use_cache is true
		result, err := kwilClient.Call(ctx, "", "get_record", args)
		if err != nil {
			return fmt.Errorf("query %d failed: %w", i, err)
		}

		if result != nil && result.QueryResult != nil && len(result.QueryResult.Values) > 0 {
			t.Logf("Query %d returned %d rows", i, len(result.QueryResult.Values))
		}

		// Small delay between queries
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// testIndexCacheHits generates cache hit metrics by calling get_index on cached streams
func testIndexCacheHits(ctx context.Context, t *testing.T, tnClient *tnclient.Client) error {
	// Get the raw Kwil client from TN client to make direct calls
	kwilClient := tnClient.GetKwilClient()
	signerAddress := tnClient.Address()

	// Get the composed stream ID from environment
	composedStreamId := os.Getenv("COMPOSED_STREAM_1")
	if composedStreamId == "" {
		return fmt.Errorf("COMPOSED_STREAM_1 not set")
	}

	// Query the composed stream multiple times with get_index - these should generate cache hits
	// because the scheduler has already refreshed this stream
	for i := 0; i < 5; i++ {
		args := []any{
			signerAddress.Address(), // data_provider
			composedStreamId,        // stream_id
			int64(1609459200),       // from_time
			int64(1609459400),       // to_time
			nil,                     // frozen_at
			int64(1609459200),       // base_time
			true,                    // use_cache
		}

		// Call get_index - it will internally use cache if available
		result, err := kwilClient.Call(ctx, "", "get_index", args)
		if err != nil {
			return fmt.Errorf("index query %d failed: %w", i, err)
		}

		if result != nil && result.QueryResult != nil && len(result.QueryResult.Values) > 0 {
			t.Logf("Index query %d returned %d rows", i, len(result.QueryResult.Values))
		}

		// Small delay between queries
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// testCacheMisses generates cache miss metrics
func testCacheMisses(ctx context.Context, t *testing.T, tnClient *tnclient.Client) error {
	kwilClient := tnClient.GetKwilClient()
	signerAddress := tnClient.Address()

	// Test 1: Query for time ranges outside the cached range
	// The cache only has data from 1609459200 onwards, so query before that
	composedStreamId := os.Getenv("COMPOSED_STREAM_1")
	if composedStreamId != "" {
		args := []any{
			signerAddress.Address(), // data_provider
			composedStreamId,        // stream_id
			int64(1609459000),       // from_time (before cached range)
			int64(1609459100),       // to_time (before cached range)
			nil,                     // frozen_at
			true,                    // use_cache
		}

		// This should generate a cache miss because the time range is not cached
		result, err := kwilClient.Call(ctx, "", "get_record", args)
		if err != nil {
			t.Logf("Query outside cached range error: %v", err)
		} else if result != nil && result.QueryResult != nil {
			t.Logf("Query outside cached range returned %d rows", len(result.QueryResult.Values))
		}
	}

	// Test 2: Query for streams that are not configured for caching
	// These streams exist but are not in the cache configuration
	uncachedStreamId := "stuncached123456789012345678900"
	for i := 0; i < 3; i++ {
		args := []any{
			signerAddress.Address(), // data_provider
			uncachedStreamId,        // stream_id not in cache config
			int64(1609459200),       // from_time
			int64(1609459400),       // to_time
			nil,                     // frozen_at
			true,                    // use_cache
		}

		// This will generate cache misses because the stream is not configured for caching
		result, err := kwilClient.Call(ctx, "", "get_record", args)
		if err != nil {
			t.Logf("Uncached stream query %d error: %v", i, err)
		} else if result != nil && result.QueryResult != nil {
			t.Logf("Uncached stream query %d returned %d rows", i, len(result.QueryResult.Values))
		}

		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// testIndexCacheMisses generates cache miss metrics for get_index calls
func testIndexCacheMisses(ctx context.Context, t *testing.T, tnClient *tnclient.Client) error {
	kwilClient := tnClient.GetKwilClient()
	signerAddress := tnClient.Address()

	// Test 1: Query for time ranges outside the cached range with get_index
	// The cache only has data from 1609459200 onwards, so query before that
	composedStreamId := os.Getenv("COMPOSED_STREAM_1")
	if composedStreamId != "" {
		args := []any{
			signerAddress.Address(), // data_provider
			composedStreamId,        // stream_id
			int64(1609459000),       // from_time (before cached range)
			int64(1609459100),       // to_time (before cached range)
			nil,                     // frozen_at
			int64(1609459000),       // base_time
			true,                    // use_cache
		}

		// This should generate a cache miss because the time range is not cached
		result, err := kwilClient.Call(ctx, "", "get_index", args)
		if err != nil {
			t.Logf("Index query outside cached range error: %v", err)
		} else if result != nil && result.QueryResult != nil {
			t.Logf("Index query outside cached range returned %d rows", len(result.QueryResult.Values))
		}
	}

	// Test 2: Query for streams that are not configured for caching with get_index
	// These streams exist but are not in the cache configuration
	uncachedStreamId := "stuncached123456789012345678900"
	for i := 0; i < 3; i++ {
		args := []any{
			signerAddress.Address(), // data_provider
			uncachedStreamId,        // stream_id not in cache config
			int64(1609459200),       // from_time
			int64(1609459400),       // to_time
			nil,                     // frozen_at
			int64(1609459200),       // base_time
			true,                    // use_cache
		}

		// This will generate cache misses because the stream is not configured for caching
		result, err := kwilClient.Call(ctx, "", "get_index", args)
		if err != nil {
			t.Logf("Uncached stream index query %d error: %v", i, err)
		} else if result != nil && result.QueryResult != nil {
			t.Logf("Uncached stream index query %d returned %d rows", i, len(result.QueryResult.Values))
		}

		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// testRefreshErrors simulates conditions that cause refresh errors
func testRefreshErrors(ctx context.Context, t *testing.T, tnClient *tnclient.Client) error {
	// The refresh errors are tracked internally by the cache refresh mechanism
	// We can't directly trigger them from the test, but they will be recorded
	// when the refresh scheduler encounters issues (e.g., invalid data, network errors)

	// The refresh error metrics will be populated by the
	// background refresh process that runs according to the cron schedules

	t.Log("Refresh errors are tracked by the background scheduler")
	return nil
}

// waitTxToBeMinedWithSuccess waits for a transaction to be successful
func waitTxToBeMinedWithSuccess(t *testing.T, ctx context.Context, client *tnclient.Client, txHash kwiltypes.Hash) {
	txRes, err := client.WaitForTx(ctx, txHash, time.Second)
	require.NoError(t, err, "Transaction failed")
	require.Equal(t, kwiltypes.CodeOk, kwiltypes.TxCode(txRes.Result.Code), "Transaction code not OK: %s", txRes.Result.Log)
}

// waitDeployTx waits for a deployment transaction to be mined, ignoring duplicate stream errors.
func waitDeployTx(t *testing.T, ctx context.Context, client *tnclient.Client, txHash kwiltypes.Hash) {
	txRes, err := client.WaitForTx(ctx, txHash, time.Second)
	require.NoError(t, err, "Transaction failed")

	if kwiltypes.TxCode(txRes.Result.Code) == kwiltypes.CodeOk {
		return
	}

	if strings.Contains(txRes.Result.Log, `duplicate key value violates unique constraint "streams_pkey"`) {
		t.Logf("Ignoring duplicate stream error for tx %s", txHash)
		return
	}

	require.Equal(t, kwiltypes.CodeOk, kwiltypes.TxCode(txRes.Result.Code), "Transaction code not OK: %s", txRes.Result.Log)
}

// grantNetworkWriterRole grants the network_writer role to the given wallet
func grantNetworkWriterRole(ctx context.Context, t *testing.T, wallet *kwilcrypto.Secp256k1PrivateKey) error {
	// For development, the migration should have already granted network_writers_manager role
	// to the DEV_DB_OWNER (0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf)
	// So we use that wallet to grant network_writer role

	// Create a client with the same wallet (which has network_writers_manager role)
	managerClient, err := tnclient.NewClient(ctx, kwildEndpoint, tnclient.WithSigner(auth.GetUserSigner(wallet)))
	if err != nil {
		return fmt.Errorf("failed to create manager client: %w", err)
	}

	roleMgmt, err := managerClient.LoadRoleManagementActions()
	if err != nil {
		return fmt.Errorf("failed to load role management actions: %w", err)
	}

	pubKey, err := auth.GetUserIdentifier(wallet.Public())
	if err != nil {
		return fmt.Errorf("failed to get user identifier: %w", err)
	}

	addr, err := util.NewEthereumAddressFromString(pubKey)
	if err != nil {
		return fmt.Errorf("failed to convert user identifier to address: %w", err)
	}

	txHash, err := roleMgmt.GrantRole(ctx, types.GrantRoleInput{
		Owner:    "system",
		RoleName: "network_writer",
		Wallets:  []util.EthereumAddress{addr},
	})
	if err != nil {
		return fmt.Errorf("failed to grant role: %w", err)
	}

	waitTxToBeMinedWithSuccess(t, ctx, managerClient, txHash)
	t.Logf("Successfully granted network_writer role to %s", addr.Address())

	return nil
}

// cleanupTestStreams drops all test streams created during the test
func cleanupTestStreams(ctx context.Context, t *testing.T, tnClient *tnclient.Client) error {
	// Get the composed stream IDs from environment
	composedStreamIds := []string{
		os.Getenv("COMPOSED_STREAM_1"),
		os.Getenv("COMPOSED_STREAM_2"),
		os.Getenv("COMPOSED_STREAM_3"),
	}

	// Drop composed streams
	for _, streamId := range composedStreamIds {
		if streamId == "" {
			continue
		}

		streamIdObj, err := util.NewStreamId(streamId)
		if err != nil {
			t.Logf("Warning: Failed to parse stream ID %s: %v", streamId, err)
			continue
		}

		// Get raw Kwil client to call drop_stream action
		kwilClient := tnClient.GetKwilClient()
		signerAddress := tnClient.Address()
		_, err = kwilClient.Execute(ctx, "", "drop_stream", [][]any{{
			signerAddress.Address(), // data_provider
			streamIdObj.String(),    // stream_id
		}})

		if err != nil {
			t.Logf("Warning: Failed to drop composed stream %s: %v", streamId, err)
		} else {
			t.Logf("Dropped composed stream %s", streamId)
		}
	}

	// Drop primitive streams
	primitiveStreamIds := []string{
		"stchild1234567890123456789child1",
		"stchild1234567890123456789child2",
	}

	for _, streamId := range primitiveStreamIds {
		streamIdObj, err := util.NewStreamId(streamId)
		if err != nil {
			t.Logf("Warning: Failed to parse stream ID %s: %v", streamId, err)
			continue
		}

		// Get raw Kwil client to call drop_stream action
		kwilClient := tnClient.GetKwilClient()
		signerAddress := tnClient.Address()
		_, err = kwilClient.Execute(ctx, "", "drop_stream", [][]any{{
			signerAddress.Address(), // data_provider
			streamIdObj.String(),    // stream_id
		}})

		if err != nil {
			t.Logf("Warning: Failed to drop primitive stream %s: %v", streamId, err)
		} else {
			t.Logf("Dropped primitive stream %s", streamId)
		}
	}

	return nil
}
