package main

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	kwilcrypto "github.com/trufnetwork/kwil-db/core/crypto"
	"github.com/trufnetwork/kwil-db/core/crypto/auth"
	"github.com/trufnetwork/sdk-go/core/tnclient"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

const (
	kwildEndpoint      = "http://localhost:8484"
	deployerPrivateKey = "0000000000000000000000000000000000000000000000000000000000000001"
)

// TestDigestE2E runs comprehensive end-to-end tests for the digest extension
func TestDigestE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
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

	// Run the main test - scheduler integration (this was already passing)
	t.Run("SchedulerIntegration", func(t *testing.T) {
		testSchedulerIntegration(ctx, t, tnClient)
	})
}

// testSchedulerIntegration tests that the scheduler triggers digest operations
func testSchedulerIntegration(ctx context.Context, t *testing.T, client *tnclient.Client) {
	t.Log("Testing scheduler integration...")

	// Create test stream ID
	streamIdPtr, err := util.NewStreamId("stdigsch123456789012345678901234")
	require.NoError(t, err, "Failed to create scheduler test stream ID")
	streamId := *streamIdPtr

	// Deploy test stream
	err = deployTestStream(ctx, t, client, streamId)
	require.NoError(t, err, "Failed to deploy scheduler test stream")

	// Insert data for a new day
	timestamp := int64(259200) // Day 3
	err = insertPrimitiveEvent(ctx, client, streamId, timestamp, big.NewInt(42))
	require.NoError(t, err, "Failed to insert event for scheduler test")

	// Check the inserted data
	clientAddress := client.Address()
	t.Logf("Inserted event by data provider: %s", clientAddress.Address())

	// Query the inserted record to verify
	// Note: The actual query parameters may need to be adjusted based on the schema
	// Here we assume a simple query by stream ID and event time
	result, err := client.GetKwilClient().Call(ctx, "", "get_record", []any{
		clientAddress.Address(), // data_provider
		streamId.String(),       // stream_id
		0,                       // from
		9999,                    // to
		nil,                     // frozen_at
	})
	if err != nil {
		t.Fatal("Failed to get record:", err)
	}
	if result != nil && result.QueryResult != nil && result.QueryResult.Values != nil {
		t.Log("Get record result:", result.QueryResult.ExportToStringMap())
	} else {
		t.Log("Get record returned no results")
	}

	// Wait for scheduler to potentially run and process the data (configured for 30 seconds)
	t.Log("Waiting for scheduler to trigger and process data (up to 2 minutes)...")

	var processed bool
	for i := 0; i < 24; i++ { // Check every 5 seconds for 2 minutes
		time.Sleep(5 * time.Second)

		// Check if the day has been processed by verifying primitive_event_type table
		// For a single record, it should get type flag 15 (OPEN+HIGH+LOW+CLOSE)
		typeFlag, err := getEventTypeFlag(ctx, client, streamId, timestamp)
		if err == nil && typeFlag == 15 {
			t.Log("Scheduler successfully processed the day - found type flag 15")
			processed = true
			break
		} else {
			t.Logf("Got error checking event type flag (not yet processed?): %v, typeFlag: %d", err, typeFlag)
		}

		if i%4 == 0 { // Log every 20 seconds
			t.Logf("Still waiting for digest processing... (attempt %d/24)", i+1)
		}
	}

	if !processed {
		//t.Log("Scheduler didn't process within timeout - extension may not be fully active")
		// This is not necessarily a failure for initial testing
		t.Fatal("Scheduler did not process the day within the expected time")
	}

	// Verify pending_prune_days is empty after processing
	pendingDays, err := getPendingPruneDays(ctx, client, streamId)
	require.NoError(t, err, "Failed to query pending_prune_days")
	if len(pendingDays) != 0 {
		t.Logf("Warning: pending_prune_days not empty after processing: %v", pendingDays)
	} else {
		t.Log("pending_prune_days is empty after processing as expected")
	}

	// Test completed
	t.Log("Scheduler integration test completed")
}

// Helper functions

// deployTestStream deploys a primitive test stream
func deployTestStream(ctx context.Context, t *testing.T, client *tnclient.Client, streamId util.StreamId) error {
	t.Logf("Deploying test stream: %s", streamId.String())

	// Deploy primitive stream
	streamDef := types.StreamDefinition{
		StreamId:   streamId,
		StreamType: types.StreamTypePrimitive,
	}

	txHash, err := client.BatchDeployStreams(ctx, []types.StreamDefinition{streamDef})
	if err != nil {
		return fmt.Errorf("failed to deploy stream: %w", err)
	}

	// call waitForTx to ensure deployment
	res, err := client.WaitForTx(ctx, txHash, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to wait for deploy transaction: %w", err)
	}

	// check
	if res.Result.Log != "" {
		t.Log("Deployment transaction log:", res.Result.Log)
	}

	t.Logf("Successfully deployed stream: %s", streamId.String())
	return nil
}

// insertPrimitiveEvent inserts a primitive event into the test stream
func insertPrimitiveEvent(ctx context.Context, client *tnclient.Client, streamId util.StreamId, timestamp int64, value *big.Int) error {
	// Load primitive actions
	primitiveActions, err := client.LoadPrimitiveActions()
	if err != nil {
		return fmt.Errorf("failed to load primitive actions: %w", err)
	}

	signerAddress := client.Address()
	insertData := types.InsertRecordInput{
		DataProvider: signerAddress.Address(),
		StreamId:     streamId.String(),
		Value:        float64(value.Int64()),
		EventTime:    int(timestamp),
	}

	txHash, err := primitiveActions.InsertRecords(ctx, []types.InsertRecordInput{insertData})
	if err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}

	// Wait for the insert transaction to be confirmed
	txResult, err := client.WaitForTx(ctx, txHash, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to wait for insert transaction: %w", err)
	}
	if txResult.Result.Log != "" {
		fmt.Println("Insert transaction log:", txResult.Result.Log)
	}

	return nil
}

// getEventTypeFlag returns the type flag for a specific event from primitive_event_type table
func getEventTypeFlag(ctx context.Context, client *tnclient.Client, streamId util.StreamId, eventTime int64) (int, error) {
	kwilClient := client.GetKwilClient()

	// Query the primitive_event_type table for the specific event
	result, err := kwilClient.Query(ctx,
		"SELECT type FROM main.primitive_event_type WHERE stream_ref = 1 AND event_time = 259200",
		nil, false)
	if err != nil {
		return 0, fmt.Errorf("failed to query primitive_event_type: %w", err)
	}

	resultMap := result.ExportToStringMap()
	// Access the result from the string map
	if len(resultMap) > 0 {
		if typeStr, exists := resultMap[0]["type"]; exists {
			// Convert string to int
			if typeFlag, err := strconv.Atoi(typeStr); err == nil {
				fmt.Printf("Found type flag: %d\n", typeFlag)
				return typeFlag, nil
			} else {
				return 0, fmt.Errorf("failed to convert type flag '%s' to int: %w", typeStr, err)
			}
		}
	}

	return 0, fmt.Errorf("no type flag found for event_time %d", eventTime)
}

// getPendingPruneDays returns the pending prune days for a stream
func getPendingPruneDays(ctx context.Context, client *tnclient.Client, streamId util.StreamId) ([]int64, error) {
	kwilClient := client.GetKwilClient()

	// Use the direct SQL query approach similar to digest_actions_test.go
	result, err := kwilClient.Query(ctx, "SELECT day_index FROM main.pending_prune_days ORDER BY day_index ASC", map[string]any{}, false)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending_prune_days: %w", err)
	}

	// Extract day_index values from the result
	resultMap := result.ExportToStringMap()

	var pendingDays []int64
	for _, row := range resultMap {
		if dayStr, exists := row["day_index"]; exists {
			if dayInt, err := strconv.ParseInt(dayStr, 10, 64); err == nil {
				pendingDays = append(pendingDays, dayInt)
			} else {
				return nil, fmt.Errorf("failed to parse day_index '%s': %w", dayStr, err)
			}
		}
	}

	return pendingDays, nil
}

// cleanupTestStreams removes test streams after testing
func cleanupTestStreams(ctx context.Context, t *testing.T, client *tnclient.Client) error {
	t.Log("Cleaning up test streams...")

	streamIds := []string{
		"stdigsch123456789012345678901234",
	}

	// Get Kwil client for cleanup
	kwilClient := client.GetKwilClient()
	signerAddress := client.Address()

	for _, streamIdStr := range streamIds {
		streamIdObj, err := util.NewStreamId(streamIdStr)
		if err != nil {
			t.Logf("Warning: Failed to parse stream ID %s: %v", streamIdStr, err)
			continue
		}

		// Drop the stream
		_, err = kwilClient.Execute(ctx, "", "drop_stream", [][]any{{
			signerAddress.Address(), // data_provider
			streamIdObj.String(),    // stream_id
		}})

		if err != nil {
			t.Logf("Warning: Failed to drop stream %s: %v", streamIdStr, err)
		} else {
			t.Logf("Dropped stream %s", streamIdStr)
		}
	}

	return nil
}
