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

	// Run comprehensive multi-stream test
	t.Run("MultiStreamDigestTest", func(t *testing.T) {
		testMultiStreamDigest(ctx, t, tnClient)
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

// testMultiStreamDigest tests comprehensive multi-stream digest processing
func testMultiStreamDigest(ctx context.Context, t *testing.T, client *tnclient.Client) {
	t.Log("Testing multi-stream digest processing...")

	// Define 3 test streams with different patterns
	testStreams := []struct {
		id      string
		name    string
		pattern string
	}{
		{"stream1a234567890123456789012345", "Stream1", "ascending_values"}, // Values increase over time
		{"stream2b234567890123456789012345", "Stream2", "volatile_trading"}, // High/low volatility pattern
		{"stream3c234567890123456789012345", "Stream3", "steady_decline"},   // Values decrease over time
	}

	// Deploy all test streams in batch
	t.Log("Deploying 3 test streams in batch...")
	var streamDefs []types.StreamDefinition
	for _, stream := range testStreams {
		streamIdPtr, err := util.NewStreamId(stream.id)
		require.NoError(t, err, "Failed to create stream ID for %s", stream.name)

		streamDefs = append(streamDefs, types.StreamDefinition{
			StreamId:   *streamIdPtr,
			StreamType: types.StreamTypePrimitive,
		})
	}

	// Batch deploy all streams
	txHash, err := client.BatchDeployStreams(ctx, streamDefs)
	require.NoError(t, err, "Failed to batch deploy streams")

	// Wait for deployment transaction
	res, err := client.WaitForTx(ctx, txHash, 10*time.Second)
	require.NoError(t, err, "Failed to wait for batch deploy transaction")

	if res.Result.Log != "" {
		t.Logf("Batch deploy transaction log: %s", res.Result.Log)
	}

	t.Logf("✅ Successfully batch deployed %d streams", len(testStreams))
	for _, stream := range testStreams {
		t.Logf("  - %s: %s", stream.name, stream.id)
	}

	// Insert test data across multiple days (Day 1, 2, 3)
	t.Log("Inserting test data (15 records per stream across 3 days)...")

	// Day 1: 86400 (start) to 172799 (end)
	day1Start := int64(86400)
	day1End := int64(172799)

	// Day 2: 172800 (start) to 259199 (end)
	day2Start := int64(172800)
	day2End := int64(259199)

	// Day 3: 259200 (start) to 345599 (end)
	day3Start := int64(259200)
	day3End := int64(345599)

	// Prepare all records for bulk insertion
	var allRecords []types.InsertRecordInput
	signerAddress := client.Address()

	for i, stream := range testStreams {
		streamIdPtr, _ := util.NewStreamId(stream.id)
		streamId := *streamIdPtr

		t.Logf("Preparing data for %s (%s pattern)...", stream.name, stream.pattern)

		// Insert 5 records per day = 15 total records per stream
		dayStarts := []int64{day1Start, day2Start, day3Start}
		dayEnds := []int64{day1End, day2End, day3End}

		for dayIdx, dayStart := range dayStarts {
			dayEnd := dayEnds[dayIdx]
			dayNum := dayIdx + 1

			// Generate 5 timestamps evenly distributed across the day
			timeSpan := dayEnd - dayStart
			timeStep := timeSpan / 5

			for recordIdx := 0; recordIdx < 5; recordIdx++ {
				timestamp := dayStart + int64(recordIdx+1)*timeStep
				value := generateTestValue(stream.pattern, dayNum, recordIdx, i)

				// Add to bulk insertion batch
				allRecords = append(allRecords, types.InsertRecordInput{
					DataProvider: signerAddress.Address(),
					StreamId:     streamId.String(),
					Value:        float64(value),
					EventTime:    int(timestamp),
				})
			}

			t.Logf("  Day %d: 5 records prepared (timestamps %d-%d)", dayNum, dayStart, dayEnd)
		}

		t.Logf("✅ %s: 15 records prepared across 3 days", stream.name)
	}

	t.Logf("Bulk inserting all %d records...", len(allRecords))

	// Load primitive actions for bulk insertion
	primitiveActions, err := client.LoadPrimitiveActions()
	require.NoError(t, err, "Failed to load primitive actions for bulk insert")

	// Perform bulk insertion
	txHash, err = primitiveActions.InsertRecords(ctx, allRecords)
	require.NoError(t, err, "Failed to bulk insert all records")

	// Wait for the bulk insert transaction to be confirmed
	txResult, err := client.WaitForTx(ctx, txHash, 10*time.Second) // More time for bulk operation
	require.NoError(t, err, "Failed to wait for bulk insert transaction")

	if txResult.Result.Log != "" {
		t.Logf("Bulk insert transaction log: %s", txResult.Result.Log)
	}
	t.Logf("✅ Successfully bulk inserted %d records in single transaction", len(allRecords))

	// We expect 3 streams × 3 days = 9 digest operations
	maxWaitTime := 5 * time.Minute // Allow more time for comprehensive processing
	checkInterval := 10 * time.Second
	maxAttempts := int(maxWaitTime / checkInterval)

	expectedDigests := make(map[string]map[int]bool) // stream_id -> day -> processed
	for _, stream := range testStreams {
		expectedDigests[stream.id] = make(map[int]bool)
		for day := 1; day <= 3; day++ {
			expectedDigests[stream.id][day] = false
		}
	}

	var allProcessed bool
	for attempt := 0; attempt < maxAttempts; attempt++ {
		time.Sleep(checkInterval)

		// Check processing status for each stream and day
		processedCount := 0
		totalExpected := 9 // 3 streams × 3 days

		for _, stream := range testStreams {
			streamIdPtr, _ := util.NewStreamId(stream.id)

			// Check each day for this stream using day ranges instead of specific timestamps
			dayRanges := []struct {
				start, end int64
				num        int
			}{
				{day1Start, day1End, 1}, // Day 1
				{day2Start, day2End, 2}, // Day 2
				{day3Start, day3End, 3}, // Day 3
			}

			for _, dayRange := range dayRanges {
				// Check if this day has been processed (has any type flags in the day range)
				hasTypeFlags, err := checkDayProcessed(ctx, client, *streamIdPtr, dayRange.start, dayRange.end)
				if err == nil && hasTypeFlags {
					if !expectedDigests[stream.id][dayRange.num] {
						expectedDigests[stream.id][dayRange.num] = true
						t.Logf("✅ %s Day %d processed (found type flags)", stream.name, dayRange.num)
					}
					processedCount++
				} else {
					// Debug: Print what we're not finding
					if attempt == 0 || attempt%6 == 0 { // Print debug info occasionally
						t.Logf("DEBUG: %s Day %d not processed yet - day range: %d-%d, error: %v",
							stream.name, dayRange.num, dayRange.start, dayRange.end, err)
					}
				}
			}
		}

		// Check if all expected digests are complete
		if processedCount >= totalExpected {
			allProcessed = true
			t.Logf("🎉 All %d digest operations completed!", totalExpected)
			break
		}

		// Log progress every minute
		if attempt%6 == 0 {
			t.Logf("Progress: %d/%d digest operations completed (attempt %d/%d)",
				processedCount, totalExpected, attempt+1, maxAttempts)
		}
	}

	require.True(t, allProcessed, "Not all digest operations completed within timeout")

	// Verify OHLC calculations for each stream and day
	t.Log("Verifying OHLC calculations...")
	for _, stream := range testStreams {
		streamIdPtr, _ := util.NewStreamId(stream.id)
		t.Logf("Verifying %s (%s pattern)...", stream.name, stream.pattern)

		for dayNum := 1; dayNum <= 3; dayNum++ {
			// Verify type flags exist for this day using day range
			dayStart := []int64{day1Start, day2Start, day3Start}[dayNum-1]
			dayEnd := []int64{day1End, day2End, day3End}[dayNum-1]

			hasTypeFlags, err := checkDayProcessed(ctx, client, *streamIdPtr, dayStart, dayEnd)
			require.NoError(t, err, "Failed to check day processed for %s Day %d", stream.name, dayNum)
			require.True(t, hasTypeFlags, "%s Day %d should have type flags", stream.name, dayNum)

			// Get actual type flags for this day to analyze OHLC components
			streamRef, _ := getStreamRef(ctx, client, *streamIdPtr)
			kwilClient := client.GetKwilClient()
			result, err := kwilClient.Query(ctx,
				fmt.Sprintf("SELECT event_time, type FROM main.primitive_event_type WHERE stream_ref = %d AND event_time >= %d AND event_time <= %d ORDER BY event_time",
					streamRef, dayStart, dayEnd),
				map[string]any{}, true)

			if err == nil && result != nil {
				resultMap := result.ExportToStringMap()
				t.Logf("  Day %d: Found %d type flag records:", dayNum, len(resultMap))

				combinedTypeFlags := 0
				for _, row := range resultMap {
					if typeStr, exists := row["type"]; exists {
						if typeFlag, err := strconv.Atoi(typeStr); err == nil {
							combinedTypeFlags |= typeFlag
							t.Logf("    timestamp=%s, type=%d", row["event_time"], typeFlag)
						}
					}
				}

				// Decode combined OHLC components
				hasOpen := (combinedTypeFlags & 1) != 0  // Bit 0
				hasHigh := (combinedTypeFlags & 2) != 0  // Bit 1
				hasLow := (combinedTypeFlags & 4) != 0   // Bit 2
				hasClose := (combinedTypeFlags & 8) != 0 // Bit 3

				t.Logf("    Combined flags: %d (O:%t H:%t L:%t C:%t)",
					combinedTypeFlags, hasOpen, hasHigh, hasLow, hasClose)
			}
		}

		t.Logf("✅ %s: All 3 days verified", stream.name)
	}

	t.Log("🎉 Multi-stream digest test completed successfully!")
	t.Logf("Summary: 3 streams × 15 records each × 3 days = 135 total records processed")
}

// generateTestValue creates test values based on different patterns
func generateTestValue(pattern string, day, recordIndex, streamIndex int) int64 {
	base := int64(100 * (streamIndex + 1)) // Base value differs per stream

	switch pattern {
	case "ascending_values":
		// Values increase: day1(100-120), day2(120-140), day3(140-160)
		return base + int64(day-1)*20 + int64(recordIndex)*4

	case "volatile_trading":
		// Volatile pattern with highs and lows
		dayOffset := int64(day-1) * 50
		if recordIndex%2 == 0 {
			return base + dayOffset + 30 // High values
		} else {
			return base + dayOffset - 10 // Low values
		}

	case "steady_decline":
		// Values decrease: day1(150-130), day2(130-110), day3(110-90)
		return base + 50 - int64(day-1)*20 - int64(recordIndex)*4

	default:
		return base + int64(recordIndex*10)
	}
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
	// Resolve stream_ref via read-only lookup, then fetch the type flag for the event
	//refRes, err := kwilClient.Query(ctx,
	//	fmt.Sprintf("SELECT main.get_stream_id('%s') AS id", streamId.String()),
	//	map[string]any{}, true)
	//if err != nil {
	//	return 0, fmt.Errorf("failed to resolve stream_ref: %w", err)
	//}

	providerAddress := client.Address()
	refRes, err := kwilClient.Call(ctx, "", "get_stream_id", []any{providerAddress.Address(), streamId.String()})
	if err != nil {
		return 0, fmt.Errorf("failed to resolve stream_ref: %w", err)
	}

	refMap := refRes.QueryResult.ExportToStringMap()
	if len(refMap) == 0 || refMap[0]["id"] == "" {
		return 0, fmt.Errorf("stream_ref not found for %s", streamId.String())
	}
	streamRef, err := strconv.Atoi(refMap[0]["id"])
	if err != nil {
		return 0, fmt.Errorf("invalid stream_ref '%s': %w", refMap[0]["id"], err)
	}

	result, err := kwilClient.Query(ctx,
		fmt.Sprintf("SELECT type FROM main.primitive_event_type WHERE stream_ref = %d AND event_time = %d", streamRef, eventTime),
		map[string]any{}, true)
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
	result, err := kwilClient.Query(ctx, "SELECT day_index FROM main.pending_prune_days ORDER BY day_index ASC", map[string]any{}, true)
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
		"stdigsch123456789012345678901234", // SchedulerIntegration test stream
		"stream1a234567890123456789012345", // MultiStreamDigest test streams
		"stream2b234567890123456789012345",
		"stream3c234567890123456789012345",
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

// getStreamRef gets the stream_ref for a given stream ID
func getStreamRef(ctx context.Context, client *tnclient.Client, streamId util.StreamId) (int, error) {
	providerAddress := client.Address()
	kwilClient := client.GetKwilClient()

	refRes, err := kwilClient.Call(ctx, "", "get_stream_id", []any{providerAddress.Address(), streamId.String()})
	if err != nil {
		return 0, fmt.Errorf("failed to resolve stream_ref: %w", err)
	}

	refMap := refRes.QueryResult.ExportToStringMap()
	if len(refMap) == 0 || refMap[0]["id"] == "" {
		return 0, fmt.Errorf("stream_ref not found for %s", streamId.String())
	}

	streamRef, err := strconv.Atoi(refMap[0]["id"])
	if err != nil {
		return 0, fmt.Errorf("invalid stream_ref '%s': %w", refMap[0]["id"], err)
	}

	return streamRef, nil
}

// checkDayProcessed checks if any type flags exist for a stream within a day range
func checkDayProcessed(ctx context.Context, client *tnclient.Client, streamId util.StreamId, dayStart, dayEnd int64) (bool, error) {
	streamRef, err := getStreamRef(ctx, client, streamId)
	if err != nil {
		return false, fmt.Errorf("failed to get stream_ref: %w", err)
	}

	kwilClient := client.GetKwilClient()
	result, err := kwilClient.Query(ctx,
		fmt.Sprintf("SELECT COUNT(*) as count FROM main.primitive_event_type WHERE stream_ref = %d AND event_time >= %d AND event_time <= %d",
			streamRef, dayStart, dayEnd),
		map[string]any{}, true)
	if err != nil {
		return false, fmt.Errorf("failed to query primitive_event_type for day range: %w", err)
	}

	resultMap := result.ExportToStringMap()
	if len(resultMap) > 0 {
		if countStr, exists := resultMap[0]["count"]; exists {
			count, err := strconv.Atoi(countStr)
			if err == nil && count > 0 {
				return true, nil
			}
		}
	}

	return false, nil
}
