package internal

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/util"
)

// TestTNOperations_Integration tests the TN operations against the kwil testing framework
// which sets up a real TN instance with all procedures loaded
// Test helpers for integration tests

// generateTestStreamID generates a unique stream ID for testing
func generateTestStreamID(prefix string) util.StreamId {
	timestamp := time.Now().UnixNano()
	return util.GenerateStreamId(fmt.Sprintf("%s_%d", prefix, timestamp))
}

// setupComposedStreamData is a helper to setup composed stream test data
type setupComposedStreamData struct {
	streamID     util.StreamId
	columnPrefix string
	values       [][]int // Each row is [eventTime, val1, val2, ...]
}

// setupComposedStream creates a composed stream with the given data
func setupComposedStream(ctx context.Context, platform *kwilTesting.Platform, data setupComposedStreamData) error {
	// Build markdown table dynamically
	numCols := len(data.values[0]) - 1 // Exclude event_time

	// Build header
	header := "| event_time"
	separator := "|----------"
	for i := 0; i < numCols; i++ {
		header += fmt.Sprintf(" | %s_%d", data.columnPrefix, i+1)
		separator += "|---------"
	}
	header += " |"
	separator += "|"

	// Build rows
	rows := []string{header, separator}
	for _, row := range data.values {
		rowStr := fmt.Sprintf("| %d", row[0])
		for i := 1; i < len(row); i++ {
			rowStr += fmt.Sprintf(" | %d", row[i])
		}
		rowStr += " |"
		rows = append(rows, rowStr)
	}

	markdownData := strings.Join(rows, "\n")

	// Setup the stream
	return setup.SetupComposedFromMarkdown(ctx, setup.MarkdownComposedSetupInput{
		Platform:     platform,
		StreamId:     data.streamID,
		MarkdownData: markdownData,
		Height:       1,
	})
}

func TestTNOperations_Integration(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "tn_ops_integration_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testTNOperationsIntegration(t),
		},
	}, testutils.GetTestOptions())
}

func testTNOperationsIntegration(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create test data provider
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000123")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())
		logger := log.NewStdoutLogger().New("tn_ops_test")

		// Test 1: ListComposedStreams
		t.Run("ListComposedStreams", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			tnOps := NewTNOperations(txPlatform.Engine, txPlatform.DB, "main", logger)

			// Create test streams
			stream1 := setupComposedStreamData{
				streamID:     generateTestStreamID("test_composed_1"),
				columnPrefix: "val_a",
				values: [][]int{
					{100, 10, 20},
					{200, 15, 25},
					{300, 20, 30},
				},
			}

			stream2 := setupComposedStreamData{
				streamID:     generateTestStreamID("test_composed_2"),
				columnPrefix: "val_b",
				values: [][]int{
					{100, 100, 200, 300},
					{200, 150, 250, 350},
				},
			}

			// Setup streams
			require.NoError(t, setupComposedStream(ctx, txPlatform, stream1))
			require.NoError(t, setupComposedStream(ctx, txPlatform, stream2))

			// Test ListComposedStreams
			streams, err := tnOps.ListComposedStreams(ctx, deployer.Address())
			require.NoError(t, err)

			// Verify results
			assert.GreaterOrEqual(t, len(streams), 2, "Should have at least 2 composed streams")

			// Check that our streams are in the list
			streamIDs := map[string]bool{stream1.streamID.String(): false, stream2.streamID.String(): false}
			for _, stream := range streams {
				if _, exists := streamIDs[stream]; exists {
					streamIDs[stream] = true
				}
			}
			assert.True(t, streamIDs[stream1.streamID.String()], "Should find first composed stream")
			assert.True(t, streamIDs[stream2.streamID.String()], "Should find second composed stream")
		}))

		// Test GetCategoryStreams
		t.Run("GetCategoryStreams", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			tnOps := NewTNOperations(txPlatform.Engine, txPlatform.DB, "main", logger)

			// Create a composed stream with known structure
			streamData := setupComposedStreamData{
				streamID:     generateTestStreamID("test_composed_cat"),
				columnPrefix: "cat_val",
				values: [][]int{
					{100, 10, 20},
					{200, 15, 25},
				},
			}

			require.NoError(t, setupComposedStream(ctx, txPlatform, streamData))

			// Get child streams for the composed stream
			categoryStreams, err := tnOps.GetCategoryStreams(ctx, deployer.Address(), streamData.streamID.String(), 0)
			require.NoError(t, err)

			// Filter out the parent stream to get only children
			var childStreams []CategoryStream
			for _, cs := range categoryStreams {
				if cs.StreamID != streamData.streamID.String() {
					childStreams = append(childStreams, cs)
				}
			}

			// Verify we have the expected number of child streams (2 columns = 2 children)
			assert.Len(t, childStreams, 2, "Should have 2 child streams")

			// Verify child stream structure
			for _, child := range childStreams {
				assert.NotEmpty(t, child.DataProvider)
				assert.NotEmpty(t, child.StreamID)
				// Child stream IDs are SHA256 hashes, so they won't contain readable prefixes
				assert.Regexp(t, "^st[a-f0-9]{30}$", child.StreamID, "Child stream ID should be a valid hash")
			}
		}))

		// Test GetRecordComposed
		t.Run("GetRecordComposed", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			tnOps := NewTNOperations(txPlatform.Engine, txPlatform.DB, "main", logger)

			// Create composed stream with test data
			streamData := setupComposedStreamData{
				streamID:     generateTestStreamID("test_composed_rec"),
				columnPrefix: "rec_val",
				values: [][]int{
					{100, 10, 20},
					{200, 15, 25},
					{300, 20, 30},
				},
			}

			require.NoError(t, setupComposedStream(ctx, txPlatform, streamData))

			// Test fetching all records
			fromTime := int64(100)
			toTime := int64(300)

			records, err := tnOps.GetRecordComposed(ctx, deployer.Address(), streamData.streamID.String(), &fromTime, &toTime)
			require.NoError(t, err)

			// Should have 3 records
			assert.Len(t, records, 3, "Should have 3 records")

			// Verify records are in order and have correct structure
			expectedTimes := []int64{100, 200, 300}
			expectedValues := []float64{15.0, 20.0, 25.0} // Average of two columns

			for i, record := range records {
				assert.Equal(t, expectedTimes[i], record.EventTime, "Event time should match")
				assert.NotNil(t, record.Value, "Value should not be nil")

				// Verify aggregated value (average)
				val, err := record.Value.Float64()
				require.NoError(t, err)
				assert.InDelta(t, expectedValues[i], val, 0.001, "Aggregated value should match expected average")
			}

			// Test with time range filter (including anchor record behavior)
			fromTime = int64(250) // Between 200 and 300
			toTime = int64(300)   // Up to 300

			records, err = tnOps.GetRecordComposed(ctx, deployer.Address(), streamData.streamID.String(), &fromTime, &toTime)
			require.NoError(t, err)

			// Should have 2 records: anchor at 200 and record at 300
			assert.Len(t, records, 2, "Should have 2 records (anchor + in-range)")
			if len(records) >= 2 {
				assert.Equal(t, int64(200), records[0].EventTime, "First should be anchor record")
				assert.Equal(t, int64(300), records[1].EventTime, "Second should be in-range record")
			}
		}))

		// Test with composed stream that has 3 children
		t.Run("GetRecordComposed_ThreeChildren", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			tnOps := NewTNOperations(txPlatform.Engine, txPlatform.DB, "main", logger)

			// Create composed stream with 3 columns
			streamData := setupComposedStreamData{
				streamID:     generateTestStreamID("test_composed_3ch"),
				columnPrefix: "ch3_val",
				values: [][]int{
					{100, 100, 200, 300},
					{200, 150, 250, 350},
				},
			}

			require.NoError(t, setupComposedStream(ctx, txPlatform, streamData))

			fromTime := int64(100)
			toTime := int64(200)

			records, err := tnOps.GetRecordComposed(ctx, deployer.Address(), streamData.streamID.String(), &fromTime, &toTime)
			require.NoError(t, err)

			// Should have 2 records
			assert.Len(t, records, 2, "Should have 2 records")

			// Verify aggregated values
			expectedValues := []float64{200.0, 250.0} // Average of three columns

			for i, record := range records {
				val, err := record.Value.Float64()
				require.NoError(t, err)
				assert.InDelta(t, expectedValues[i], val, 0.001, fmt.Sprintf("Record %d aggregated value should match", i))
			}
		}))

		// Test error cases
		t.Run("ErrorCases", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			tnOps := NewTNOperations(txPlatform.Engine, txPlatform.DB, "main", logger)

			testCases := []struct {
				name     string
				testFunc func() (interface{}, error)
				isEmpty  func(interface{}) bool
			}{
				{
					name: "non-existent provider",
					testFunc: func() (interface{}, error) {
						return tnOps.ListComposedStreams(ctx, "0xNonExistentProvider")
					},
					isEmpty: func(v interface{}) bool {
						return len(v.([]string)) == 0
					},
				},
				{
					name: "non-existent stream",
					testFunc: func() (interface{}, error) {
						return tnOps.GetRecordComposed(ctx, deployer.Address(), "non_existent_stream", nil, nil)
					},
					isEmpty: func(v interface{}) bool {
						return len(v.([]ComposedRecord)) == 0
					},
				},
				{
					name: "invalid stream for categories",
					testFunc: func() (interface{}, error) {
						return tnOps.GetCategoryStreams(ctx, deployer.Address(), "invalid_stream", 0)
					},
					isEmpty: func(v interface{}) bool {
						return len(v.([]CategoryStream)) == 0
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					result, err := tc.testFunc()
					require.NoError(t, err, "Should not error, just return empty")
					assert.True(t, tc.isEmpty(result), "Should return empty result")
				})
			}
		}))

		// Test with primitive stream (should not appear in composed streams list)
		t.Run("PrimitiveStreamExclusion", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			tnOps := NewTNOperations(txPlatform.Engine, txPlatform.DB, "main", logger)

			// Create a primitive stream
			primitiveStreamId := generateTestStreamID("test_primitive")

			err = setup.SetupPrimitiveFromMarkdown(ctx, setup.MarkdownPrimitiveSetupInput{
				Platform: txPlatform,
				StreamId: primitiveStreamId,
				MarkdownData: `
				| event_time | value |
				|------------|-------|
				| 100        | 1000  |
				`,
				Height: 1,
			})
			require.NoError(t, err)

			// List composed streams
			streams, err := tnOps.ListComposedStreams(ctx, deployer.Address())
			require.NoError(t, err)

			// Verify primitive stream is not in the list
			for _, stream := range streams {
				assert.NotEqual(t, primitiveStreamId.String(), stream,
					"Primitive stream should not appear in composed streams list")
			}
		}))

		return nil
	}
}

// TestTNOperations_RealTimeData tests TN operations with streams that have real-time data updates
func TestTNOperations_RealTimeData(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "tn_ops_realtime_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testTNOperationsRealTime(t),
		},
	}, testutils.GetTestOptions())
}

func testTNOperationsRealTime(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		deployer, err := util.NewEthereumAddressFromString("0x0000000000000000000000000000000000000456")
		require.NoError(t, err)

		platform = procedure.WithSigner(platform, deployer.Bytes())
		logger := log.NewStdoutLogger()

		// Test fetching recent data
		t.Run("FetchRecentData", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			tnOps := NewTNOperations(txPlatform.Engine, txPlatform.DB, "main", logger)

			// Create stream with recent timestamps
			currentTime := time.Now().Unix()
			streamData := setupComposedStreamData{
				streamID:     generateTestStreamID("realtime_composed"),
				columnPrefix: "rt_val",
				values: [][]int{
					{int(currentTime - 300), 100, 200},
					{int(currentTime - 200), 150, 250},
					{int(currentTime - 100), 200, 300},
				},
			}

			require.NoError(t, setupComposedStream(ctx, txPlatform, streamData))

			// Query recent data
			fromTime := currentTime - 400
			toTime := currentTime

			records, err := tnOps.GetRecordComposed(ctx, deployer.Address(), streamData.streamID.String(), &fromTime, &toTime)
			require.NoError(t, err)

			// Should have all 3 records
			assert.Len(t, records, 3, "Should have 3 recent records")

			// Verify timestamps are in expected range
			for _, record := range records {
				assert.GreaterOrEqual(t, record.EventTime, fromTime)
				assert.LessOrEqual(t, record.EventTime, toTime)
			}

			// Verify values
			expectedValues := []float64{150.0, 200.0, 250.0} // Averages
			for i, record := range records {
				val, err := record.Value.Float64()
				require.NoError(t, err)
				assert.InDelta(t, expectedValues[i], val, 0.001)
			}
		}))

		// Test with future time range (should return empty)
		t.Run("FutureTimeRange", testutils.WithTx(platform, func(t *testing.T, txPlatform *kwilTesting.Platform) {
			tnOps := NewTNOperations(txPlatform.Engine, txPlatform.DB, "main", logger)

			// Create stream with current timestamp
			currentTime := time.Now().Unix()
			streamData := setupComposedStreamData{
				streamID:     generateTestStreamID("future_test"),
				columnPrefix: "ft_val",
				values: [][]int{
					{int(currentTime - 100), 100, 200},
				},
			}

			require.NoError(t, setupComposedStream(ctx, txPlatform, streamData))

			// Query with future time range
			fromTime := currentTime + 1000
			toTime := currentTime + 2000

			records, err := tnOps.GetRecordComposed(ctx, deployer.Address(), streamData.streamID.String(), &fromTime, &toTime)
			require.NoError(t, err)

			// TN returns the anchor record (last known value) even for future queries
			// This is expected behavior - it shows the current state
			assert.Len(t, records, 1, "Should have 1 anchor record for future time range")
			if len(records) > 0 {
				// The record should be from the past (current state)
				assert.Less(t, records[0].EventTime, fromTime, "Anchor record should be from before the future range")
				assert.Equal(t, int64(currentTime-100), records[0].EventTime, "Should be the last known record")
			}
		}))

		return nil
	}
}
