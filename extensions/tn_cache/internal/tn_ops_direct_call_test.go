package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
)

// TestTNOperations_DirectCalls tests TN operations with direct engine calls
// This is a more focused test that validates the exact behavior of each TN action
// Test helpers for direct call tests

// directCallTest represents a test case for direct TN calls
type directCallTest struct {
	name       string
	action     string
	args       []any
	setupFunc  func(*kwilTesting.Platform)
	verifyFunc func(*testing.T, [][]any)
}

// executeDirectCall executes a direct call and collects results
func executeDirectCall(ctx context.Context, platform *kwilTesting.Platform, action string, args []any) ([][]any, error) {
	var results [][]any

	_, err := platform.Engine.CallWithoutEngineCtx(
		ctx,
		platform.DB,
		"main",
		action,
		args,
		func(row *common.Row) error {
			results = append(results, row.Values)
			return nil
		},
	)

	return results, err
}

// executeDirectCallWithEngineCtx executes a direct call with engine context
func executeDirectCallWithEngineCtx(ctx context.Context, platform *kwilTesting.Platform, action string, args []any) ([][]any, error) {
	var results [][]any

	engineCtx := &common.EngineContext{
		TxContext: &common.TxContext{
			Ctx: ctx,
			BlockContext: &common.BlockContext{
				Height: 0,
			},
			Signer: []byte("test"),
			Caller: "test",
			TxID:   platform.Txid(),
		},
		OverrideAuthz: true,
	}

	_, err := platform.Engine.Call(
		engineCtx,
		platform.DB,
		"main",
		action,
		args,
		func(row *common.Row) error {
			results = append(results, row.Values)
			return nil
		},
	)

	return results, err
}

// logResults logs the first n results for debugging
func logResults(t *testing.T, action string, results [][]any, maxRows int) {
	t.Logf("Raw %s results: %d rows", action, len(results))
	for i, row := range results {
		if i < maxRows {
			t.Logf("Row %d: %v", i, row)
		}
	}
}

func TestTNOperations_DirectCalls(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "tn_ops_direct_call_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testDirectTNCalls(t),
		},
	}, testutils.GetTestOptions())
}

func testDirectTNCalls(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		logger := log.NewStdoutLogger()

		// Test the raw list_streams action
		t.Run("Raw_list_streams", func(t *testing.T) {
			args := []any{
				"0x0000000000000000000000000000000000000123", // data_provider
				5000,        // limit
				0,           // offset
				"stream_id", // order_by
				nil,         // block_height
			}

			results, err := executeDirectCall(ctx, platform, "list_streams", args)
			require.NoError(t, err)

			// Log results for debugging
			logResults(t, "list_streams", results, 5)

			// Verify result structure
			if len(results) > 0 {
				// Each row should have: [data_provider, stream_id, stream_type, created_at]
				assert.GreaterOrEqual(t, len(results[0]), 3, "Row should have at least 3 columns")
			}
		})

		// Test the raw get_category_streams action
		t.Run("Raw_get_category_streams", func(t *testing.T) {
			provider := "0x0000000000000000000000000000000000000456"
			streamID := "test_composed_stream"

			args := []any{
				provider, // data_provider
				streamID, // stream_id
				int64(0), // active_from
				nil,      // active_to
			}

			results, err := executeDirectCall(ctx, platform, "get_category_streams", args)
			require.NoError(t, err)

			// Log results for debugging
			logResults(t, "get_category_streams", results, -1) // Log all results

			// Verify result structure if we have data
			if len(results) > 0 {
				// Each row should have: [data_provider, stream_id]
				assert.GreaterOrEqual(t, len(results[0]), 2, "Row should have at least 2 columns")
			}
		})

		// Test the raw get_record_composed action
		t.Run("Raw_get_record_composed", func(t *testing.T) {
			provider := "0x0000000000000000000000000000000000000456"
			streamID := "test_composed_stream"
			fromTime := int64(1)
			toTime := int64(1000000)

			args := []any{
				provider,  // data_provider
				streamID,  // stream_id
				&fromTime, // from timestamp
				&toTime,   // to timestamp
				nil,       // frozen_at
			}

			results, err := executeDirectCallWithEngineCtx(ctx, platform, "get_record_composed", args)
			require.NoError(t, err)

			// Log results for debugging
			t.Logf("Raw get_record_composed results: %d rows", len(results))
			for i, row := range results {
				if len(row) >= 2 {
					t.Logf("Row %d: event_time=%v (type: %T), value=%v (type: %T)",
						i, row[0], row[0], row[1], row[1])
				}
			}

			// Verify result structure and test type parsing
			if len(results) > 0 {
				// Each row should have: [event_time, value]
				assert.GreaterOrEqual(t, len(results[0]), 2, "Row should have at least 2 columns")

				// Test type parsing
				eventTime, err := parseEventTime(results[0][0])
				assert.NoError(t, err, "Should be able to parse event_time")
				t.Logf("Parsed event_time: %d", eventTime)

				value, err := parseEventValue(results[0][1])
				assert.NoError(t, err, "Should be able to parse value")
				t.Logf("Parsed value: %s", value.String())
			}
		})

		// Test TNOperations wrapper
		t.Run("TNOperations_Wrapper", func(t *testing.T) {
			tnOps := NewTNOperations(platform.Engine, platform.DB, "main", logger)
			provider := "0x0000000000000000000000000000000000000123"

			// Test ListComposedStreams
			streams, err := tnOps.ListComposedStreams(ctx, provider)
			require.NoError(t, err)
			t.Logf("ListComposedStreams returned %d streams", len(streams))

			// If we have composed streams, test GetCategoryStreams and GetRecordComposed
			if len(streams) > 0 {
				firstStream := streams[0]
				t.Logf("Testing with stream: %s", firstStream)

				// Test GetCategoryStreams
				children, err := tnOps.GetCategoryStreams(ctx, provider, firstStream, 0)
				require.NoError(t, err)
				t.Logf("GetCategoryStreams returned %d children", len(children))

				// Test GetRecordComposed
				fromTime := int64(1)
				toTime := int64(1000000)
				records, err := tnOps.GetRecordComposed(ctx, provider, firstStream, &fromTime, &toTime)
				require.NoError(t, err)
				t.Logf("GetRecordComposed returned %d records", len(records))

				// Verify record structure
				for i, record := range records {
					if i < 3 { // Log first 3 records
						t.Logf("Record %d: time=%d, value=%s", i, record.EventTime, record.Value.String())
					}
				}
			}
		})

		return nil
	}
}

// TestTNOperations_TypeHandling specifically tests various type handling scenarios
func TestTNOperations_TypeHandling(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "tn_ops_types_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			testTypeHandling(t),
		},
	}, testutils.GetTestOptions())
}

func testTypeHandling(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Define type conversion test cases
		typeTests := []struct {
			category string
			cases    []struct {
				name     string
				input    interface{}
				testFunc func(interface{}) error
			}
		}{
			{
				category: "event_time conversions",
				cases: []struct {
					name     string
					input    interface{}
					testFunc func(interface{}) error
				}{
					{
						name:  "int64",
						input: int64(12345),
						testFunc: func(v interface{}) error {
							_, err := parseEventTime(v)
							return err
						},
					},
					{
						name:  "string timestamp",
						input: "12345",
						testFunc: func(v interface{}) error {
							_, err := parseEventTime(v)
							return err
						},
					},
					{
						name:  "uint64",
						input: uint64(99999),
						testFunc: func(v interface{}) error {
							_, err := parseEventTime(v)
							return err
						},
					},
				},
			},
			{
				category: "value conversions",
				cases: []struct {
					name     string
					input    interface{}
					testFunc func(interface{}) error
				}{
					{
						name:  "string decimal",
						input: "123.456",
						testFunc: func(v interface{}) error {
							_, err := parseEventValue(v)
							return err
						},
					},
					{
						name:  "float64",
						input: float64(123.456),
						testFunc: func(v interface{}) error {
							_, err := parseEventValue(v)
							return err
						},
					},
					{
						name:  "integer",
						input: 12345,
						testFunc: func(v interface{}) error {
							_, err := parseEventValue(v)
							return err
						},
					},
				},
			},
		}

		// Run all type tests
		for _, tt := range typeTests {
			t.Run(tt.category, func(t *testing.T) {
				for _, tc := range tt.cases {
					t.Run(tc.name, func(t *testing.T) {
						err := tc.testFunc(tc.input)
						assert.NoError(t, err, fmt.Sprintf("Failed to handle %s conversion", tc.name))
					})
				}
			})
		}

		return nil
	}
}
