package internal

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
	"github.com/trufnetwork/kwil-db/core/types"
	"github.com/trufnetwork/kwil-db/node/types/sql"
)

// mockEngine implements common.Engine interface for testing
// Test fixtures and helpers for TN operations tests

// testStreamData represents test data for a stream
type testStreamData struct {
	provider   string
	streamID   string
	streamType string
	createdAt  string
}

// testChildStream represents test data for child streams
type testChildStream struct {
	provider string
	streamID string
}

// testRecord represents test data for composed records
type testRecord struct {
	eventTime int64
	value     string
}

// createMockStreams generates mock stream data
func createMockStreams(streams []testStreamData) func(action string, args []any, fn func(*common.Row) error) {
	return func(action string, args []any, fn func(*common.Row) error) {
		// Filter by provider if one is specified in args
		provider := ""
		if len(args) > 0 {
			if p, ok := args[0].(string); ok {
				provider = p
			}
		}

		for _, stream := range streams {
			// Only return streams matching the requested provider
			if provider == "" || stream.provider == provider {
				fn(&common.Row{Values: []any{stream.provider, stream.streamID, stream.streamType, stream.createdAt}})
			}
		}
	}
}

// createMockChildStreams generates mock child stream data
func createMockChildStreams(children []testChildStream) func(action string, args []any, fn func(*common.Row) error) {
	return func(action string, args []any, fn func(*common.Row) error) {
		for _, child := range children {
			fn(&common.Row{Values: []any{child.provider, child.streamID}})
		}
	}
}

// createMockRecords generates mock composed record data
func createMockRecords(records []testRecord) func(action string, args []any, fn func(*common.Row) error) {
	return func(action string, args []any, fn func(*common.Row) error) {
		for _, record := range records {
			fn(&common.Row{Values: []any{record.eventTime, record.value}})
		}
	}
}

// assertEngineCall verifies the engine was called with expected parameters
func assertEngineCall(t *testing.T, expectedAction string, expectedProvider string) func(ctx *common.EngineContext, db sql.DB, namespace, action string, args []any, fn func(*common.Row) error) (*common.CallResult, error) {
	return func(ctx *common.EngineContext, db sql.DB, namespace, action string, args []any, fn func(*common.Row) error) (*common.CallResult, error) {
		assert.Equal(t, expectedAction, action)
		assert.Equal(t, expectedProvider, args[0])
		return &common.CallResult{}, nil
	}
}

// createTestTNOps creates a TNOperations instance with a discard logger
func createTestTNOps(engine common.Engine) *TNOperations {
	return NewTNOperations(engine, nil, "main", log.New(log.WithWriter(io.Discard)))
}

type mockEngine struct {
	callFunc func(ctx *common.EngineContext, db sql.DB, namespace, action string, args []any, fn func(*common.Row) error) (*common.CallResult, error)
}

func (m *mockEngine) CallWithoutEngineCtx(ctx context.Context, db sql.DB, namespace, action string, args []any, fn func(*common.Row) error) (*common.CallResult, error) {
	return &common.CallResult{}, nil
}

func (m *mockEngine) Call(ctx *common.EngineContext, db sql.DB, namespace, action string, args []any, fn func(*common.Row) error) (*common.CallResult, error) {
	if m.callFunc != nil {
		return m.callFunc(ctx, db, namespace, action, args, fn)
	}
	return &common.CallResult{}, nil
}

func (m *mockEngine) Execute(ctx *common.EngineContext, db sql.DB, stmt string, values map[string]any, fn func(*common.Row) error) error {
	return nil
}

func (m *mockEngine) ExecuteWithoutEngineCtx(ctx context.Context, db sql.DB, stmt string, values map[string]any, fn func(*common.Row) error) error {
	return nil
}

func TestTNOperations_ListComposedStreams(t *testing.T) {
	tests := []struct {
		name          string
		provider      string
		streams       []testStreamData
		expectedCount int
		expectedIDs   []string
	}{
		{
			name:     "multiple composed streams",
			provider: "0x1234567890abcdef",
			streams: []testStreamData{
				{"0x1234567890abcdef", "stream1", "composed", "2024-01-01"},
				{"0x1234567890abcdef", "stream2", "composed", "2024-01-01"},
				{"0x1234567890abcdef", "stream3", "primitive", "2024-01-01"},
				{"0x1234567890abcdef", "stream4", "composed", "2024-01-01"},
			},
			expectedCount: 3,
			expectedIDs:   []string{"stream1", "stream2", "stream4"},
		},
		{
			name:     "only primitive streams",
			provider: "0xabcdef1234567890",
			streams: []testStreamData{
				{"0xabcdef1234567890", "stream1", "primitive", "2024-01-01"},
				{"0xabcdef1234567890", "stream2", "primitive", "2024-01-01"},
			},
			expectedCount: 0,
			expectedIDs:   []string{},
		},
		{
			name:          "no streams",
			provider:      "0x0000000000000000",
			streams:       []testStreamData{},
			expectedCount: 0,
			expectedIDs:   []string{},
		},
		{
			name:     "mixed providers filtered",
			provider: "0x1111111111111111",
			streams: []testStreamData{
				{"0x1111111111111111", "stream1", "composed", "2024-01-01"},
				{"0x2222222222222222", "stream2", "composed", "2024-01-01"},
				{"0x1111111111111111", "stream3", "composed", "2024-01-01"},
			},
			expectedCount: 2,
			expectedIDs:   []string{"stream1", "stream3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock engine
			engine := &mockEngine{
				callFunc: func(ctx *common.EngineContext, db sql.DB, namespace, action string, args []any, fn func(*common.Row) error) (*common.CallResult, error) {
					assert.Equal(t, "list_streams", action)
					assert.Equal(t, tt.provider, args[0])
					assert.Equal(t, 5000, args[1]) // limit
					assert.Equal(t, 0, args[2])    // offset
					assert.Equal(t, "stream_id", args[3])
					assert.Equal(t, int64(0), args[4]) // block_height

					createMockStreams(tt.streams)(action, args, fn)
					return &common.CallResult{}, nil
				},
			}

			// Execute test
			tnOps := createTestTNOps(engine)
			streams, err := tnOps.ListComposedStreams(context.Background(), tt.provider)

			// Assertions
			require.NoError(t, err)
			assert.Len(t, streams, tt.expectedCount)
			assert.ElementsMatch(t, tt.expectedIDs, streams)
		})
	}
}

func TestTNOperations_GetCategoryStreams(t *testing.T) {
	tests := []struct {
		name          string
		provider      string
		streamID      string
		activeFrom    int64
		children      []testChildStream
		expectedCount int
	}{
		{
			name:       "stream with multiple children",
			provider:   "0x1234567890abcdef",
			streamID:   "composed_stream",
			activeFrom: 1000,
			children: []testChildStream{
				{"0x1234567890abcdef", "child1"},
				{"0x1234567890abcdef", "child2"},
				{"0xabcdef1234567890", "child3"},
			},
			expectedCount: 3,
		},
		{
			name:          "stream with no children",
			provider:      "0x1234567890abcdef",
			streamID:      "leaf_stream",
			activeFrom:    1000,
			children:      []testChildStream{},
			expectedCount: 0,
		},
		{
			name:       "stream with single child",
			provider:   "0xaaaa000000000000",
			streamID:   "parent_stream",
			activeFrom: 0,
			children: []testChildStream{
				{"0xaaaa000000000000", "only_child"},
			},
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock engine
			engine := &mockEngine{
				callFunc: func(ctx *common.EngineContext, db sql.DB, namespace, action string, args []any, fn func(*common.Row) error) (*common.CallResult, error) {
					assert.Equal(t, "get_category_streams", action)
					assert.Equal(t, tt.provider, args[0])
					assert.Equal(t, tt.streamID, args[1])
					assert.Equal(t, tt.activeFrom, args[2])
					assert.Nil(t, args[3]) // active_to

					createMockChildStreams(tt.children)(action, args, fn)
					return &common.CallResult{}, nil
				},
			}

			// Execute test
			tnOps := createTestTNOps(engine)
			children, err := tnOps.GetCategoryStreams(context.Background(), tt.provider, tt.streamID, tt.activeFrom)

			// Assertions
			require.NoError(t, err)
			assert.Len(t, children, tt.expectedCount)

			// Verify structure of returned data
			for i, child := range children {
				if i < len(tt.children) {
					assert.Equal(t, tt.children[i].provider, child.DataProvider)
					assert.Equal(t, tt.children[i].streamID, child.StreamID)
				}
			}
		})
	}
}

func TestTNOperations_GetRecordComposed(t *testing.T) {
	// Common test time values
	fromTime := int64(1000)
	toTime := int64(2000)

	tests := []struct {
		name          string
		provider      string
		streamID      string
		from          *int64
		to            *int64
		records       []testRecord
		expectedCount int
		expectError   bool
	}{
		{
			name:     "records within time range",
			provider: "0x1234567890abcdef",
			streamID: "test_stream",
			from:     &fromTime,
			to:       &toTime,
			records: []testRecord{
				{1100, "100.5"},
				{1200, "200.75"},
				{1300, "300.25"},
			},
			expectedCount: 3,
		},
		{
			name:          "no records in range",
			provider:      "0x1234567890abcdef",
			streamID:      "empty_stream",
			from:          &fromTime,
			to:            &toTime,
			records:       []testRecord{},
			expectedCount: 0,
		},
		{
			name:     "records with nil time bounds",
			provider: "0xabcd000000000000",
			streamID: "unbounded_stream",
			from:     nil,
			to:       nil,
			records: []testRecord{
				{500, "50.0"},
				{1500, "150.0"},
				{2500, "250.0"},
			},
			expectedCount: 3,
		},
		{
			name:     "single record",
			provider: "0x1111222233334444",
			streamID: "single_record_stream",
			from:     &fromTime,
			to:       &toTime,
			records: []testRecord{
				{1500, "999.999"},
			},
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock engine
			engine := &mockEngine{
				callFunc: func(ctx *common.EngineContext, db sql.DB, namespace, action string, args []any, fn func(*common.Row) error) (*common.CallResult, error) {
					assert.Equal(t, "get_record_composed", action)
					assert.Equal(t, tt.provider, args[0])
					assert.Equal(t, tt.streamID, args[1])
					assert.Equal(t, tt.from, args[2])
					assert.Equal(t, tt.to, args[3])
					assert.Nil(t, args[4]) // frozen_at

					createMockRecords(tt.records)(action, args, fn)
					return &common.CallResult{}, nil
				},
			}

			// Execute test
			tnOps := createTestTNOps(engine)
			records, err := tnOps.GetRecordComposed(context.Background(), tt.provider, tt.streamID, tt.from, tt.to)

			// Assertions
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, records, tt.expectedCount)

				// Verify the structure and values of returned data
				for i, record := range records {
					if i < len(tt.records) {
						assert.Equal(t, tt.records[i].eventTime, record.EventTime)
						assert.NotNil(t, record.Value)
						// Verify decimal precision
						assert.Equal(t, uint16(36), record.Value.Precision())
						assert.Equal(t, uint16(18), record.Value.Scale())
					}
				}
			}
		})
	}
}

func TestParseEventTime(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
		wantErr  bool
	}{
		// Valid conversions
		{"int64", int64(12345), 12345, false},
		{"int", int(12345), 12345, false},
		{"int32", int32(12345), 12345, false},
		{"uint64", uint64(12345), 12345, false},
		{"uint32", uint32(12345), 12345, false},
		{"string valid", "12345", 12345, false},

		// Error cases
		{"string invalid", "not a number", 0, true},
		{"string empty", "", 0, true},
		{"unsupported type float", 12.34, 0, true},
		{"unsupported type bool", true, 0, true},
		{"nil value", nil, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseEventTime(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseEventValue(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		wantErr     bool
		expectedStr string // Expected string representation of decimal
	}{
		// Valid conversions
		{"string decimal", "123.456", false, "123.456"},
		{"string integer", "123", false, "123"},
		{"float64", float64(123.456), false, "123.456000"},
		{"int64", int64(123), false, "123"},
		{"int", int(123), false, "123"},

		// Edge cases
		{"zero string", "0", false, "0"},
		{"negative string", "-123.456", false, "-123.456"},
		{"large number", "999999999999999999.999999999999999999", false, "999999999999999999.999999999999999999"},

		// Error cases
		{"nil", nil, true, ""},
		{"empty string", "", true, ""},
		{"invalid string", "not-a-number", true, ""},
		{"unsupported type array", []int{1, 2, 3}, true, ""},
		{"unsupported type map", map[string]int{"a": 1}, true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseEventValue(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)

				// Verify decimal properties
				assert.Equal(t, uint16(36), result.Precision())
				assert.Equal(t, uint16(18), result.Scale())

				// Compare string representation
				// The decimal is stored with 18 scale, so we need to handle trailing zeros
				actualStr := result.String()

				// For test comparison, normalize expected values to have the same format
				expectedDec, err := types.ParseDecimalExplicit(tt.expectedStr, 36, 18)
				require.NoError(t, err)
				assert.Equal(t, expectedDec.String(), actualStr)
			}
		})
	}
}

func TestParseEventValue_Decimal(t *testing.T) {
	// Test parsing of actual decimal types
	testCases := []struct {
		name      string
		setupFunc func() (*types.Decimal, error)
		wantErr   bool
	}{
		{
			name: "valid decimal with correct precision",
			setupFunc: func() (*types.Decimal, error) {
				return types.ParseDecimalExplicit("123.456", 36, 18)
			},
			wantErr: false,
		},
		{
			name: "decimal at max precision",
			setupFunc: func() (*types.Decimal, error) {
				return types.ParseDecimalExplicit("999999999999999999.999999999999999999", 36, 18)
			},
			wantErr: false,
		},
		{
			name: "decimal with different precision",
			setupFunc: func() (*types.Decimal, error) {
				// Create decimal with different precision, should be converted to (36,18)
				return types.ParseDecimalExplicit("123.45", 10, 2)
			},
			wantErr: false,
		},
		{
			name: "nil decimal",
			setupFunc: func() (*types.Decimal, error) {
				return nil, nil
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dec, err := tc.setupFunc()
			require.NoError(t, err, "Setup should not fail")

			result, err := parseEventValue(dec)

			if tc.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)

				// Verify it was converted to decimal(36,18)
				assert.Equal(t, uint16(36), result.Precision())
				assert.Equal(t, uint16(18), result.Scale())

				// If input was non-nil, verify value preservation
				if dec != nil {
					assert.Equal(t, dec.String(), result.String())
				}
			}
		})
	}
}
