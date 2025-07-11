package internal

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/common"
	"github.com/trufnetwork/kwil-db/core/log"
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

// createTestEngineOps creates an EngineOperations instance with a discard logger
func createTestEngineOps(engine common.Engine) *EngineOperations {
	return NewEngineOperations(engine, nil, "main", log.New(log.WithWriter(io.Discard)))
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

func TestEngineOperations_ListComposedStreams(t *testing.T) {
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
		// Removed "only primitive streams" and "no streams" - trivial edge cases
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
			engineOps := createTestEngineOps(engine)
			streams, err := engineOps.ListComposedStreams(context.Background(), tt.provider)

			// Assertions
			require.NoError(t, err)
			assert.Len(t, streams, tt.expectedCount)
			assert.ElementsMatch(t, tt.expectedIDs, streams)
		})
	}
}

func TestEngineOperations_GetCategoryStreams(t *testing.T) {
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
		// Removed "stream with no children" - trivial edge case
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
			engineOps := createTestEngineOps(engine)
			children, err := engineOps.GetCategoryStreams(context.Background(), tt.provider, tt.streamID, tt.activeFrom)

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

func TestEngineOperations_GetRecordComposed(t *testing.T) {
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
			engineOps := createTestEngineOps(engine)
			records, err := engineOps.GetRecordComposed(context.Background(), tt.provider, tt.streamID, tt.from, tt.to)

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
