package composed_stream

import (
	"errors"
	"reflect"
	"testing"

	"github.com/kwilteam/kwil-db/common"
	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/extensions/precompiles"
	"github.com/stretchr/testify/mock"
	"github.com/truflation/tsn-db/internal/utils"
	"github.com/truflation/tsn-db/mocks"

	"github.com/stretchr/testify/assert"
)

func TestCalculateWeightedResultsWithFn(t *testing.T) {
	tests := []struct {
		name          string
		weightMap     map[string]int64
		fn            func(string) ([]utils.ValueWithDate, error)
		expected      []utils.ValueWithDate
		expectedError error
	}{
		{
			name: "empty results",
			weightMap: map[string]int64{
				"abc": 1,
				"def": 1,
			},
			fn: func(s string) ([]utils.ValueWithDate, error) {
				return []utils.ValueWithDate{}, nil
			},
			expected:      []utils.ValueWithDate{},
			expectedError: nil,
		},
		{
			name: "single item",
			weightMap: map[string]int64{
				"abc": 1,
			},
			fn: func(s string) ([]utils.ValueWithDate, error) {
				return []utils.ValueWithDate{{Date: "2024-01-01", Value: 3}}, nil
			},
			expected:      []utils.ValueWithDate{{Date: "2024-01-01", Value: 3}},
			expectedError: nil,
		},
		{
			name: "multiple items with same weight",
			weightMap: map[string]int64{
				"abc": 5,
				"def": 5,
			},
			fn: func(s string) ([]utils.ValueWithDate, error) {
				return []utils.ValueWithDate{{Date: "2024-01-01", Value: 10}, {Date: "2024-01-02", Value: 20}}, nil
			},
			expected:      []utils.ValueWithDate{{Date: "2024-01-01", Value: 10}, {Date: "2024-01-02", Value: 20}},
			expectedError: nil,
		},
		{
			name: "multiple items with different weights",
			weightMap: map[string]int64{
				"abc": 1,
				"def": 9,
			},
			fn: func(s string) ([]utils.ValueWithDate, error) {
				if s == "abc" {
					return []utils.ValueWithDate{{Date: "2024-01-01", Value: 10}, {Date: "2024-01-02", Value: 20}}, nil
				} else {
					return []utils.ValueWithDate{{Date: "2024-01-01", Value: 0}, {Date: "2024-01-02", Value: 0}}, nil
				}
			},
			expected:      []utils.ValueWithDate{{Date: "2024-01-01", Value: 1}, {Date: "2024-01-02", Value: 2}},
			expectedError: nil,
		},
		{
			name: "composing different number of results from databases fill the latest forward",
			weightMap: map[string]int64{
				"abc": 1,
				"def": 9,
			},
			fn: func(s string) ([]utils.ValueWithDate, error) {
				if s == "abc" {
					return []utils.ValueWithDate{{Date: "2024-01-01", Value: 10}}, nil
				} else {
					return []utils.ValueWithDate{{Date: "2024-01-01", Value: 0}, {Date: "2024-01-02", Value: 0}}, nil
				}
			},
			expected:      []utils.ValueWithDate{{Date: "2024-01-01", Value: 1}, {Date: "2024-01-02", Value: 1}},
			expectedError: nil,
		},
		{
			name: "zero denominator",
			weightMap: map[string]int64{
				"abc": 0,
				"def": 0,
			},
			fn: func(s string) ([]utils.ValueWithDate, error) {
				return []utils.ValueWithDate{{Date: "2024-01-01", Value: 10}, {Date: "2024-01-02", Value: 20}}, nil
			},
			expected:      nil,
			expectedError: errors.New("denominator cannot be zero"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			totalWeight := int64(0)
			for _, weight := range test.weightMap {
				totalWeight += weight
			}
			s := &ComposedStreamExt{
				weightMap:   test.weightMap,
				totalWeight: totalWeight,
			}
			result, err := s.CalculateWeightedResultsWithFn(test.fn)
			if test.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, test.expectedError, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expected, result)
			}
		})
	}
}

func TestFillForwardWithLatestFromCols(t *testing.T) {
	tests := []struct {
		name               string
		originalResultsSet [][]utils.ValueWithDate
		expectedResultsSet [][]utils.ValueWithDate
	}{
		{
			name:               "empty original results set",
			originalResultsSet: [][]utils.ValueWithDate{},
			expectedResultsSet: [][]utils.ValueWithDate{},
		},
		{
			name:               "single date with single value",
			originalResultsSet: [][]utils.ValueWithDate{{{Date: "2024-01-01", Value: 1}}},
			expectedResultsSet: [][]utils.ValueWithDate{{{Date: "2024-01-01", Value: 1}}},
		},
		{
			name:               "multiple dates with single values",
			originalResultsSet: [][]utils.ValueWithDate{{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}}},
			expectedResultsSet: [][]utils.ValueWithDate{{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}}},
		},
		{
			name: "multiple dates from more sources without gaps",
			originalResultsSet: [][]utils.ValueWithDate{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 5}},
			},
			expectedResultsSet: [][]utils.ValueWithDate{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 5}},
			},
		},
		{
			name: "multiple dates from more sources with gap in the middle",
			originalResultsSet: [][]utils.ValueWithDate{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}, {Date: "2024-01-03", Value: 4}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-03", Value: 5}},
			},
			expectedResultsSet: [][]utils.ValueWithDate{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}, {Date: "2024-01-03", Value: 4}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 4}, {Date: "2024-01-03", Value: 5}},
			},
		},
		{
			name: "multiple dates from more sources with gap in the end",
			originalResultsSet: [][]utils.ValueWithDate{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}},
			},
			expectedResultsSet: [][]utils.ValueWithDate{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 4}},
			},
		},
		{
			name: "multiple dates from more sources with gap in the beginning",
			originalResultsSet: [][]utils.ValueWithDate{
				{{Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 5}},
			},
			expectedResultsSet: [][]utils.ValueWithDate{
				{{Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-02", Value: 5}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := FillForwardWithLatestFromCols(tt.originalResultsSet)
			if !reflect.DeepEqual(results, tt.expectedResultsSet) {
				t.Errorf("Expected %v, got %v", tt.expectedResultsSet, results)
			}
		})
	}
}

type composeStreamsTest struct {
	mock.Mock
	scoper         *precompiles.ProcedureContext
	app            *common.App
	composedStream *ComposedStreamExt
}

func newComposeStreamsTest() composeStreamsTest {
	return composeStreamsTest{
		Mock:   mock.Mock{},
		scoper: &precompiles.ProcedureContext{},
		app:    &common.App{},
		composedStream: &ComposedStreamExt{
			weightMap:   map[string]int64{"dbId": 1},
			totalWeight: 1,
		},
	}
}

func TestInitializeComposedStream(t *testing.T) {
	//instance := newComposeStreamsTest()
	t.Run("success - it should return ComposedStreamExt instance", func(t *testing.T) {
		metadata := map[string]string{"key_id": "dbId", "key_weight": "1"}
		_, err := InitializeComposedStream(nil, nil, metadata)
		assert.NoError(t, err, "InitializeComposedStream returned an error")
	})

	t.Run("validation - missing weightStr for composed_stream", func(t *testing.T) {
		metadata := map[string]string{"key_id": "dbId"}
		_, err := InitializeComposedStream(nil, nil, metadata)
		assert.EqualError(t, err, "missing weightStr for composed_stream dbId")
	})

	t.Run("error - it should return error when weightStr is not a number", func(t *testing.T) {
		metadata := map[string]string{"key_id": "dbId", "key_weight": "not_a_number"}
		_, err := InitializeComposedStream(nil, nil, metadata)
		assert.Error(t, err, "InitializeComposedStream did not return an error")
	})
}

func TestCallOnTargetDBID(t *testing.T) {
	instance := newComposeStreamsTest()
	mockEngine := mocks.NewEngine(t)
	instance.app.Engine = mockEngine
	expectedResultSet := &sql.ResultSet{
		Columns: []string{"date", "value"},
		Rows:    [][]interface{}{{"2023-12-31", int64(1)}},
	}

	t.Run("success - it should return nil when method is get_index", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Procedure(mock.Anything, mock.Anything, mock.Anything).Return(expectedResultSet, nil)
		_, err := CallOnTargetDBID(instance.scoper, instance.app, "get_index", "targetDBID", "2023-11-01", "2023-12-31")
		assert.NoError(t, err, "composedStream.Call returned an error")
	})

	t.Run("success - it should return nil when method is get_primitive", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Procedure(mock.Anything, mock.Anything, mock.Anything).Return(expectedResultSet, nil)
		_, err := CallOnTargetDBID(instance.scoper, instance.app, "get_primitive", "targetDBID", "2023-11-01", "2023-12-31")
		assert.NoError(t, err, "composedStream.Call returned an error")
	})

	t.Run("error - it should return error when app.Engine.Procedure returns an error", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Procedure(mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		_, err := CallOnTargetDBID(instance.scoper, instance.app, "get_primitive", "targetDBID", "2023-11-01", "2023-12-31")
		assert.Error(t, err, "composedStream.Call did not return an error")
		assert.Contains(t, err.Error(), assert.AnError.Error())
	})

	t.Run("validation - it should return composedStream returned nil error when app.Engine.Procedure returns nil", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Procedure(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		_, err := CallOnTargetDBID(instance.scoper, instance.app, "get_primitive", "targetDBID", "2023-11-01", "2023-12-31")
		assert.Error(t, err, "composedStream.Call did not return an error")
		assert.Contains(t, err.Error(), "stream returned nil")
	})

	t.Run("validation - it should return error getting scalar", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Procedure(mock.Anything, mock.Anything, mock.Anything).Return(&sql.ResultSet{}, nil)
		_, err := CallOnTargetDBID(instance.scoper, instance.app, "get_primitive", "targetDBID", "wrongDate", "2023-12-31")
		assert.Error(t, err, "composedStream.Call did not return an error")
		assert.Contains(t, err.Error(), "error getting scalar")
	})
}

func TestStream_Call(t *testing.T) {
	instance := newComposeStreamsTest()
	mockEngine := mocks.NewEngine(t)
	instance.app.Engine = mockEngine

	t.Run("success - it should return nil when method is get_index", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		expectedResultSet := &sql.ResultSet{
			Columns: []string{"date", "value"},
			Rows:    [][]interface{}{{"2023-12-30", int64(1)}, {"2023-12-31", int64(2)}},
		}
		mockEngine.EXPECT().Procedure(mock.Anything, mock.Anything, mock.Anything).Return(expectedResultSet, nil)
		_, err := instance.composedStream.Call(instance.scoper, instance.app, "get_index", []interface{}{"2023-11-01", "2023-12-31"})
		assert.NoError(t, err, "composedStream.Call returned an error")
	})

	t.Run("success - it should return nil when method is get_primitive", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		expectedResultSet := &sql.ResultSet{
			Columns: []string{"date", "value"},
			Rows:    [][]interface{}{{"2023-12-31", int64(1)}},
		}
		mockEngine.EXPECT().Procedure(mock.Anything, mock.Anything, mock.Anything).Return(expectedResultSet, nil)
		_, err := instance.composedStream.Call(instance.scoper, instance.app, "get_primitive", []interface{}{"2023-11-01", "2023-12-31"})
		assert.NoError(t, err, "composedStream.Call returned an error")
	})

	t.Run("error - it should return error when Engine.Procedure returns error", func(t *testing.T) {
		mockEngine.ExpectedCalls = nil
		mockEngine.EXPECT().Procedure(mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		_, err := instance.composedStream.Call(instance.scoper, instance.app, "get_index", []interface{}{"2023-11-01", "2023-12-31"})
		assert.Error(t, err, "composedStream.Call did not return an error")
		assert.Contains(t, err.Error(), assert.AnError.Error())
	})

	t.Run("validation - it should return unknown method error", func(t *testing.T) {
		_, err := instance.composedStream.Call(nil, nil, "unknown", nil)
		assert.Contains(t, err.Error(), "unknown method")
	})

	t.Run("validation - it should return error when inputs length is less than 2", func(t *testing.T) {
		_, err := instance.composedStream.Call(nil, nil, "get_index", []interface{}{})
		assert.Contains(t, err.Error(), "expected 2 inputs")
	})

	t.Run("validation - it should return error when inputs[0] is not string", func(t *testing.T) {
		_, err := instance.composedStream.Call(nil, nil, "get_index", []interface{}{1, "2023-12-31"})
		assert.Contains(t, err.Error(), "expected string")
	})

	t.Run("validation - it should return error when inputs[1] is not string", func(t *testing.T) {
		_, err := instance.composedStream.Call(nil, nil, "get_index", []interface{}{"2023-11-01", 1})
		assert.Contains(t, err.Error(), "expected string")
	})

	t.Run("validation - it should return error when inputs[0] is not valid date", func(t *testing.T) {
		_, err := instance.composedStream.Call(nil, nil, "get_index", []interface{}{"2023-11-01", "not_a_date"})
		assert.Contains(t, err.Error(), "invalid date")
	})

	t.Run("validation - it should return error when inputs[1] is not valid date", func(t *testing.T) {
		_, err := instance.composedStream.Call(nil, nil, "get_index", []interface{}{"not_a_date", "2023-12-31"})
		assert.Contains(t, err.Error(), "invalid date")
	})
}
