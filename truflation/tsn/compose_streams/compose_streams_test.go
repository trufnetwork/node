package compose_streams

import (
	"github.com/kwilteam/kwil-db/truflation/tsn/utils"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestCalculateWeightedResultsWithFn(t *testing.T) {
	tests := []struct {
		name          string
		weightMap     map[string]int64
		fn            func(string) ([]utils.WithDate[int64], error)
		expected      []utils.WithDate[int64]
		expectedError error
	}{
		{
			name: "empty results",
			weightMap: map[string]int64{
				"abc": 1,
				"def": 1,
			},
			fn: func(s string) ([]utils.WithDate[int64], error) {
				return []utils.WithDate[int64]{}, nil
			},
			expected:      []utils.WithDate[int64]{},
			expectedError: nil,
		},
		{
			name: "single item",
			weightMap: map[string]int64{
				"abc": 1,
			},
			fn: func(s string) ([]utils.WithDate[int64], error) {
				return []utils.WithDate[int64]{{Date: "2024-01-01", Value: 3}}, nil
			},
			expected:      []utils.WithDate[int64]{{Date: "2024-01-01", Value: 3}},
			expectedError: nil,
		},
		{
			name: "multiple items with same weight",
			weightMap: map[string]int64{
				"abc": 5,
				"def": 5,
			},
			fn: func(s string) ([]utils.WithDate[int64], error) {
				return []utils.WithDate[int64]{{Date: "2024-01-01", Value: 10}, {Date: "2024-01-02", Value: 20}}, nil
			},
			expected:      []utils.WithDate[int64]{{Date: "2024-01-01", Value: 10}, {Date: "2024-01-02", Value: 20}},
			expectedError: nil,
		},
		{
			name: "multiple items with different weights",
			weightMap: map[string]int64{
				"abc": 1,
				"def": 9,
			},
			fn: func(s string) ([]utils.WithDate[int64], error) {
				if s == "abc" {
					return []utils.WithDate[int64]{{Date: "2024-01-01", Value: 10}, {Date: "2024-01-02", Value: 20}}, nil
				} else {
					return []utils.WithDate[int64]{{Date: "2024-01-01", Value: 0}, {Date: "2024-01-02", Value: 0}}, nil
				}
			},
			expected:      []utils.WithDate[int64]{{Date: "2024-01-01", Value: 1}, {Date: "2024-01-02", Value: 2}},
			expectedError: nil,
		},
		{
			name: "composing different number of results from databases fill the latest forward",
			weightMap: map[string]int64{
				"abc": 1,
				"def": 9,
			},
			fn: func(s string) ([]utils.WithDate[int64], error) {
				if s == "abc" {
					return []utils.WithDate[int64]{{Date: "2024-01-01", Value: 10}}, nil
				} else {
					return []utils.WithDate[int64]{{Date: "2024-01-01", Value: 0}, {Date: "2024-01-02", Value: 0}}, nil
				}
			},
			expected:      []utils.WithDate[int64]{{Date: "2024-01-01", Value: 1}, {Date: "2024-01-02", Value: 1}},
			expectedError: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			totalWeight := int64(0)
			for _, weight := range test.weightMap {
				totalWeight += weight
			}
			s := &Stream{
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
		originalResultsSet [][]utils.WithDate[int]
		expectedResultsSet [][]utils.WithDate[int]
	}{
		{
			name:               "empty original results set",
			originalResultsSet: [][]utils.WithDate[int]{},
			expectedResultsSet: [][]utils.WithDate[int]{},
		},
		{
			name:               "single date with single value",
			originalResultsSet: [][]utils.WithDate[int]{{{Date: "2024-01-01", Value: 1}}},
			expectedResultsSet: [][]utils.WithDate[int]{{{Date: "2024-01-01", Value: 1}}},
		},
		{
			name:               "multiple dates with single values",
			originalResultsSet: [][]utils.WithDate[int]{{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}}},
			expectedResultsSet: [][]utils.WithDate[int]{{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}}},
		},
		{
			name: "multiple dates from more sources without gaps",
			originalResultsSet: [][]utils.WithDate[int]{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 5}},
			},
			expectedResultsSet: [][]utils.WithDate[int]{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 5}},
			},
		},
		{
			name: "multiple dates from more sources with gap in the middle",
			originalResultsSet: [][]utils.WithDate[int]{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}, {Date: "2024-01-03", Value: 4}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-03", Value: 5}},
			},
			expectedResultsSet: [][]utils.WithDate[int]{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}, {Date: "2024-01-03", Value: 4}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 4}, {Date: "2024-01-03", Value: 5}},
			},
		},
		{
			name: "multiple dates from more sources with gap in the end",
			originalResultsSet: [][]utils.WithDate[int]{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}},
			},
			expectedResultsSet: [][]utils.WithDate[int]{
				{{Date: "2024-01-01", Value: 2}, {Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 4}},
			},
		},
		{
			name: "multiple dates from more sources with gap in the beginning",
			originalResultsSet: [][]utils.WithDate[int]{
				{{Date: "2024-01-02", Value: 3}},
				{{Date: "2024-01-01", Value: 4}, {Date: "2024-01-02", Value: 5}},
			},
			expectedResultsSet: [][]utils.WithDate[int]{
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
