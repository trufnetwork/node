package compose_streams

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCalculateWeightedResultsWithFn(t *testing.T) {
	tests := []struct {
		name          string
		weightMap     map[string]float64
		fn            func(string) ([]int64, error)
		expected      []int64
		expectedError error
	}{
		{
			name: "empty results",
			weightMap: map[string]float64{
				"abc": 0.5,
				"def": 0.5,
			},
			fn: func(s string) ([]int64, error) {
				return []int64{}, nil
			},
			expected:      []int64{},
			expectedError: nil,
		},
		{
			name: "single item",
			weightMap: map[string]float64{
				"abc": 1,
			},
			fn: func(s string) ([]int64, error) {
				return []int64{3}, nil
			},
			expected:      []int64{3},
			expectedError: nil,
		},
		{
			name: "multiple items with same weight",
			weightMap: map[string]float64{
				"abc": 0.5,
				"def": 0.5,
			},
			fn: func(s string) ([]int64, error) {
				return []int64{10, 20}, nil
			},
			expected:      []int64{10, 20},
			expectedError: nil,
		},
		{
			name: "multiple items with different weights",
			weightMap: map[string]float64{
				"abc": 0.1,
				"def": 0.9,
			},
			fn: func(s string) ([]int64, error) {
				if s == "abc" {
					return []int64{10, 20}, nil
				} else {
					return []int64{0, 0}, nil
				}
			},
			expected:      []int64{1, 2},
			expectedError: nil,
		},
		{
			name: "results from different databases do not match",
			weightMap: map[string]float64{
				"abc": 0.1,
				"def": 0.9,
			},
			fn: func(s string) ([]int64, error) {
				if s == "abc" {
					return []int64{10}, nil
				} else {
					return []int64{40, 80}, nil
				}
			},
			expected:      nil,
			expectedError: fmt.Errorf("different number of results from databases"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Stream{
				weightMap: test.weightMap,
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
