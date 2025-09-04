package table

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
)

type AssertResultRowsEqualMarkdownTableInput struct {
	Actual             []procedure.ResultRow
	Expected           string
	ColumnTransformers map[string]func(string) string
	SortColumns        []string
	ExcludedColumns    []int // drop only from actual
}

func AssertResultRowsEqualMarkdownTable(t *testing.T, input AssertResultRowsEqualMarkdownTableInput) {
	expectedTable, err := TableFromMarkdown(input.Expected)
	if err != nil {
		t.Fatalf("error parsing expected markdown table: %v", err)
	}

	// Transform expected rows (apply transformers, keep all columns)
	expected := [][]string{}
	for _, row := range expectedTable.Rows {
		if row[1] != "" {
			transformedRow := make([]string, len(row))
			for colIdx, value := range row {
				colName := expectedTable.Headers[colIdx]
				if transformer, exists := input.ColumnTransformers[colName]; exists && transformer != nil {
					transformedRow[colIdx] = transformer(value)
				} else {
					transformedRow[colIdx] = value
				}
			}
			expected = append(expected, transformedRow)
		}
	}

	// Build a set of excluded column positions for actual
	excludedIdx := make(map[int]struct{})
	for _, idx := range input.ExcludedColumns {
		excludedIdx[idx] = struct{}{}
	}

	// Transform actual rows (drop excluded columns)
	actualInStrings := [][]string{}
	for _, row := range input.Actual {
		actualRow := []string{}
		for colIdx, value := range row {
			if _, skip := excludedIdx[colIdx]; skip {
				continue
			}
			actualRow = append(actualRow, value)
		}
		actualInStrings = append(actualInStrings, actualRow)
	}

	// Sort both expected and actual data if sort columns are specified
	if len(input.SortColumns) > 0 {
		// Build mapping from header name -> index in expected
		expectedHeaderMap := make(map[string]int)
		for i, h := range expectedTable.Headers {
			expectedHeaderMap[h] = i
		}

		// Build mapping from header name -> index in actual (after dropping columns)
		actualHeaderMap := make(map[string]int)
		actualColIdx := 0
		for i := range expectedTable.Headers {
			if _, skip := excludedIdx[i]; skip {
				continue
			}
			actualHeaderMap[expectedTable.Headers[i]] = actualColIdx
			actualColIdx++
		}

		// Sort function
		sortFunc := func(rows [][]string, headerMap map[string]int) func(i, j int) bool {
			return func(i, j int) bool {
				for _, colName := range input.SortColumns {
					if idx, ok := headerMap[colName]; ok && idx < len(rows[i]) && idx < len(rows[j]) {
						if rows[i][idx] != rows[j][idx] {
							return rows[i][idx] < rows[j][idx] // ascending
						}
					}
				}
				return false
			}
		}

		sort.SliceStable(expected, sortFunc(expected, expectedHeaderMap))
		sort.SliceStable(actualInStrings, sortFunc(actualInStrings, actualHeaderMap))
	}

	assert.Equal(t, expected, actualInStrings, "Result rows do not match expected markdown table")
}
