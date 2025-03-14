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
}

func AssertResultRowsEqualMarkdownTable(t *testing.T, input AssertResultRowsEqualMarkdownTableInput) {
	expectedTable, err := TableFromMarkdown(input.Expected)
	if err != nil {
		t.Fatalf("error parsing expected markdown table: %v", err)
	}

	// clear empty rows, because we won't get those from answer, but
	// tests might include it just to be explicit about what is being tested
	expected := [][]string{}
	for _, row := range expectedTable.Rows {
		if row[1] != "" {
			transformedRow := make([]string, len(row))
			for colIdx, value := range row {
				// Get the column name from headers
				colName := expectedTable.Headers[colIdx]
				// Apply transformer if one exists for this column
				if transformer, exists := input.ColumnTransformers[colName]; exists && transformer != nil {
					transformedRow[colIdx] = transformer(value)
				} else {
					transformedRow[colIdx] = value
				}
			}
			expected = append(expected, transformedRow)
		}
	}

	actualInStrings := [][]string{}
	for _, row := range input.Actual {
		actualRow := []string{}
		for _, column := range row {
			actualRow = append(actualRow, column)
		}
		actualInStrings = append(actualInStrings, actualRow)
	}

	// Sort both expected and actual data if sort columns are specified
	if len(input.SortColumns) > 0 {
		// Create maps of column name to index for sorting
		headerIndexMap := make(map[string]int)
		for i, header := range expectedTable.Headers {
			headerIndexMap[header] = i
		}

		// Create a custom sort function that can sort by multiple columns
		sortFunc := func(rows [][]string) func(i, j int) bool {
			return func(i, j int) bool {
				for _, colName := range input.SortColumns {
					if idx, ok := headerIndexMap[colName]; ok && idx < len(rows[i]) && idx < len(rows[j]) {
						if rows[i][idx] != rows[j][idx] {
							return rows[i][idx] < rows[j][idx]
						}
					}
				}
				return false // If all sort columns are equal, maintain original order
			}
		}

		// Sort both expected and actual results
		sort.SliceStable(expected, sortFunc(expected))
		sort.SliceStable(actualInStrings, sortFunc(actualInStrings))
	}

	assert.Equal(t, expected, actualInStrings, "Result rows do not match expected markdown table")
}
