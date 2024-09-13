package table

import (
	"github.com/stretchr/testify/assert"
	"github.com/truflation/tsn-db/internal/contracts/tests/utils/procedure"
	"testing"
)

func AssertResultRowsEqualMarkdownTable(t *testing.T, actual []procedure.ResultRow, markdownTable string) {
	expectedTable, err := TableFromMarkdown(markdownTable)
	if err != nil {
		t.Fatalf("error parsing expected markdown table: %v", err)
	}

	// clear empty rows, because we won't get those from answer, but
	// tests might include it just to be explicit about what is being tested
	expected := [][]string{}
	for _, row := range expectedTable.Rows {
		if row[1] != "" {
			expected = append(expected, row)
		}
	}

	actualInStrings := [][]string{}
	for _, row := range actual {
		actualInStrings = append(actualInStrings, row)
	}

	assert.Equal(t, expected, actualInStrings, "Result rows do not match expected markdown table")
}
