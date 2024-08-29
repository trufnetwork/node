package benchexport

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSaveAsMarkdown(t *testing.T) {
	testData := []SavedResults{
		{Procedure: "Test1", Depth: 1, Days: 7, DurationMs: 100, Visibility: "Public", Samples: 10},
		{Procedure: "Test1", Depth: 2, Days: 7, DurationMs: 200, Visibility: "Public", Samples: 10},
		{Procedure: "Test2", Depth: 1, Days: 14, DurationMs: 150, Visibility: "Private", Samples: 10},
		{Procedure: "Test2", Depth: 2, Days: 14, DurationMs: 250, Visibility: "Private", Samples: 10},
	}

	tempFile, err := os.CreateTemp("", "test_markdown_*.md")
	assert.NoError(t, err)
	defer os.Remove(tempFile.Name())

	currentDate := time.Date(2023, 4, 15, 12, 0, 0, 0, time.UTC)
	input := SaveAsMarkdownInput{
		Results:      testData,
		CurrentDate:  currentDate,
		InstanceType: "TestInstance",
		FilePath:     tempFile.Name(),
	}

	err = SaveAsMarkdown(input)
	assert.NoError(t, err)

	content, err := os.ReadFile(tempFile.Name())
	assert.NoError(t, err)

	expectedContent := `Date: 2023-04-15 12:00:00

## Dates x Depth

Samples per query: 10
Results in milliseconds

### TestInstance

TestInstance - Test1 - Public 

| queried days / depth | 1   | 2   |
| -------------------- | --- | --- |
| 7                    | 100 | 200 |



TestInstance - Test2 - Private 

| queried days / depth | 1   | 2   |
| -------------------- | --- | --- |
| 14                   | 150 | 250 |



`

	assert.Equal(t, expectedContent, string(content))
}
