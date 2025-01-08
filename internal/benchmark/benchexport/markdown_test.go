package benchexport

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSaveAsMarkdown(t *testing.T) {
	testData := []SavedResults{
		{Procedure: "Test1", BranchingFactor: 1, QtyStreams: 1, DataPoints: 100, DurationMs: 100, Visibility: "Public", Samples: 10, UnixOnly: false},
		{Procedure: "Test1", BranchingFactor: 1, QtyStreams: 2, DataPoints: 100, DurationMs: 100, Visibility: "Public", Samples: 10, UnixOnly: false},
		{Procedure: "Test1", BranchingFactor: 1, QtyStreams: 3, DataPoints: 100, DurationMs: 100, Visibility: "Public", Samples: 10, UnixOnly: false},
		{Procedure: "Test2", BranchingFactor: 1, QtyStreams: 100, DataPoints: 150, DurationMs: 150, Visibility: "Private", Samples: 10, UnixOnly: false},
		{Procedure: "Test1", BranchingFactor: 2, QtyStreams: 10, DataPoints: 200, DurationMs: 200, Visibility: "Public", Samples: 10, UnixOnly: false},
		{Procedure: "Test1", BranchingFactor: 2, QtyStreams: 10, DataPoints: 300, DurationMs: 300, Visibility: "Public", Samples: 10, UnixOnly: false},
		{Procedure: "Test2", BranchingFactor: 2, QtyStreams: 10, DataPoints: 250, DurationMs: 250, Visibility: "Private", Samples: 10, UnixOnly: false},
		{Procedure: "Test2", BranchingFactor: 2, QtyStreams: 100, DataPoints: 350, DurationMs: 350, Visibility: "Private", Samples: 10, UnixOnly: false},
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

## Data points / Qty streams

Samples per query: 10
Results in milliseconds

### TestInstance

#### Branching Factor: 1

TestInstance - Test1 - Public

**UnixOnly = false**

| Data points / Qty streams | 1   | 2   | 3   |
| ------------------------- | --- | --- | --- |
| 100                       | 100 | 100 | 100 |




TestInstance - Test2 - Private

**UnixOnly = false**

| Data points / Qty streams | 100 |
| ------------------------- | --- |
| 150                       | 150 |




#### Branching Factor: 2

TestInstance - Test1 - Public

**UnixOnly = false**

| Data points / Qty streams | 10  |
| ------------------------- | --- |
| 200                       | 200 |
| 300                       | 300 |




TestInstance - Test2 - Private

**UnixOnly = false**

| Data points / Qty streams | 10  | 100 |
| ------------------------- | --- | --- |
| 250                       | 250 | -   |
| 350                       | -   | 350 |




`

	assert.Equal(t, expectedContent, string(content))
}
