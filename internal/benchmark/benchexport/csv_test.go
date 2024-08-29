package benchexport

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSaveOrAppendToCSV(t *testing.T) {
	testData := []SavedResults{
		{Procedure: "Test1", Depth: 1, Days: 7, DurationMs: 100, Visibility: "Public", Samples: 10},
		{Procedure: "Test2", Depth: 2, Days: 14, DurationMs: 200, Visibility: "Private", Samples: 10},
	}

	tempFile, err := os.CreateTemp("", "test_csv_*.csv")
	assert.NoError(t, err)
	defer os.Remove(tempFile.Name())

	err = SaveOrAppendToCSV(testData, tempFile.Name())
	assert.NoError(t, err)

	content, err := os.ReadFile(tempFile.Name())
	assert.NoError(t, err)

	expectedContent := "procedure,depth,days,duration_ms,visibility\nTest1,1,7,100,Public\nTest2,2,14,200,Private\n"
	assert.Equal(t, expectedContent, string(content))
}

func TestLoadCSV(t *testing.T) {
	csvData := "procedure,depth,days,duration_ms,visibility\nTest1,1,7,100,Public\nTest2,2,14,200,Private\n"
	reader := bytes.NewBufferString(csvData)

	results, err := LoadCSV[SavedResults](reader)
	if err != nil {
		t.Fatalf("LoadCSV returned an error: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("LoadCSV returned an empty slice")
	}

	expectedResults := []SavedResults{
		{Procedure: "Test1", Depth: 1, Days: 7, DurationMs: 100, Visibility: "Public", Samples: 10},
		{Procedure: "Test2", Depth: 2, Days: 14, DurationMs: 200, Visibility: "Private", Samples: 10},
	}

	assert.Equal(t, expectedResults, results)
}
