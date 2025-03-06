package testutils

import (
	"os"
	"path/filepath"

	"github.com/kwilteam/kwil-db/testing"
)

func Ptr[T any](v T) *T {
	return &v
}

// GetTestOptions returns the common test options
func GetTestOptions() *testing.Options {
	return &testing.Options{
		UseTestContainer: true,
	}
}

func GetSeedScriptPaths() []string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	seedsDir := filepath.Join(dir, "../../internal/migrations")

	seedsFiles, err := filepath.Glob(filepath.Join(seedsDir, "*.sql"))
	if err != nil {
		panic(err)
	}

	if len(seedsFiles) == 0 {
		panic("no seeds files found at " + seedsDir)
	}

	return seedsFiles
}
