package migrations

import (
	"embed"
	"path/filepath"
	"runtime"
	"strings"
)

//go:embed *.sql test-migrations/*.sql
var seedFiles embed.FS

func GetSeedScriptPaths() []string {
	var seedsFiles []string

	// Get the absolute path to the directory where this file (migration.go) is located
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	// Read embedded seed files from root directory
	entries, err := seedFiles.ReadDir(".")
	if err != nil {
		panic(err)
	}

	// Process root directory SQL files
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			// Create absolute path by joining the directory path with the file name
			seedsFiles = append(seedsFiles, filepath.Join(dir, entry.Name()))
		}
	}

	// Read embedded seed files from test-migrations directory
	testSeedsEntries, err := seedFiles.ReadDir("test-migrations")
	if err != nil {
		panic(err)
	}

	// Process test-migrations SQL files
	for _, entry := range testSeedsEntries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			// Create absolute path by joining the directory path with the test-migrations subdirectory and file name
			seedsFiles = append(seedsFiles, filepath.Join(dir, "test-migrations", entry.Name()))
		}
	}

	if len(seedsFiles) == 0 {
		panic("no seeds files found in embedded directory")
	}

	return seedsFiles
}
