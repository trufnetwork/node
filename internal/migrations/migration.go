package migrations

import (
	"embed"
	"path/filepath"
	"runtime"
	"strings"
)

//go:embed *.sql test_only/*.sql
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

	// process test_only directory
	entries, err = seedFiles.ReadDir("test_only")
	if err != nil {
		panic(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			seedsFiles = append(seedsFiles, filepath.Join(dir, "test_only", entry.Name()))
		}
	}

	if len(seedsFiles) == 0 {
		panic("no seeds files found in embedded directory")
	}

	return seedsFiles
}
