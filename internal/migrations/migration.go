package migrations

import (
	"embed"
	"path/filepath"
	"runtime"
)

//go:embed *.sql
var seedFiles embed.FS

func GetSeedScriptPaths() []string {
	var seedsFiles []string

	// Get the absolute path to the directory where this file (migration.go) is located
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	// Read embedded seed files
	entries, err := seedFiles.ReadDir(".")
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			// Create absolute path by joining the directory path with the file name
			seedsFiles = append(seedsFiles, filepath.Join(dir, entry.Name()))
		}
	}

	if len(seedsFiles) == 0 {
		panic("no seeds files found in embedded directory")
	}

	return seedsFiles
}
