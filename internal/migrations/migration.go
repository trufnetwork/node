package migrations

import (
	"embed"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

//go:embed *.sql test_only/*.sql erc20-bridge/*.sql
var seedFiles embed.FS

func GetSeedScriptPaths() []string {
	var seedFilesPaths []string

	// Get the absolute path to the directory where this file (migration.go) is located
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)

	err := fs.WalkDir(seedFiles, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}
		if strings.HasSuffix(path, ".prod.sql") {
			return nil
		}
		seedFilesPaths = append(seedFilesPaths, filepath.Join(dir, filepath.FromSlash(path)))
		return nil
	})
	if err != nil {
		panic(err)
	}

	if len(seedFilesPaths) == 0 {
		panic("no seeds files found in embedded directory")
	}

	sort.Strings(seedFilesPaths)
	return seedFilesPaths
}
