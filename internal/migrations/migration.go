package migrations

import (
	"embed"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

//go:embed *.sql 0_test_only/*.sql erc20-bridge/*.sql
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

// GetSeedScriptStatements returns migration SQL statements with test-specific replacements.
// This is used for test environments to replace production bridge namespace with test bridge.
//
// Replacement strategy:
// - Production migrations use 'ethereum_bridge' (mainnet)
// - Test environments need 'sepolia_bridge' (testnet)
// - Runtime string replacement avoids duplicate migration files
//
// This implements the approach discussed to avoid maintaining separate test-only override files
// that duplicate logic and can become a bug source if production and test logic diverges.
func GetSeedScriptStatements() []string {
	var statements []string

	// Collect all SQL file paths in sorted order
	var sqlPaths []string
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
		sqlPaths = append(sqlPaths, path)
		return nil
	})
	if err != nil {
		panic(err)
	}

	if len(sqlPaths) == 0 {
		panic("no SQL files found in embedded directory")
	}

	// Sort paths to ensure consistent loading order (0_test_only/ loads after production)
	sort.Strings(sqlPaths)

	// Read each file and apply test-specific replacements
	for _, path := range sqlPaths {
		content, err := seedFiles.ReadFile(path)
		if err != nil {
			panic("failed to read migration file " + path + ": " + err.Error())
		}

		sql := string(content)

		// Replace production bridge namespace with test bridge namespace
		// This allows tests to use the same migration logic without duplication
		sql = strings.ReplaceAll(sql, "ethereum_bridge", "sepolia_bridge")

		statements = append(statements, sql)
	}

	return statements
}
