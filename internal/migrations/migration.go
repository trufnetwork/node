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

// GetSeedScriptStatements returns the dev/test migration SQL statements as-is.
//
// Dev source-of-truth lives in the literal `*.sql` files; mainnet overrides
// live in matching `*.prod.sql` files. Each environment's bridge namespace
// (dev: hoodi_tt / hoodi_tt2 / sepolia_bridge; mainnet: eth_truf / eth_usdc)
// appears verbatim in its own file — no runtime placeholder substitution.
//
// (Earlier revisions rewrote `ethereum_bridge` → `sepolia_bridge` here so a
// single `*.sql` could target both environments. That created a placeholder
// that only worked through this loader; `scripts/migrate.sh` applied files
// literally, so any path that bypassed in-process tests hit a missing
// `ethereum_bridge` namespace. The placeholder is now removed.)
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

	for _, path := range sqlPaths {
		content, err := seedFiles.ReadFile(path)
		if err != nil {
			panic("failed to read migration file " + path + ": " + err.Error())
		}
		statements = append(statements, string(content))
	}

	return statements
}
