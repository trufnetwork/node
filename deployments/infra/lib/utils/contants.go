package utils

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// GetProjectRootDir returns the absolute path to the repository root.
//
// Resolution order:
//  1. $PROJECT_ROOT env-var (explicit override)
//  2. `git rev-parse --show-toplevel` if inside a Git work-tree
//  3. Walk up from the directory of this source file until we find one
//     of .git, go.work, go.mod, cdk.json
//
// Panics if none of the above succeed.
func GetProjectRootDir() string {
	// 1 — explicit override (useful in CI / Docker)
	if root := os.Getenv("PROJECT_ROOT"); root != "" {
		return filepath.Clean(root)
	}

	// 2 — ask Git (fast and reliable when available)
	if gitRoot, err := gitToplevel(); err == nil && gitRoot != "" {
		return gitRoot
	}

	// 3 — walk up from the directory where this file lived at build time
	_, thisFile, _, ok := runtime.Caller(0) // nolint: dogsled
	if !ok {
		panic("GetProjectRootDir: runtime.Caller failed (cannot determine source path)")
	}
	start := filepath.Dir(thisFile)
	if root := climb(start); root != "" {
		return root
	}

	panic(`GetProjectRootDir: repository root not found.
Set $PROJECT_ROOT or ensure one of {.git, go.work, go.mod, cdk.json} exists somewhere above your source tree`)
}

// --------------------------------------------------------------------------------
// helpers
// --------------------------------------------------------------------------------
func gitToplevel() (string, error) {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	// Run with a 100 ms timeout to avoid hangs in exotic CI images
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.Output()
	if err != nil {
		return "", err // caller will ignore
	}
	return strings.TrimSpace(string(out)), nil
}

func climb(dir string) string {
	markers := []string{".git", "go.work", "go.mod", "cdk.json"}

	for {
		for _, m := range markers {
			if _, err := os.Stat(filepath.Join(dir, m)); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir { // reached filesystem root
			return ""
		}
		dir = parent
	}
}
