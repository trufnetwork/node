package buildcmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	// No longer import main goasset package
	// "github.com/trufnetwork/node/infra/lib/goasset"
)

// Options configure the Go asset bundling process.
// Moved from goasset package to break import cycle.
type Options struct {
	// SrcPath is the path to the Go source directory or file to build.
	SrcPath string
	// OutName is the desired name of the executable artifact within the asset.
	OutName string
	// IsTest indicates if this is a test binary build (`go test -c`).
	IsTest bool
	// ExtraEnv defines additional environment variables for the go build command.
	ExtraEnv []string
	// Platform specifies the target GOOS/GOARCH.
	Platform string
	// BuildFlags provides extra flags for the `go build` or `go test -c` command.
	BuildFlags []string
	// GoProxy sets the GOPROXY environment variable for the build.
	GoProxy string
	// Logger specifies an optional logger instance.
	// Note: Logger field isn't used directly by Build, but kept with Options struct.
	Logger interface{} // Use interface{} or a logger interface if needed here
}

// Build constructs the *exec.Cmd needed to build a Go asset based on the provided options.
func Build(opt Options, outputPath string, srcInfo os.FileInfo) (*exec.Cmd, error) {
	// Determine target platform
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	if opt.Platform != "" {
		parts := strings.SplitN(opt.Platform, "/", 2)
		if len(parts) == 2 {
			goos = parts[0]
			goarch = parts[1]
		} else {
			return nil, fmt.Errorf("invalid target platform format '%s', expected 'GOOS/GOARCH'", opt.Platform)
		}
	}

	// Determine build command, arguments, and target
	goCmd := "go"
	var buildArgs []string
	var buildTarget string
	// Default build flags for caching and reproducibility
	defaultBuildFlags := []string{"-trimpath"}
	if !opt.IsTest {
		if !sliceContainsPrefix(opt.BuildFlags, "-buildvcs=") {
			defaultBuildFlags = append(defaultBuildFlags, "-buildvcs=false")
		}
	}

	if opt.IsTest {
		// Validation in Bundle ensures srcInfo represents a directory here
		buildArgs = []string{"test", "-c"}
		buildTarget = "."
	} else {
		buildArgs = []string{"build"}
		if srcInfo.IsDir() {
			buildTarget = "."
		} else {
			buildTarget = filepath.Base(opt.SrcPath)
		}
	}

	// Combine args
	finalBuildArgs := buildArgs
	finalBuildArgs = append(finalBuildArgs, defaultBuildFlags...)
	finalBuildArgs = append(finalBuildArgs, opt.BuildFlags...)
	finalBuildArgs = append(finalBuildArgs, "-o", outputPath)
	finalBuildArgs = append(finalBuildArgs, buildTarget)

	// Prepare environment
	env := os.Environ()
	env = append(env, fmt.Sprintf("GOOS=%s", goos))
	env = append(env, fmt.Sprintf("GOARCH=%s", goarch))
	if !sliceContains(opt.ExtraEnv, "CGO_ENABLED=1") && !sliceContains(opt.BuildFlags, "CGO_ENABLED=1") {
		env = append(env, "CGO_ENABLED=0")
	}
	if opt.GoProxy != "" {
		env = append(env, fmt.Sprintf("GOPROXY=%s", opt.GoProxy))
	}
	if modCache := os.Getenv("GOMODCACHE"); modCache != "" && !sliceContainsPrefix(opt.ExtraEnv, "GOMODCACHE=") {
		env = append(env, fmt.Sprintf("GOMODCACHE=%s", modCache))
	}
	env = append(env, opt.ExtraEnv...)
	env = filterEnv(env) // Use helper

	// Create command
	cmd := exec.Command(goCmd, finalBuildArgs...)
	cmd.Env = env

	// Set working directory
	if srcInfo.IsDir() {
		cmd.Dir = opt.SrcPath
	} else {
		cmd.Dir = filepath.Dir(opt.SrcPath)
	}

	return cmd, nil
}

// --- Helpers (Copied from goasset - consider sharing if needed elsewhere) ---

// filterEnv removes duplicate environment variables, keeping the last occurrence.
func filterEnv(env []string) []string {
	envMap := make(map[string]string, len(env))
	keysInOrder := make([]string, 0, len(env))
	for _, pair := range env {
		parts := strings.SplitN(pair, "=", 2)
		key := parts[0]
		if key == "" {
			continue
		}
		var value string
		if len(parts) == 2 {
			value = parts[1]
		}
		if _, exists := envMap[key]; !exists {
			keysInOrder = append(keysInOrder, key)
		}
		envMap[key] = value
	}
	out := make([]string, 0, len(keysInOrder))
	for _, key := range keysInOrder {
		out = append(out, fmt.Sprintf("%s=%s", key, envMap[key]))
	}
	return out
}

// sliceContains checks if a string slice contains a specific string.
func sliceContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// sliceContainsPrefix checks if a string slice contains an item starting with a specific prefix.
func sliceContainsPrefix(slice []string, prefix string) bool {
	for _, s := range slice {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}
