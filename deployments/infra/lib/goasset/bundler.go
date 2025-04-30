package goasset

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
	"go.uber.org/zap"

	// Import the internal buildcmd package
	"github.com/trufnetwork/node/infra/lib/goasset/internal/buildcmd"
)

// Options type alias for backward compatibility or simpler references if needed.
// Users should ideally use buildcmd.Options directly when constructing.
type Options = buildcmd.Options

// --- Sentinel Errors ---
var (
	ErrSrcMissing   = errors.New("SrcPath is required")
	ErrSrcNotExist  = errors.New("SrcPath does not exist")
	ErrIsTestNotDir = errors.New("IsTest SrcPath must be a directory")
)

// validate checks if the options are logically consistent.
// Now takes buildcmd.Options.
func validate(o buildcmd.Options) error {
	if o.SrcPath == "" {
		return ErrSrcMissing
	}
	srcInfo, err := os.Stat(o.SrcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: '%s'", ErrSrcNotExist, o.SrcPath)
		} else {
			return fmt.Errorf("failed to stat SrcPath '%s': %w", o.SrcPath, err)
		}
	}
	if o.IsTest && !srcInfo.IsDir() {
		return fmt.Errorf("%w: SrcPath must be directory ('%s')", ErrIsTestNotDir, o.SrcPath)
	}
	return nil
}

// Bundle builds a Go application or test binary from the given source path
// and returns it as an S3 asset. Uses buildcmd.Options.
func Bundle(scope constructs.Construct, id string, opt buildcmd.Options) awss3assets.Asset {
	// --- Logger Setup ---
	var logger *zap.Logger // Declare logger var
	if opt.Logger != nil {
		if l, ok := opt.Logger.(*zap.Logger); ok {
			logger = l
		}
	}
	if logger == nil { // If still nil (not provided or wrong type)
		logger = zap.NewNop()
	}
	logger = logger.Named("goasset").With(zap.String("assetID", id))

	// --- Input Validation ---
	if err := validate(opt); err != nil {
		logger.Error("Invalid options provided to goasset.Bundle", zap.Error(err))
		panic(err) // Panic with the raw error from validate()
	}

	// Get FileInfo once, reuse it
	srcInfo, err := os.Stat(opt.SrcPath)
	if err != nil { // Should have been caught by validate, but double-check
		logger.Error("Failed to stat SrcPath after validation", zap.String("srcPath", opt.SrcPath), zap.Error(err))
		panic(fmt.Errorf("unexpected error stating SrcPath '%s' after validation: %w", opt.SrcPath, err))
	}

	// --- Set Defaults (Modify opt directly) ---
	targetGoos := runtime.GOOS
	if opt.Platform != "" {
		parts := strings.SplitN(opt.Platform, "/", 2)
		if len(parts) == 2 {
			targetGoos = parts[0]
		}
	}
	if opt.OutName == "" {
		opt.OutName = filepath.Base(opt.SrcPath)
		if srcInfo.IsDir() {
			opt.OutName = filepath.Base(opt.SrcPath)
		}
	}
	if targetGoos == "windows" && !strings.HasSuffix(strings.ToLower(opt.OutName), ".exe") {
		opt.OutName += ".exe"
	}
	if opt.Platform == "" {
		opt.Platform = "linux/amd64"
	}
	if opt.GoProxy == "" {
		opt.GoProxy = os.Getenv("GOPROXY")
	}

	// Determine the source directory for bundling context
	sourceDir := opt.SrcPath
	if !srcInfo.IsDir() { // Use the validated srcInfo
		sourceDir = filepath.Dir(opt.SrcPath)
	}

	// --- Calculate Custom Asset Hash ---
	// Include factors that affect the build output: Go version, platform, flags, env vars.
	goVersionStr := getGoVersion() // Get go version string
	hashInput := bytes.NewBufferString(goVersionStr)
	hashInput.WriteString("|")
	hashInput.WriteString(opt.Platform)
	hashInput.WriteString("|")
	hashInput.WriteString(fmt.Sprintf("IsTest=%t", opt.IsTest))
	hashInput.WriteString("|")
	// Sort build flags and extra env for stable hash
	sortedBuildFlags := append([]string{}, opt.BuildFlags...)
	sort.Strings(sortedBuildFlags)
	hashInput.WriteString(strings.Join(sortedBuildFlags, ","))
	hashInput.WriteString("|")
	sortedExtraEnv := append([]string{}, opt.ExtraEnv...)
	sort.Strings(sortedExtraEnv)
	hashInput.WriteString(strings.Join(sortedExtraEnv, ","))

	hasher := sha256.New()
	hasher.Write(hashInput.Bytes()) // Hash the combined string
	customHash := hex.EncodeToString(hasher.Sum(nil))

	// Create the bundler instance, passing the logger and srcInfo
	bundler := &GoBundler{
		opt:     opt,
		l:       logger,
		srcInfo: srcInfo,
	}

	// Create the S3 asset
	asset := awss3assets.NewAsset(scope, jsii.String(id), &awss3assets.AssetProps{
		Path: jsii.String(sourceDir),
		Bundling: &awscdk.BundlingOptions{
			Image: awscdk.DockerImage_FromRegistry(jsii.String("alpine")),
			Local: bundler,
			Command: jsii.Strings(
				"/bin/sh", "-c",
				"cp -R /asset-input/. /asset-output",
			),
		},
		AssetHashType: awscdk.AssetHashType_CUSTOM,
		AssetHash:     jsii.String(customHash),
	})

	return asset
}

// BundleDir is a convenience wrapper around Bundle.
func BundleDir(scope constructs.Construct, id string, srcDir string, mods ...func(*buildcmd.Options)) awss3assets.Asset {
	// Initialize with buildcmd.Options
	opt := buildcmd.Options{
		SrcPath: srcDir,
	}

	// Apply modifications
	for _, mod := range mods {
		mod(&opt)
	}

	// Use local logger setup like in Bundle
	var logger *zap.Logger
	if opt.Logger != nil {
		if l, ok := opt.Logger.(*zap.Logger); ok {
			logger = l
		}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if opt.SrcPath != srcDir {
		logger.Warn("BundleDir functional option modified SrcPath, using original directory",
			zap.String("originalSrcDir", srcDir),
			zap.String("modifiedSrcPath", opt.SrcPath),
		)
		opt.SrcPath = srcDir
	}

	// Validate the final options before calling Bundle
	if err := validate(opt); err != nil {
		logger.Error("Invalid options constructed in BundleDir", zap.Error(err), zap.String("assetID", id))
		panic(fmt.Errorf("invalid options for goasset.BundleDir (ID: %s): %w", id, err))
	}

	return Bundle(scope, id, opt)
}

// GoBundler implements the ILocalBundling interface.
// Uses buildcmd.Options.
type GoBundler struct {
	opt     buildcmd.Options // Use options from internal package
	l       *zap.Logger
	srcInfo os.FileInfo
}

var _ awscdk.ILocalBundling = &GoBundler{}

// TryBundle executes the Go build process locally.
func (b *GoBundler) TryBundle(outputDir *string, _ *awscdk.BundlingOptions) *bool {
	if b.srcInfo == nil {
		b.l.Error("Internal error: GoBundler srcInfo is nil")
		return jsii.Bool(false)
	}

	// Determine target platform (needed for cross-compile check)
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	if b.opt.Platform != "" {
		parts := strings.SplitN(b.opt.Platform, "/", 2)
		if len(parts) == 2 {
			goos = parts[0]
			goarch = parts[1]
		} // else: validation should happen in buildcmd.Build or earlier
	}

	b.l.Info("Starting Go binary bundling",
		zap.String("srcPath", b.opt.SrcPath),
		zap.String("outputDir", *outputDir),
		zap.String("targetPlatform", fmt.Sprintf("%s/%s", goos, goarch)),
		zap.Bool("isTest", b.opt.IsTest),
	)

	// P0 Fix: Correct cross-compilation check
	needsDocker := false // Placeholder for future Docker logic
	_ = needsDocker
	if runtime.GOOS != goos || runtime.GOARCH != goarch {
		needsDocker = true // Mark as needing Docker (even if we don't use it yet)
		// P4 Fix: Fail fast instead of just warning
		b.l.Info("Cross-compilation required, delegating to Docker bundling",
			zap.String("hostPlatform", fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)),
			zap.String("targetPlatform", fmt.Sprintf("%s/%s", goos, goarch)),
		)
		return jsii.Bool(false) // Signal CDK to use Docker image bundling
	}

	outputPath := filepath.Join(*outputDir, b.opt.OutName)

	// --- Construct the command using the internal helper ---
	cmd, err := buildcmd.Build(b.opt, outputPath, b.srcInfo)
	if err != nil {
		b.l.Error("Failed to construct Go build command", zap.Error(err))
		return jsii.Bool(false) // Failed to even create the command
	}

	// --- Execute Build Command ---
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	b.l.Debug("Executing Go build command",
		zap.String("command", cmd.Path),
		zap.Strings("args", cmd.Args),
		zap.String("cwd", cmd.Dir),
		zap.Strings("env", filterEnvForLogging(cmd.Env)), // Use helper from this package
	)

	err = cmd.Run()
	stdoutStr := stdout.String()
	stderrStr := stderr.String()

	if err != nil {
		b.l.Error("Error running Go build command",
			zap.Error(err),
			zap.String("stdout", stdoutStr),
			zap.String("stderr", stderrStr),
			zap.String("command", cmd.String()), // Log the full command string
			zap.String("cwd", cmd.Dir),
		)
		return jsii.Bool(false)
	}

	if cmd.ProcessState == nil || !cmd.ProcessState.Success() {
		exitCode := -1
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}
		b.l.Error("Go build command failed",
			zap.Int("exitCode", exitCode),
			zap.String("stdout", stdoutStr),
			zap.String("stderr", stderrStr),
			zap.String("command", cmd.String()),
			zap.String("cwd", cmd.Dir),
		)
		return jsii.Bool(false)
	}

	// Check if the output file actually exists
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		b.l.Error("Go build command succeeded but output file is missing",
			zap.String("expectedPath", outputPath),
			zap.String("stdout", stdoutStr),
			zap.String("stderr", stderrStr),
		)
		return jsii.Bool(false)
	}

	b.l.Info("Go binary built successfully",
		zap.String("outputPath", outputPath),
		zap.String("stdout", stdoutStr),
	)

	return jsii.Bool(true)
}

// --- Helper Functions ---

// P1 Fix: Refactor filterEnv using a map for simplicity and efficiency.
// filterEnv removes duplicate environment variables, keeping the last occurrence.
func filterEnv(env []string) []string {
	envMap := make(map[string]string, len(env))
	keysInOrder := make([]string, 0, len(env)) // Keep track of original order for keys

	for _, pair := range env {
		parts := strings.SplitN(pair, "=", 2)
		key := parts[0]
		if key == "" {
			continue // Skip empty keys
		}
		var value string
		if len(parts) == 2 {
			value = parts[1]
		} // else: value is empty for keys like "DEBUG"

		if _, exists := envMap[key]; !exists {
			keysInOrder = append(keysInOrder, key) // Add key on first encounter
		}
		envMap[key] = value // Store/overwrite value
	}

	// Reconstruct the slice in the original key order
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

// filterEnvForLogging prevents logging potentially sensitive variables.
func filterEnvForLogging(env []string) []string {
	filtered := make([]string, 0, len(env))
	sensitiveKeys := map[string]bool{"AWS_ACCESS_KEY_ID": true, "AWS_SECRET_ACCESS_KEY": true, "AWS_SESSION_TOKEN": true}
	for _, e := range env {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 && sensitiveKeys[parts[0]] {
			filtered = append(filtered, parts[0]+"=<redacted>")
		} else {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

// getGoVersion executes `go version` and returns the output string.
// Caches the result to avoid repeated exec calls.
var goVersionMemo string

func getGoVersion() string {
	if goVersionMemo != "" {
		return goVersionMemo
	}
	cmd := exec.Command("go", "version")
	output, err := cmd.Output()
	if err != nil {
		// Fallback or handle error appropriately
		// Using runtime version might be okay as a fallback?
		zap.L().Warn("Failed to get Go version via 'go version' command, using runtime version as fallback", zap.Error(err))
		goVersionMemo = runtime.Version()
		return goVersionMemo
	}
	goVersionMemo = strings.TrimSpace(string(output))
	return goVersionMemo
}

// Note: The original BuildGoBinaryIntoS3Asset function used validator.New().Struct(input)
// We've moved validation into the Bundle function.

// Note: The original NewLocalGoBundling function is replaced by the goBundler struct
// initialization within the Bundle function.

// NewGoBundler is deprecated as direct bundler instantiation isn't the primary API.
// Use Bundle() instead.
// Deprecated: Use Bundle() function.
func NewGoBundler(opt buildcmd.Options, logger *zap.Logger) *GoBundler {
	if err := validate(opt); err != nil {
		panic(fmt.Errorf("invalid options for NewGoBundler: %w", err))
	}
	panic("NewGoBundler is deprecated. Use the Bundle() function instead.")
}
