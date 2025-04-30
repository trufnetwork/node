package goasset_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/trufnetwork/node/infra/lib/goasset"
	// Import internal buildcmd for Options type and Build function
	"github.com/trufnetwork/node/infra/lib/goasset/internal/buildcmd"
)

// Test Suite
type GoAssetSuite struct {
	suite.Suite
	tmpDir string
	logger *zap.Logger // Suite-level logger
	app    awscdk.App  // CDK context for Bundle calls
	stack  awscdk.Stack
}

func (s *GoAssetSuite) SetupSuite() {
	var err error
	// Use a logger that writes to stderr for easy visibility during tests
	s.logger, err = zap.NewDevelopment()
	s.Require().NoError(err)
}

func (s *GoAssetSuite) SetupTest() {
	var err error
	s.tmpDir, err = os.MkdirTemp("", "goasset-test-*")
	s.Require().NoError(err, "Failed to create temp dir")

	// Create a simple dummy go module and main file in a subdirectory
	srcDir := filepath.Join(s.tmpDir, "mysrc")
	err = os.MkdirAll(srcDir, 0755)
	s.Require().NoError(err)

	mainGo := filepath.Join(srcDir, "main.go")
	mainContent := `
package main
import "fmt"
func main() {
	fmt.Println("Hello from test app!")
}
`
	err = os.WriteFile(mainGo, []byte(mainContent), 0644)
	s.Require().NoError(err)

	goMod := filepath.Join(srcDir, "go.mod")
	goModContent := `
module testmodule
go 1.21
`
	err = os.WriteFile(goMod, []byte(goModContent), 0644)
	s.Require().NoError(err)

	// Need App/Stack for Bundle calls
	s.app = awscdk.NewApp(nil)
	s.stack = awscdk.NewStack(s.app, jsii.String("TestStack"), nil)
}

func (s *GoAssetSuite) TearDownTest() {
	err := os.RemoveAll(s.tmpDir)
	s.Require().NoError(err, "Failed to remove temp dir")
}

func TestGoAssetSuite(t *testing.T) {
	suite.Run(t, new(GoAssetSuite))
}

// --- Test Cases ---

func (s *GoAssetSuite) TestSimpleBuild_Success() {
	srcPath := filepath.Join(s.tmpDir, "mysrc")
	outName := "myTestApp"
	testLogger := s.logger.Named("TestSimpleBuild")

	// Use buildcmd.Options
	options := buildcmd.Options{
		SrcPath:  srcPath,
		OutName:  outName,
		Platform: fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		Logger:   testLogger,
	}

	// Call Bundle - uses buildcmd.Options
	asset := goasset.Bundle(s.stack, "TestAssetSimple", options)
	s.Require().NotNil(asset)

	outDir, err := os.MkdirTemp("", "bundle-out-*")
	s.Require().NoError(err)
	defer os.RemoveAll(outDir)

	expectedBinaryPath := filepath.Join(outDir, outName)
	// Adjust expected path for Windows target
	if goos, _, _ := strings.Cut(options.Platform, "/"); goos == "windows" {
		if !strings.HasSuffix(expectedBinaryPath, ".exe") {
			expectedBinaryPath += ".exe"
		}
	}

	// Use the buildcmd.Build helper to get the command
	srcInfo, err := os.Stat(options.SrcPath)
	s.Require().NoError(err)
	cmd, err := buildcmd.Build(options, expectedBinaryPath, srcInfo)
	s.Require().NoError(err)

	// Execute the command
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	testLogger.Debug("Executing build command", zap.String("dir", cmd.Dir), zap.Strings("cmd", cmd.Args))
	err = cmd.Run()
	s.Require().NoError(err, "go build command failed. Stdout: %s\nStderr: %s", stdout.String(), stderr.String())
	s.Require().True(cmd.ProcessState.Success(), "go build command did not succeed. Stdout: %s\nStderr: %s", stdout.String(), stderr.String())

	// Check result
	info, err := os.Stat(expectedBinaryPath)
	s.Require().NoError(err, "Expected output binary %s to exist", expectedBinaryPath)
	if runtime.GOOS != "windows" {
		s.Require().True((info.Mode()&0111) != 0, "Binary should be executable")
	}
	s.Require().Greater(info.Size(), int64(1000), "Binary size should be reasonable")
}

func (s *GoAssetSuite) TestBuildTestBinary_Success() {
	srcDir := filepath.Join(s.tmpDir, "mysrc")
	testGo := filepath.Join(srcDir, "main_test.go")
	testContent := `package main
import "testing"
func TestDummy(t *testing.T) { t.Log("Running dummy test") }`
	err := os.WriteFile(testGo, []byte(testContent), 0644)
	s.Require().NoError(err)

	outName := "myTestPkg.test"
	testLogger := s.logger.Named("TestBuildTestBinary")

	options := buildcmd.Options{
		SrcPath:  srcDir,
		OutName:  outName,
		Platform: fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		IsTest:   true,
		Logger:   testLogger,
	}

	asset := goasset.Bundle(s.stack, "TestAssetTestBin", options)
	s.Require().NotNil(asset)

	outDir, err := os.MkdirTemp("", "bundle-test-out-*")
	s.Require().NoError(err)
	defer os.RemoveAll(outDir)

	expectedBinaryPath := filepath.Join(outDir, outName)
	if goos, _, _ := strings.Cut(options.Platform, "/"); goos == "windows" {
		if !strings.HasSuffix(expectedBinaryPath, ".exe") {
			expectedBinaryPath += ".exe"
		}
	}

	srcInfo, err := os.Stat(options.SrcPath)
	s.Require().NoError(err)
	cmd, err := buildcmd.Build(options, expectedBinaryPath, srcInfo)
	s.Require().NoError(err)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	testLogger.Debug("Executing test build command", zap.String("dir", cmd.Dir), zap.Strings("cmd", cmd.Args))
	err = cmd.Run()
	s.Require().NoError(err, "go test -c command failed. Stdout: %s\nStderr: %s", stdout.String(), stderr.String())
	s.Require().True(cmd.ProcessState.Success(), "go test -c command did not succeed. Stdout: %s\nStderr: %s", stdout.String(), stderr.String())

	info, err := os.Stat(expectedBinaryPath)
	s.Require().NoError(err, "Expected output test binary %s to exist", expectedBinaryPath)
	if runtime.GOOS != "windows" {
		s.Require().True((info.Mode()&0111) != 0, "Test binary should be executable")
	}
	s.Require().Greater(info.Size(), int64(1000), "Test binary size should be reasonable")
}

func (s *GoAssetSuite) TestBuild_SyntaxError() {
	srcDir := filepath.Join(s.tmpDir, "mysrc_syntax_error")
	err := os.MkdirAll(srcDir, 0755)
	s.Require().NoError(err)

	mainGo := filepath.Join(srcDir, "main.go")
	mainContent := `package main
import "fmt"
func main() { fmt.Println("Hello!")
this is not valid go code }`
	err = os.WriteFile(mainGo, []byte(mainContent), 0644)
	s.Require().NoError(err)

	goMod := filepath.Join(srcDir, "go.mod")
	goModContent := `module testmodule_syntaxerror
go 1.21`
	err = os.WriteFile(goMod, []byte(goModContent), 0644)
	s.Require().NoError(err)

	outName := "syntaxErrorApp"
	testLogger := s.logger.Named("TestBuildSyntaxError")

	options := buildcmd.Options{
		SrcPath:  srcDir,
		OutName:  outName,
		Platform: fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		Logger:   testLogger,
	}

	asset := goasset.Bundle(s.stack, "TestAssetSyntaxError", options)
	s.Require().NotNil(asset)

	outDir, err := os.MkdirTemp("", "bundle-syntax-err-out-*")
	s.Require().NoError(err)
	defer os.RemoveAll(outDir)

	expectedBinaryPath := filepath.Join(outDir, outName)
	if goos, _, _ := strings.Cut(options.Platform, "/"); goos == "windows" {
		if !strings.HasSuffix(expectedBinaryPath, ".exe") {
			expectedBinaryPath += ".exe"
		}
	}

	srcInfo, err := os.Stat(options.SrcPath)
	s.Require().NoError(err)
	cmd, err := buildcmd.Build(options, expectedBinaryPath, srcInfo)
	s.Require().NoError(err)

	var stderr bytes.Buffer
	cmd.Stdout = &bytes.Buffer{}
	cmd.Stderr = &stderr
	testLogger.Debug("Executing build command (expecting failure)", zap.String("dir", cmd.Dir), zap.Strings("cmd", cmd.Args))
	err = cmd.Run()
	s.Require().Error(err, "go build command should fail due to syntax error")
	s.Require().False(cmd.ProcessState.Success(), "go build command process state should indicate failure")

	stderrStr := stderr.String()
	s.Require().Contains(stderrStr, "syntax error", "Stderr should contain syntax error message")
	testLogger.Info("Build failed as expected", zap.String("stderr", stderrStr))

	_, err = os.Stat(expectedBinaryPath)
	s.Require().Error(err, "Output binary should not exist after failed build")
	s.Require().True(os.IsNotExist(err), "Error should be os.IsNotExist for the output binary")
}

// --- Validation Tests ---

func (s *GoAssetSuite) TestValidation_SrcPathMissing() {
	options := buildcmd.Options{OutName: "test", Logger: s.logger}
	s.Require().PanicsWithError(goasset.ErrSrcMissing.Error(), func() {
		goasset.Bundle(s.stack, "TestValidationSrcMissing", options)
	}, "Expected panic due to missing SrcPath")
}

func (s *GoAssetSuite) TestValidation_SrcPathDoesNotExist() {
	nonExistentPath := filepath.Join(s.tmpDir, "does-not-exist")
	options := buildcmd.Options{
		SrcPath: nonExistentPath,
		OutName: "test",
		Logger:  s.logger,
	}
	// Check for wrapped error ErrSrcNotExist
	s.Require().Panics(func() {
		defer func() {
			if r := recover(); r != nil {
				err, ok := r.(error)
				s.Require().True(ok, "Panic value should be an error")
				s.Require().ErrorIs(err, goasset.ErrSrcNotExist, "Panic error should wrap ErrSrcNotExist")
				panic(r)
			} else {
				s.Fail("Expected a panic but did not get one")
			}
		}()
		goasset.Bundle(s.stack, "TestValidationSrcNotExist", options)
	}, "Expected panic due to non-existent SrcPath")
}

func (s *GoAssetSuite) TestValidation_IsTestWithFile() {
	mainGoFile := filepath.Join(s.tmpDir, "mysrc", "main.go")
	options := buildcmd.Options{
		SrcPath: mainGoFile,
		OutName: "test",
		IsTest:  true,
		Logger:  s.logger,
	}
	s.Require().Panics(func() {
		defer func() {
			if r := recover(); r != nil {
				err, ok := r.(error)
				s.Require().True(ok, "Panic value should be an error")
				s.Require().ErrorIs(err, goasset.ErrIsTestNotDir, "Panic error should wrap ErrIsTestNotDir")
				panic(r)
			} else {
				s.Fail("Expected a panic but did not get one")
			}
		}()
		goasset.Bundle(s.stack, "TestValidationIsTestFile", options)
	}, "Expected panic due to IsTest=true with a file SrcPath")
}

// TODO: Add tests for:
// - BuildFlags and ExtraEnv being passed correctly to the simulated command
