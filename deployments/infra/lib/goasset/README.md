# Go Asset Bundling (`goasset`)

This package provides a standardized way to bundle Go applications or test binaries into CDK S3 assets using local Go build commands.

## Usage

Import the package:

```go
import "github.com/trufnetwork/node/infra/lib/goasset"
```

### Bundling a Directory (Common Case)

Use `BundleDir` to bundle a Go package located in a directory (e.g., `cmd/my-app` containing `main.go` and potentially other files).

```go
myAppDir := "../../cmd/my-app" // Relative path to the Go package directory

asset := goasset.BundleDir(stack, "MyAppAsset", myAppDir,
    // Optional functional options:
    func(opts *goasset.Options) {
        opts.OutName = "my-renamed-app" // Default is directory name ("my-app")
        opts.Platform = "linux/arm64"    // Default is "linux/amd64"
        opts.BuildFlags = []string{"-tags=netgo", "-ldflags=-s -w"}
        opts.Logger = myCustomLogger // Default is zap.NewNop()
    },
)

// Use asset.S3ObjectUrl, asset.S3BucketName, asset.S3ObjectKey as needed
```

### Bundling with Full Options

Use `Bundle` for more control or when bundling a single file (not recommended for tests).

```go
options := goasset.Options{
    SrcPath:    "../../cmd/another-app/main.go",
    OutName:    "another-app",
    Platform:   "linux/amd64",
    BuildFlags: []string{"-ldflags=-X main.Version=1.2.3"},
    ExtraEnv:   []string{"CGO_ENABLED=1", "MY_VAR=value"},
    Logger:     zap.L(), // Use a global logger
}

asset := goasset.Bundle(stack, "AnotherAppAsset", options)
```

### Building Test Binaries

Set `IsTest: true`. `SrcPath` **must** be the directory containing the `*_test.go` files.

```go
asset := goasset.BundleDir(stack, "MyAppTestAsset", "../../cmd/my-app",
    func(opts *goasset.Options) {
        opts.IsTest = true
        opts.OutName = "my-app.test" // Often useful to distinguish test binaries
    },
)
```

## Caching

The bundler adds `-trimpath` and `-buildvcs=false` flags by default to improve build cacheability. It also respects the `GOMODCACHE` environment variable if set during the CDK synthesis process.

## Asset Hashing

The CDK asset hash is automatically generated based on the source directory content and includes:

*   Go version (`go version`)
*   Target platform
*   Build flags
*   Extra environment variables

This ensures that changes to the Go toolchain or build configuration trigger a new asset upload. 