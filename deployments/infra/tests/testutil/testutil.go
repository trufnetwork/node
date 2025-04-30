package testutil

import (
	_ "embed"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2/awsecrassets"
	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

//---------------------------------------------------------------------
// 1. Generic helpers
//---------------------------------------------------------------------

// TmpFile creates a temp file with given content and returns its path.
func TmpFile(t *testing.T, content []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", "fixture-*")
	if err != nil {
		t.Fatalf("tmp-file: %v", err)
	}
	if _, err := f.Write(content); err != nil {
		t.Fatalf("tmp-file-write: %v", err)
	}
	f.Close()
	return f.Name()
}

// TmpDir creates a temp directory and returns its path.
func TmpDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "fixture-dir-*")
	if err != nil {
		t.Fatalf("tmp-dir: %v", err)
	}
	return dir
}

//---------------------------------------------------------------------
// 2. CDK asset fixtures
//---------------------------------------------------------------------

//go:embed Dockerfile.alpine
var alpineDockerfile string

// DummyDockerImageAsset builds a one-layer alpine image in a temp dir.
func DummyDockerImageAsset(scope constructs.Construct, id string, t *testing.T) awsecrassets.DockerImageAsset {
	t.Helper()
	ctx := TmpDir(t)
	if err := os.WriteFile(filepath.Join(ctx, "Dockerfile"), []byte(alpineDockerfile), 0o644); err != nil {
		t.Fatalf("write-dockerfile: %v", err)
	}
	return awsecrassets.NewDockerImageAsset(scope, jsii.String(id),
		&awsecrassets.DockerImageAssetProps{Directory: jsii.String(ctx)})
}

// DummyFileAsset returns an S3 asset backed by an empty file.
func DummyFileAsset(scope constructs.Construct, id string, t *testing.T) awss3assets.Asset {
	t.Helper()
	empty := TmpFile(t, nil)
	return awss3assets.NewAsset(scope, jsii.String(id),
		&awss3assets.AssetProps{Path: jsii.String(empty)})
}
