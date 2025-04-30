//go:build test

package observer

import (
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2/awss3assets"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/trufnetwork/node/infra/tests/testutil"
)

// GetObserverAsset is stubbed for tests to return a dummy file asset.
// It satisfies the interface expected by constructs using observer assets during unit tests.
func GetObserverAsset(scope constructs.Construct, id *string) awss3assets.Asset {
	// We need a testing.T, but since this stub only runs during tests,
	// we can create a throwaway one. A nil T would cause DummyFileAsset to panic.
	t := new(testing.T)
	return testutil.DummyFileAsset(scope, *id, t)
}
