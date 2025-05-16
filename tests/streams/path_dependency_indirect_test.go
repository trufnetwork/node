package tests

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"

	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/stretchr/testify/assert"
)

var (
	// Shared deployer for path dependency test
	deployerAddrStringPD = "0xABCDEF0123456789ABCDEF0123456789ABCDEF01"
	sharedDeployerPD     = util.Unsafe_NewEthereumAddressFromString(deployerAddrStringPD)

	// Stream IDs for the path dependency test
	gcpStreamIdPD = util.GenerateStreamId("st_gcp_indirect_pd")
	spStreamIdPD  = util.GenerateStreamId("st_sp_direct_pd")
	ccStreamIdPD  = util.GenerateStreamId("st_cc_intermed_pd")
	rcStreamIdPD  = util.GenerateStreamId("st_rc_main_pathd")
)

// TestPathDependencyIndirectChildrenCorrectness tests path independence (correct behavior) for indirect children.
func TestPathDependencyIndirectChildrenCorrectness(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "path_dependency_indirect_children_correctness_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			setupPathDependencyIndirectStreams(testPathDependencyIndirectCorrectnessFunc(t)),
		},
	}, testutils.GetTestOptions())
}

// setupPathDependencyIndirectStreams prepares a GCP->CC->RC and SP->RC hierarchy.
func setupPathDependencyIndirectStreams(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, sharedDeployerPD.Bytes())

		// 1. Deploy Grandchild Primitive (GCP)
		gcpRecords := []setup.InsertRecordInput{{EventTime: 100, Value: 10.0}, {EventTime: 200, Value: 20.0}}
		err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{StreamLocator: types.StreamLocator{StreamId: gcpStreamIdPD, DataProvider: sharedDeployerPD}},
				Data:                      gcpRecords,
			},
		})
		if err != nil {
			return errors.Wrap(err, "error deploying GCP stream for path dependency test")
		}

		// 2. Deploy Sibling Primitive (SP)
		spRecords := []setup.InsertRecordInput{{EventTime: 100, Value: 50.0}, {EventTime: 200, Value: 60.0}}
		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform:                platform,
			Height:                  1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{StreamLocator: types.StreamLocator{StreamId: spStreamIdPD, DataProvider: sharedDeployerPD}}, Data: spRecords},
		})
		if err != nil {
			return errors.Wrap(err, "error deploying SP stream for path dependency test")
		}

		// 3. Deploy Child Composed (CC)
		err = setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{Platform: platform, StreamId: ccStreamIdPD, Height: 2})
		if err != nil {
			return errors.Wrap(err, "error deploying CC stream for path dependency test")
		}

		// 4. Deploy Root Composed (RC)
		err = setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{Platform: platform, StreamId: rcStreamIdPD, Height: 2})
		if err != nil {
			return errors.Wrap(err, "error deploying RC stream for path dependency test")
		}

		// 5. Taxonomy for CC -> GCP
		start := int64(0)
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: types.StreamLocator{StreamId: ccStreamIdPD, DataProvider: sharedDeployerPD},
			DataProviders: []string{sharedDeployerPD.Address()},
			StreamIds:     []string{gcpStreamIdPD.String()},
			Weights:       []string{"1.0"},
			StartTime:     &start,
			Height:        3,
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for CC")
		}

		// 6. Taxonomy for RC -> CC, SP
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: types.StreamLocator{StreamId: rcStreamIdPD, DataProvider: sharedDeployerPD},
			DataProviders: []string{sharedDeployerPD.Address(), sharedDeployerPD.Address()},
			StreamIds:     []string{ccStreamIdPD.String(), spStreamIdPD.String()},
			Weights:       []string{"0.7", "0.3"},
			StartTime:     &start,
			Height:        3,
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for RC")
		}

		return testFn(ctx, platform)
	}
}

// testPathDependencyIndirectCorrectnessFunc runs queries and asserts path independence.
func testPathDependencyIndirectCorrectnessFunc(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		locator := types.StreamLocator{StreamId: rcStreamIdPD, DataProvider: sharedDeployerPD}
		target := int64(200)
		var v1, v2 string

		// Query1: from=100
		f1 := int64(100)
		res1, err := procedure.GetRecord(ctx, procedure.GetRecordInput{Platform: platform, StreamLocator: locator, FromTime: &f1, ToTime: &target})
		if !assert.NoError(t, err, "GetRecord (Query1 with from=%d) failed", f1) {
			return errors.Wrap(err, "GetRecord Query1 failed")
		}
		found := false
		for _, r := range res1 {
			if et, _ := strconv.ParseInt(r[0], 10, 64); et == target {
				v1 = r[1]
				found = true
				break
			}
		}
		if !assert.True(t, found, "Target event time %d not found in Query1 (from=%d) results. Got: %v", target, f1, res1) {
			t.Logf("Query1 (from %d to %d) results: %v", f1, target, res1)
			return fmt.Errorf("target event time %d not found in Query1 results", target)
		}

		// Query2: from=200
		f2 := int64(200)
		res2, err := procedure.GetRecord(ctx, procedure.GetRecordInput{Platform: platform, StreamLocator: locator, FromTime: &f2, ToTime: &target})
		if !assert.NoError(t, err, "GetRecord (Query2 with from=%d) failed", f2) {
			return errors.Wrap(err, "GetRecord Query2 failed")
		}
		found = false
		for _, r := range res2 {
			if et, _ := strconv.ParseInt(r[0], 10, 64); et == target {
				v2 = r[1]
				found = true
				break
			}
		}
		if !assert.True(t, found, "Target event time %d not found in Query2 (from=%d) results. Got: %v", target, f2, res2) {
			t.Logf("Query2 (from %d to %d) results: %v", f2, target, res2)
			return fmt.Errorf("target event time %d not found in Query2 results", target)
		}

		// Assert path independence: values should be equal
		assert.Equal(t, v1, v2,
			"Path Independence Check: Values for targetEventTime %d should be EQUAL. Query1 (from=%d) gave '%s', Query2 (from=%d) gave '%s'. A difference indicates a path dependency issue.",
			target, f1, v1, f2, v2)
		t.Logf("Path Independence Check: For targetEventTime %d", target)
		t.Logf("  Query 1 (from=%d): Value = %s", f1, v1)
		t.Logf("  Query 2 (from=%d): Value = %s", f2, v2)
		if v1 == v2 {
			t.Logf("  The values being the same DEMONSTRATES CORRECT path independence for this scenario.")
		} else {
			t.Logf("  The values being different INDICATES A PATH DEPENDENCY BUG for this scenario.")
		}
		// Check against expected correct value
		expectedCorrect := "32.000000000000000000"
		t.Logf("  Reference (Correct Value): Expected ~%s", expectedCorrect)
		if v1 == v2 {
			assert.Equal(t, expectedCorrect, v1,
				"Calculated value '%s' for RC@%d does not match expected correct value '%s'", v1, target, expectedCorrect)
		}
		return nil
	}
}
