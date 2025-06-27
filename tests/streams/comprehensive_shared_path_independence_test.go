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

	"github.com/stretchr/testify/assert"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

var (
	// Shared deployer for comprehensive shared primitive test
	deployerSharedComplex = util.Unsafe_NewEthereumAddressFromString("0x51111111111111ABCDEF0123456789ABCDEF0123")

	// Stream IDs for this simplified test
	gcp1StreamIdShared = util.GenerateStreamId("s_gcp1_shrd_pd")
	cc2StreamIdShared  = util.GenerateStreamId("s_cc2_shrd_pd")
	rcStreamIdShared   = util.GenerateStreamId("s_rc_main_shrd_pd")
)

// TestComprehensivePathIndependenceWithSharedPrimitive tests path independence
// for a hierarchy where a single primitive contributes via multiple composed paths.
func TestComprehensivePathIndependenceWithSharedPrimitive(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "comprehensive_path_independence_shared_primitive_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			setupComprehensiveSharedStreams(testComprehensivePathIndependenceSharedFunc(t)),
		},
	}, testutils.GetTestOptions())
}

// setupComprehensiveSharedStreams sets up GCP1, GCP2, SP primitives and CC1, CC2, RC composed streams
// where GCP1 is shared between CC1 and CC2.
func setupComprehensiveSharedStreams(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, deployerSharedComplex.Bytes())
		start := int64(0)

		// 1. Deploy primitives
		if err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{StreamId: gcp1StreamIdShared, DataProvider: deployerSharedComplex},
				},
				Data: []setup.InsertRecordInput{{EventTime: 100, Value: 10.0}, {EventTime: 300, Value: 15.0}},
			},
		}); err != nil {
			return errors.Wrap(err, "deploying GCP1")
		}

		// 2. Deploy composed streams
		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{Platform: platform, StreamId: cc2StreamIdShared, Height: 2}); err != nil {
			return errors.Wrap(err, "deploying CC2")
		}
		if err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{Platform: platform, StreamId: rcStreamIdShared, Height: 2}); err != nil {
			return errors.Wrap(err, "deploying RC")
		}

		// 3. Set static taxonomies
		// CC2 -> GCP1 only
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{Platform: platform, StreamLocator: types.StreamLocator{StreamId: cc2StreamIdShared, DataProvider: deployerSharedComplex}, DataProviders: []string{deployerSharedComplex.Address()}, StreamIds: []string{gcp1StreamIdShared.String()}, Weights: []string{"1.0"}, StartTime: &start, Height: 3}); err != nil {
			return errors.Wrap(err, "tax for CC2")
		}
		// RC -> GCP1 direct, CC2
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{Platform: platform, StreamLocator: types.StreamLocator{StreamId: rcStreamIdShared, DataProvider: deployerSharedComplex}, DataProviders: []string{deployerSharedComplex.Address(), deployerSharedComplex.Address()}, StreamIds: []string{gcp1StreamIdShared.String(), cc2StreamIdShared.String()}, Weights: []string{"1.0", "1.0"}, StartTime: &start, Height: 3}); err != nil {
			return errors.Wrap(err, "tax for RC")
		}

		return testFn(ctx, platform)
	}
}

// testComprehensivePathIndependenceSharedFunc queries and asserts path independence and correctness.
func testComprehensivePathIndependenceSharedFunc(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		locator := types.StreamLocator{StreamId: rcStreamIdShared, DataProvider: deployerSharedComplex}
		target := int64(300)

		// queryAndExtract runs a GetRecord and extracts the value at target
		queryAndExtract := func(fromTime int64, name string) (string, error) {
			res, err := procedure.GetRecord(ctx, procedure.GetRecordInput{Platform: platform, StreamLocator: locator, FromTime: &fromTime, ToTime: &target})
			if err != nil {
				return "", errors.Wrapf(err, "GetRecord %s failed", name)
			}
			for _, r := range res {
				if et, _ := strconv.ParseInt(r[0], 10, 64); et == target {
					return r[1], nil
				}
			}
			return "", fmt.Errorf("%s: target %d not found", name, target)
		}

		v1, err := queryAndExtract(50, "Q1")
		if !assert.NoError(t, err) {
			return err
		}
		v2, err := queryAndExtract(150, "Q2")
		if !assert.NoError(t, err) {
			return err
		}
		v3, err := queryAndExtract(275, "Q3")
		if !assert.NoError(t, err) {
			return err
		}
		v4, err := queryAndExtract(300, "Q4")
		if !assert.NoError(t, err) {
			return err
		}

		t.Logf("Values: Q1=%s Q2=%s Q3=%s Q4=%s", v1, v2, v3, v4)
		assert.Equal(t, v1, v2, "RC@%d path independence Q1 vs Q2", target)
		assert.Equal(t, v1, v3, "RC@%d path independence Q1 vs Q3", target)
		assert.Equal(t, v1, v4, "RC@%d path independence Q1 vs Q4", target)

		expected := "15.000000000000000000"
		assert.Equal(t, expected, v1, "RC@%d correct value mismatch", target)

		return nil
	}
}
