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

	"github.com/cockroachdb/apd/v3"
	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"

	kwilTesting "github.com/kwilteam/kwil-db/testing"
	"github.com/stretchr/testify/assert"
)

var (
	investigatedDeployerAddressString = "0x0000000000000000000000000000000000000ABC"
	investigatedComposedStreamId      = "stb11111111111111111111111111111"
	investigatedChildDeployer         = util.Unsafe_NewEthereumAddressFromString(investigatedDeployerAddressString)
)

type childStreamDefinition struct {
	id      string
	weight  string
	records []setup.InsertRecordInput
}

func strToAPDDecimal(val string) apd.Decimal {
	d, _, err := apd.NewFromString(val)
	if err != nil {
		panic(fmt.Sprintf("Error converting string %s to apd.Decimal: %v", val, err))
	}
	return *d
}

// Child stream data derived from new-investigation.md and user-provided table.
// Values are those seen when children are queried with from_ts=1746921600,
// except for st04eb... which has two distinct records.
var investigatedChildDefs = []childStreamDefinition{
	{id: "stb523fb7c77c14e8c4c4275fc31c65b", weight: "15.300", records: []setup.InsertRecordInput{{EventTime: 1745798400, Value: 1520.733868919920579452}}},
	{id: "st219f6ecde94642ab9117be6f60e770", weight: "23.200", records: []setup.InsertRecordInput{{EventTime: 1745798400, Value: 707.804220369060001316}}},
	{id: "st04ebb38623197b73a85c8e4d92cdff", weight: "19.832", records: []setup.InsertRecordInput{ // Special case
		{EventTime: 1746921600, Value: 12308.607809587054523883}, // Contributes when parent from_ts is early
		{EventTime: 1747008000, Value: 12308.607616916077965804}, // Contributes when parent from_ts is late
	}},
	{id: "st32ad2108a36905d03b05e4384ead1a", weight: "5.899", records: []setup.InsertRecordInput{{EventTime: 1743465600, Value: 191.747120357586331598}}},
	{id: "st7937623c83f27b64dcb7ad2a1be2c0", weight: "8.545", records: []setup.InsertRecordInput{{EventTime: 1740787200, Value: 618.131289696426325047}}},
	{id: "st27e2bb3d3c00693488adc6c3a84170", weight: "7.164", records: []setup.InsertRecordInput{{EventTime: 1745625600, Value: 163.435214761157390107}}},
	{id: "st9a8963bf3f65a1fed4d0c3fa682e3e", weight: "1.900", records: []setup.InsertRecordInput{{EventTime: 1745625600, Value: 573.356227622544589458}}},
	{id: "st7fd2d9cb0b76ff918c88318989be22", weight: "3.800", records: []setup.InsertRecordInput{{EventTime: 1743465600, Value: 122.510840189398938932}}},
	{id: "stca38a7e8b0a454a82cadca0055eadb", weight: "3.176", records: []setup.InsertRecordInput{{EventTime: 1743465600, Value: 48.473187523571930400}}},
	{id: "st28a92425e5d4ed5ec76a0cc5c96419", weight: "2.334", records: []setup.InsertRecordInput{{EventTime: 1743465600, Value: 2993.164021502373261308}}},
	{id: "st5b77cc0678284182b893830b0e249d", weight: "5.606", records: []setup.InsertRecordInput{{EventTime: 1745625600, Value: 184.013556338948606493}}},
	{id: "st72ab74384a83566d6faec830a9b219", weight: "3.244", records: []setup.InsertRecordInput{{EventTime: 1745625600, Value: 161.537195283600493218}}},
}

func TestPathDependencyBugReproduction(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "path_dependency_bug_repro_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			setupInvestigatedStream(testPathDependencyBugReproFunc(t)),
		},
	}, testutils.GetTestOptions())
}

func setupInvestigatedStream(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		platform = procedure.WithSigner(platform, investigatedChildDeployer.Bytes()) // Signer for deploying children and parent

		childStreamIds := make([]string, len(investigatedChildDefs))
		childDataProviders := make([]string, len(investigatedChildDefs))
		childWeights := make([]string, len(investigatedChildDefs))

		for i, childDef := range investigatedChildDefs {
			// Deploy primitive stream for the child
			err := setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
				Platform: platform,
				Height:   1,
				PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
					PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
						StreamLocator: types.StreamLocator{StreamId: *util.NewRawStreamId(childDef.id), DataProvider: investigatedChildDeployer},
					},
					Data: childDef.records,
				},
			})
			if err != nil {
				return errors.Wrapf(err, "error deploying primitive stream %s", childDef.id)
			}
			childStreamIds[i] = childDef.id
			childDataProviders[i] = investigatedChildDeployer.Address()
			childWeights[i] = childDef.weight
		}

		// Deploy the composed stream
		err := setup.SetupComposedStream(ctx, setup.SetupComposedStreamInput{
			Platform: platform,
			StreamId: *util.NewRawStreamId(investigatedComposedStreamId),
			Height:   2, // Deploy after children
		})
		if err != nil {
			return errors.Wrap(err, "error deploying investigated composed stream")
		}

		// Set taxonomy for composed stream
		if err := procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform:      platform,
			StreamLocator: types.StreamLocator{StreamId: *util.NewRawStreamId(investigatedComposedStreamId), DataProvider: investigatedChildDeployer},
			DataProviders: childDataProviders,
			StreamIds:     childStreamIds,
			Weights:       childWeights,
		}); err != nil {
			return errors.Wrap(err, "error setting taxonomy for composed stream")
		}

		return testFn(ctx, platform)
	}
}

func testPathDependencyBugReproFunc(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		parentStreamLocator := types.StreamLocator{
			StreamId:     *util.NewRawStreamId(investigatedComposedStreamId),
			DataProvider: investigatedChildDeployer,
		}

		targetEventTime := int64(1747008000)
		var valueFromQuery1, valueFromQuery2 string

		// Query 1: FromTime is early (1746921600)
		fromTime1 := int64(1746921600)
		resultQuery1, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: parentStreamLocator,
			FromTime:      &fromTime1,
			ToTime:        &targetEventTime,
		})
		if !assert.NoError(t, err, "GetRecord (Query 1) should not return an error") {
			return errors.Wrap(err, "error in GetRecord (Query 1)")
		}

		found1 := false
		for _, row := range resultQuery1 {
			if et, _ := strconv.ParseInt(row[0], 10, 64); et == targetEventTime {
				valueFromQuery1 = row[1]
				found1 = true
				break
			}
		}
		if !assert.True(t, found1, "Target event time %d not found in Query 1 results. Got: %v", targetEventTime, resultQuery1) {
			// Log all rows from Query 1 if target not found
			t.Logf("Query 1 (from %d) results for parent stream %s:", fromTime1, parentStreamLocator.StreamId)
			for _, row := range resultQuery1 {
				t.Logf("  EventTime: %s, Value: %s", row[0], row[1])
			}
			return fmt.Errorf("target event time %d not found in Query 1 results", targetEventTime)
		}

		// Query 2: FromTime is the target event time (1747008000)
		fromTime2 := int64(1747008000)
		resultQuery2, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: parentStreamLocator,
			FromTime:      &fromTime2,
			ToTime:        &targetEventTime,
		})
		if !assert.NoError(t, err, "GetRecord (Query 2) should not return an error") {
			return errors.Wrap(err, "error in GetRecord (Query 2)")
		}

		found2 := false
		for _, row := range resultQuery2 {
			if et, _ := strconv.ParseInt(row[0], 10, 64); et == targetEventTime {
				valueFromQuery2 = row[1]
				found2 = true
				break
			}
		}
		if !assert.True(t, found2, "Target event time %d not found in Query 2 results. Got: %v", targetEventTime, resultQuery2) {
			// Log all rows from Query 2 if target not found
			t.Logf("Query 2 (from %d) results for parent stream %s:", fromTime2, parentStreamLocator.StreamId)
			for _, row := range resultQuery2 {
				t.Logf("  EventTime: %s, Value: %s", row[0], row[1])
			}
			return fmt.Errorf("target event time %d not found in Query 2 results", targetEventTime)
		}

		// The core assertion: values should be different due to the bug.
		assert.NotEqual(t, valueFromQuery1, valueFromQuery2,
			"Path Dependency Bug: Values for targetEventTime %d should be DIFFERENT. "+
				"Query1 (from=%d) gave '%s', Query2 (from=%d) gave '%s'. "+
				"This demonstrates the bug where the 'from_ts' in the parent query incorrectly alters the calculated value for a specific event_time.",
			targetEventTime, fromTime1, valueFromQuery1, fromTime2, valueFromQuery2)

		t.Logf("Path Dependency Bug Reproduction Test: For targetEventTime %d", targetEventTime)
		t.Logf("  Query 1 (from=%d): Value = %s", fromTime1, valueFromQuery1)
		t.Logf("  Query 2 (from=%d): Value = %s", fromTime2, valueFromQuery2)
		t.Logf("  The values being different demonstrates the bug.")

		// For reference, based on manual calculations with provided child data and weights:
		// Expected value for Query 1 (from=1746921600) ~ 3016.27014489...
		// Expected value for Query 2 (from=1747008000) ~ 3016.27010741...
		// The exact string representation depends on DB precision (typically 18 decimal places for composed streams).
		// e.g. "3016.270144894308763429" vs "3016.270107410370784862"

		return nil
	}
}
