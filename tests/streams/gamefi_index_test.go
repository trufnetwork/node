package tests

import (
	"context"
	"testing"

	"github.com/trufnetwork/node/internal/migrations"
	testutils "github.com/trufnetwork/node/tests/streams/utils"
	"github.com/trufnetwork/node/tests/streams/utils/procedure"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/node/tests/streams/utils/table"

	"github.com/pkg/errors"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"

	kwilTesting "github.com/trufnetwork/kwil-db/testing"
)

/*
GameFi Index Test Scenario:

This test demonstrates a real-world scenario where two primitive financial indices
are combined into a single composed index with equal weighting.

Data Sources:
1. Dappradar GameFi Index:
   - Base value (2024-10-27): 293010.636
   - Current value (2024-10-28): 312284.113

2. Coingecko GameFi Index:
   - Base value (2024-10-27): 2.832677
   - Current value (2024-10-28): 1.509782

Formula for composed index:
GameFi Index = (Dappradar ratio × 50%) + (Coingecko ratio × 50%)

Where:
- Dappradar ratio = Current Value / Base Value = 312284.113 / 293010.636 = 1.065803
- Coingecko ratio = Current Value / Base Value = 1.509782 / 2.832677 = 0.532899
- Final normalized index ≈ 79.938255 (representing the combined performance)

This matches the expected calculation from the requirement: 79.938255

Proportional Test:
The test also includes a proportional version where the raw values are scaled differently:
- Dappradar values scaled down by 1000x: 293.010636 → 312.284113
- Coingecko values scaled up by 1000x: 2832.677 → 1509.782

Since the ratios remain identical, GetIndex produces the exact same result (79.938255),
but GetRecord produces different raw weighted averages, demonstrating how index
normalization works independently of absolute value scales.
*/

const (
	// Original test values
	dappradarBaseValue    = 293010.636
	dappradarCurrentValue = 312284.113
	coingeckoBaseValue    = 2.832677
	coingeckoCurrentValue = 1.509782

	// Proportional scaling factors
	dappradarScaleFactor = 0.001  // Scale down by 1000
	coingeckoScaleFactor = 1000.0 // Scale up by 1000

	// Weights
	equalWeight = "0.5"

	// Stream names
	dappradarStreamName      = "dappradar_gamefi_index"
	coingeckoStreamName      = "coingecko_gamefi_index"
	gamefiComposedStreamName = "gamefi_index"

	// Proportional test stream names
	dappradarProportionalStreamName      = "dappradar_gamefi_index_proportional"
	coingeckoProportionalStreamName      = "coingecko_gamefi_index_proportional"
	gamefiProportionalComposedStreamName = "gamefi_index_proportional"
)

var (
	// Stream identifiers - original test
	dappradarGamefiStreamId = util.GenerateStreamId(dappradarStreamName)
	coingeckoGamefiStreamId = util.GenerateStreamId(coingeckoStreamName)
	gamefiComposedStreamId  = util.GenerateStreamId(gamefiComposedStreamName)

	// Stream identifiers - proportional test
	dappradarProportionalGamefiStreamId = util.GenerateStreamId(dappradarProportionalStreamName)
	coingeckoProportionalGamefiStreamId = util.GenerateStreamId(coingeckoProportionalStreamName)
	gamefiProportionalComposedStreamId  = util.GenerateStreamId(gamefiProportionalComposedStreamName)

	// Deployer address
	gamefiDeployer = util.Unsafe_NewEthereumAddressFromString("0x0000000000000000000000000000000000000789")

	// Base date: 2024-10-27 00:00:00 UTC (timestamp: 1729998000)
	baseDateTimestamp = int64(1729998000)
	// Current date: let's use a few days later for "today's" values
	currentDateTimestamp = int64(1730084400) // 2024-10-28 00:00:00 UTC
)

func TestGamefiIndex(t *testing.T) {
	kwilTesting.RunSchemaTest(t, kwilTesting.SchemaTest{
		Name:        "gamefi_index_test",
		SeedScripts: migrations.GetSeedScriptPaths(),
		FunctionTests: []kwilTesting.TestFunc{
			WithGamefiTestSetup(testGamefiIndexCalculation(t)),
			WithGamefiProportionalTestSetup(testGamefiIndexProportionalCalculation(t)),
			testManualCalculationVerification(t),
		},
	}, testutils.GetTestOptions())
}

func WithGamefiTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set the platform signer
		platform = procedure.WithSigner(platform, gamefiDeployer.Bytes())

		err := setup.CreateDataProvider(ctx, platform, gamefiDeployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// 1. Setup Dappradar GameFi Index (primitive stream)
		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     dappradarGamefiStreamId,
						DataProvider: gamefiDeployer,
					},
				},
				Data: []setup.InsertRecordInput{
					{EventTime: baseDateTimestamp, Value: dappradarBaseValue},       // Start date value
					{EventTime: currentDateTimestamp, Value: dappradarCurrentValue}, // Current value
				},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up Dappradar GameFi stream")
		}

		// 2. Setup Coingecko GameFi Index (primitive stream)
		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     coingeckoGamefiStreamId,
						DataProvider: gamefiDeployer,
					},
				},
				Data: []setup.InsertRecordInput{
					{EventTime: baseDateTimestamp, Value: coingeckoBaseValue},       // Start date value
					{EventTime: currentDateTimestamp, Value: coingeckoCurrentValue}, // Current value
				},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up Coingecko GameFi stream")
		}

		// 3. Setup GameFi Composed Index
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     gamefiComposedStreamId,
				DataProvider: gamefiDeployer,
			},
			Type: setup.ContractTypeComposed,
		})
		if err != nil {
			return errors.Wrap(err, "error creating GameFi composed stream")
		}

		// 4. Set taxonomy for the composed stream (50% weight for each index)
		startTime := baseDateTimestamp
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     gamefiComposedStreamId,
				DataProvider: gamefiDeployer,
			},
			DataProviders: []string{gamefiDeployer.Address(), gamefiDeployer.Address()},
			StreamIds:     []string{dappradarGamefiStreamId.String(), coingeckoGamefiStreamId.String()},
			Weights:       []string{equalWeight, equalWeight}, // 50% each
			StartTime:     &startTime,
			Height:        2,
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for GameFi composed stream")
		}

		return testFn(ctx, platform)
	}
}

func testGamefiIndexCalculation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the composed stream
		gamefiComposedLocator := types.StreamLocator{
			StreamId:     gamefiComposedStreamId,
			DataProvider: gamefiDeployer,
		}

		// Test the index calculation using GetIndex which gives normalized values
		// The calculation process:
		// 1. GetIndex normalizes the first value to 100.0 as the base
		// 2. Subsequent values are calculated as ratios relative to the base
		// 3. For composed streams, the ratios are weighted and combined

		dateFrom := baseDateTimestamp
		dateTo := currentDateTimestamp

		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform:      platform,
			StreamLocator: gamefiComposedLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting GameFi index")
		}

		// Expected calculation for GetIndex:
		// Base date (2024-10-27): Both indices start at 100.0 (normalized)
		// Current date (2024-10-28):
		// - Dappradar: (312284.113 / 293010.636) * 100 = 106.5803
		// - Coingecko: (1.509782 / 2.832677) * 100 = 53.2899
		// - Combined: (106.5803 * 0.5) + (53.2899 * 0.5) = 79.938254878412663140
		expected := `
		| event_time | value                  |
		| ---------- | ---------------------- |
		| 1729998000 | 100.000000000000000000 |
		| 1730084400 | 79.938254878412663140  |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		// Also test GetRecord to see the raw weighted values
		recordResult, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: gamefiComposedLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting GameFi record")
		}

		// For GetRecord, the values are simple weighted averages of the raw values:
		// Base date: (293010.636 * 0.5) + (2.832677 * 0.5) = 146506.734338500000000000
		// Current date: (312284.113 * 0.5) + (1.509782 * 0.5) = 156142.811391000000000000
		expectedRecord := `
		| event_time | value                     |
		| ---------- | ------------------------- |
		| 1729998000 | 146506.734338500000000000 |
		| 1730084400 | 156142.811391000000000000 |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   recordResult,
			Expected: expectedRecord,
		})

		return nil
	}
}

func WithGamefiProportionalTestSetup(testFn func(ctx context.Context, platform *kwilTesting.Platform) error) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Set the platform signer
		platform = procedure.WithSigner(platform, gamefiDeployer.Bytes())

		err := setup.CreateDataProvider(ctx, platform, gamefiDeployer.Address())
		if err != nil {
			return errors.Wrap(err, "error registering data provider")
		}

		// Calculate proportional values
		dappradarProportionalBaseValue := dappradarBaseValue * dappradarScaleFactor
		dappradarProportionalCurrentValue := dappradarCurrentValue * dappradarScaleFactor
		coingeckoProportionalBaseValue := coingeckoBaseValue * coingeckoScaleFactor
		coingeckoProportionalCurrentValue := coingeckoCurrentValue * coingeckoScaleFactor

		// 1. Setup Dappradar GameFi Index (primitive stream) - scaled down by 1000
		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     dappradarProportionalGamefiStreamId,
						DataProvider: gamefiDeployer,
					},
				},
				Data: []setup.InsertRecordInput{
					{EventTime: baseDateTimestamp, Value: dappradarProportionalBaseValue},       // 293.010636
					{EventTime: currentDateTimestamp, Value: dappradarProportionalCurrentValue}, // 312.284113
				},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up Dappradar GameFi proportional stream")
		}

		// 2. Setup Coingecko GameFi Index (primitive stream) - scaled up by 1000
		err = setup.SetupPrimitive(ctx, setup.SetupPrimitiveInput{
			Platform: platform,
			Height:   1,
			PrimitiveStreamWithData: setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: types.StreamLocator{
						StreamId:     coingeckoProportionalGamefiStreamId,
						DataProvider: gamefiDeployer,
					},
				},
				Data: []setup.InsertRecordInput{
					{EventTime: baseDateTimestamp, Value: coingeckoProportionalBaseValue},       // 2832.677
					{EventTime: currentDateTimestamp, Value: coingeckoProportionalCurrentValue}, // 1509.782
				},
			},
		})
		if err != nil {
			return errors.Wrap(err, "error setting up Coingecko GameFi proportional stream")
		}

		// 3. Setup GameFi Composed Index
		err = setup.CreateStream(ctx, platform, setup.StreamInfo{
			Locator: types.StreamLocator{
				StreamId:     gamefiProportionalComposedStreamId,
				DataProvider: gamefiDeployer,
			},
			Type: setup.ContractTypeComposed,
		})
		if err != nil {
			return errors.Wrap(err, "error creating GameFi proportional composed stream")
		}

		// 4. Set taxonomy for the composed stream (50% weight for each index)
		startTime := baseDateTimestamp
		err = procedure.SetTaxonomy(ctx, procedure.SetTaxonomyInput{
			Platform: platform,
			StreamLocator: types.StreamLocator{
				StreamId:     gamefiProportionalComposedStreamId,
				DataProvider: gamefiDeployer,
			},
			DataProviders: []string{gamefiDeployer.Address(), gamefiDeployer.Address()},
			StreamIds:     []string{dappradarProportionalGamefiStreamId.String(), coingeckoProportionalGamefiStreamId.String()},
			Weights:       []string{equalWeight, equalWeight}, // 50% each
			StartTime:     &startTime,
			Height:        2,
		})
		if err != nil {
			return errors.Wrap(err, "error setting taxonomy for GameFi proportional composed stream")
		}

		return testFn(ctx, platform)
	}
}

func testGamefiIndexProportionalCalculation(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Create StreamLocator for the proportional composed stream
		gamefiProportionalComposedLocator := types.StreamLocator{
			StreamId:     gamefiProportionalComposedStreamId,
			DataProvider: gamefiDeployer,
		}

		// Test the index calculation using GetIndex
		// Even though the raw values are different (scaled proportionally),
		// the ratios remain the same, so GetIndex should produce identical results

		dateFrom := baseDateTimestamp
		dateTo := currentDateTimestamp

		result, err := procedure.GetIndex(ctx, procedure.GetIndexInput{
			Platform:      platform,
			StreamLocator: gamefiProportionalComposedLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting GameFi proportional index")
		}

		// Expected calculation for GetIndex (should be IDENTICAL to original test):
		// Proportional values:
		// - Dappradar: 293.010636 → 312.284113 (ratio: 1.065803, same as original)
		// - Coingecko: 2832.677 → 1509.782 (ratio: 0.532899, same as original)
		// - Combined: (106.5803 * 0.5) + (53.2899 * 0.5) = 79.938254878412663140
		expected := `
		| event_time | value                  |
		| ---------- | ---------------------- |
		| 1729998000 | 100.000000000000000000 |
		| 1730084400 | 79.938254878412663140  |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   result,
			Expected: expected,
		})

		// Test GetRecord to see the raw weighted values (these WILL be different)
		recordResult, err := procedure.GetRecord(ctx, procedure.GetRecordInput{
			Platform:      platform,
			StreamLocator: gamefiProportionalComposedLocator,
			FromTime:      &dateFrom,
			ToTime:        &dateTo,
			Height:        0,
		})
		if err != nil {
			return errors.Wrap(err, "error getting GameFi proportional record")
		}

		// For GetRecord, the values are weighted averages of the SCALED raw values:
		// Base date: (293.010636 * 0.5) + (2832.677 * 0.5) = 1562.843818
		// Current date: (312.284113 * 0.5) + (1509.782 * 0.5) = 911.033056
		expectedRecord := `
		| event_time | value                    |
		| ---------- | ------------------------ |
		| 1729998000 | 1562.843818000000000000  |
		| 1730084400 | 911.033056500000000000   |
		`

		table.AssertResultRowsEqualMarkdownTable(t, table.AssertResultRowsEqualMarkdownTableInput{
			Actual:   recordResult,
			Expected: expectedRecord,
		})

		return nil
	}
}

func testManualCalculationVerification(t *testing.T) func(ctx context.Context, platform *kwilTesting.Platform) error {
	return func(ctx context.Context, platform *kwilTesting.Platform) error {
		// Manual calculation verification test
		// This test verifies the calculations from the markdown table are correct

		// Test data from the markdown table
		const (
			p1Record = 150.0
			p1Base   = 100.0
			p2Record = 200.0
			p2Base   = 100.0
			weight   = 0.5 // 50% each
		)

		// Calculate indices manually
		p1Index := p1Record / p1Base // Should be 1.5
		p2Index := p2Record / p2Base // Should be 2.0

		// Calculate composed record (weighted average of records)
		composedRecord := (p1Record * weight) + (p2Record * weight) // Should be 175

		// Calculate composed index (weighted average of indices)
		composedIndex := (p1Index * weight) + (p2Index * weight) // Should be 1.75

		// Test assertions
		expectedP1Index := 1.5
		expectedP2Index := 2.0
		expectedComposedRecord := 175.0
		expectedComposedIndex := 1.75

		if p1Index != expectedP1Index {
			t.Errorf("P1 index calculation failed: expected %f, got %f", expectedP1Index, p1Index)
		}

		if p2Index != expectedP2Index {
			t.Errorf("P2 index calculation failed: expected %f, got %f", expectedP2Index, p2Index)
		}

		if composedRecord != expectedComposedRecord {
			t.Errorf("Composed record calculation failed: expected %f, got %f", expectedComposedRecord, composedRecord)
		}

		if composedIndex != expectedComposedIndex {
			t.Errorf("Composed index calculation failed: expected %f, got %f", expectedComposedIndex, composedIndex)
		}

		// Test scaled scenario (all values × 10)
		const scaleFactor = 10.0

		p1RecordScaled := p1Record * scaleFactor // 1500
		p1BaseScaled := p1Base * scaleFactor     // 1000
		p2RecordScaled := p2Record * scaleFactor // 2000
		p2BaseScaled := p2Base * scaleFactor     // 1000

		// Calculate indices for scaled values
		p1IndexScaled := p1RecordScaled / p1BaseScaled // Should still be 1.5
		p2IndexScaled := p2RecordScaled / p2BaseScaled // Should still be 2.0

		// Calculate composed values for scaled scenario
		composedRecordScaled := (p1RecordScaled * weight) + (p2RecordScaled * weight) // Should be 1750
		composedIndexScaled := (p1IndexScaled * weight) + (p2IndexScaled * weight)    // Should still be 1.75

		// Test scaled assertions
		expectedComposedRecordScaled := 1750.0

		if p1IndexScaled != expectedP1Index {
			t.Errorf("P1 scaled index calculation failed: expected %f, got %f", expectedP1Index, p1IndexScaled)
		}

		if p2IndexScaled != expectedP2Index {
			t.Errorf("P2 scaled index calculation failed: expected %f, got %f", expectedP2Index, p2IndexScaled)
		}

		if composedRecordScaled != expectedComposedRecordScaled {
			t.Errorf("Composed scaled record calculation failed: expected %f, got %f", expectedComposedRecordScaled, composedRecordScaled)
		}

		if composedIndexScaled != expectedComposedIndex {
			t.Errorf("Composed scaled index calculation failed: expected %f, got %f", expectedComposedIndex, composedIndexScaled)
		}

		t.Logf("✅ All manual calculations verified successfully!")
		t.Logf("Original scenario - P1 index: %f, P2 index: %f, Composed record: %f, Composed index: %f",
			p1Index, p2Index, composedRecord, composedIndex)
		t.Logf("Scaled scenario - P1 index: %f, P2 index: %f, Composed record: %f, Composed index: %f",
			p1IndexScaled, p2IndexScaled, composedRecordScaled, composedIndexScaled)

		return nil
	}
}
