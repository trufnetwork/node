package digest

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/trufnetwork/kwil-db/common"
	kwilTypes "github.com/trufnetwork/kwil-db/core/types"
	kwilTesting "github.com/trufnetwork/kwil-db/testing"
	"github.com/trufnetwork/node/tests/streams/utils/setup"
	"github.com/trufnetwork/sdk-go/core/types"
	"github.com/trufnetwork/sdk-go/core/util"
)

// SetupDataProvider registers a data provider for the benchmark.
// This is the entry point for setting up the testing environment.
func SetupDataProvider(ctx context.Context, platform *kwilTesting.Platform, dataProviderAddr string) error {
	kwilPlatform := platform

	// Use the existing setup pattern from the codebase
	return setup.CreateDataProvider(ctx, kwilPlatform, dataProviderAddr)
}

// GenerateRandomRecords creates random data records for a given day.
// This implements the "random" pattern with uniform random values.
func GenerateRandomRecords(dayStart int64, records int) []InsertRecordInput {
	return NewRandomPattern().GenerateRecords(dayStart, records)
}

// GeneratePatternRecords creates records based on the specified pattern.
// This is a factory method that delegates to specific pattern implementations.
func GeneratePatternRecords(pattern string, dayStart int64, records int) []InsertRecordInput {
	var generator DigestBenchmarkPattern

	switch pattern {
	case "random":
		generator = NewRandomPattern()
	case "dups50":
		generator = NewDups50Pattern()
	case "monotonic":
		generator = NewMonotonicPattern()
	case "equal":
		generator = NewEqualPattern()
	case "time_dup":
		generator = NewTimeDupPattern()
	default:
		// Default to random pattern
		generator = NewRandomPattern()
	}

	return generator.GenerateRecords(dayStart, records)
}

// InsertPrimitiveDataInBatches inserts primitive data in batches for memory efficiency.
// This processes batches sequentially to avoid connection pool exhaustion.
func InsertPrimitiveDataInBatches(ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, records []InsertRecordInput) error {
	if len(records) == 0 {
		return nil
	}

	streamId := locator.StreamId.String()
	totalRecords := len(records)

	// Split records into batches for efficient processing
	batchSize := InsertBatchSize
	batches := lo.Chunk(records, batchSize)
	totalBatches := len(batches)

	// Only log if we have multiple batches to avoid spam
	if totalBatches > 1 {
		fmt.Printf("    Stream (%s): Processing %d records in %d batches\n",
			streamId[:8]+"...", totalRecords, totalBatches)
	}

	streamStart := time.Now()

	// Process batches sequentially to avoid connection pool exhaustion
	for i, batch := range batches {
		batchStart := time.Now()

		if err := insertBatch(ctx, platform, locator, batch, i); err != nil {
			fmt.Printf("    ❌ Stream (%s) batch %d/%d failed after %v: %v\n",
				streamId[:8]+"...", i+1, totalBatches, time.Since(batchStart), err)
			return errors.Wrapf(err, "failed to insert batch %d", i)
		}

		// Only log progress for large batch operations
		if totalBatches > 5 && (i+1)%(totalBatches/5) == 0 {
			fmt.Printf("    Stream (%s): %d/%d batches completed\n",
				streamId[:8]+"...", i+1, totalBatches)
		}
	}

	streamDuration := time.Since(streamStart)
	if totalBatches > 1 {
		fmt.Printf("    ✅ Stream (%s) completed in %v (%d batches, %.0f records/sec)\n",
			streamId[:8]+"...", streamDuration, totalBatches, float64(totalRecords)/streamDuration.Seconds())
	}

	return nil
}

// insertBatch inserts a single batch of records (helper for InsertPrimitiveDataInBatches)
func insertBatch(ctx context.Context, platform *kwilTesting.Platform, locator types.StreamLocator, records []InsertRecordInput, batchIndex int) error {
	kwilPlatform := platform
	streamLocator := locator

	// Convert records to the format expected by the batch insertion function
	var insertInputs []setup.InsertRecordInput
	for _, record := range records {
		insertInputs = append(insertInputs, setup.InsertRecordInput{
			EventTime: record.EventTime,
			Value:     record.Value,
		})
	}

	// Create the primitive stream with data
	primitiveStream := setup.PrimitiveStreamWithData{
		PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
			StreamLocator: streamLocator,
		},
		Data: insertInputs,
	}

	// Use the existing batch insertion function
	input := setup.InsertPrimitiveDataInput{
		Platform:        kwilPlatform,
		Height:          1, // Default height for testing
		PrimitiveStream: primitiveStream,
	}

	return setup.InsertPrimitiveDataBatch(ctx, input)
}

// InsertPendingPruneDays queues days for digest processing by inserting into pending_prune_days table.
func InsertPendingPruneDays(ctx context.Context, platform *kwilTesting.Platform, streamRefs []int, dayIdxs []int) error {
	kwilPlatform := platform

	if len(streamRefs) != len(dayIdxs) {
		return errors.New("streamRefs and dayIdxs must have the same length")
	}

	// Get the deployer for signing via helper
	deployer, err := GetDeployerOrDefault(kwilPlatform)
	if err != nil {
		return errors.Wrap(err, "failed to get deployer")
	}

	// Batch insert all pending days using UNNEST for better performance
	if len(streamRefs) > 0 {
		err = batchInsertPendingDays(ctx, kwilPlatform, streamRefs, dayIdxs, deployer)
		if err != nil {
			return errors.Wrap(err, "error batch inserting pending days")
		}
	}

	return nil
}

// insertPendingDay inserts a single day into the pending_prune_days queue
func insertPendingDay(ctx context.Context, platform *kwilTesting.Platform, streamRef int, dayIndex int64, signer util.EthereumAddress) error {
	// Create transaction context
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       signer.Bytes(),
		Caller:       signer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext: txContext,
	}

	// Execute the insert using the existing pattern
	err := platform.Engine.Execute(engineContext, platform.DB,
		"INSERT INTO pending_prune_days (stream_ref, day_index) VALUES ($stream_ref, $day_index) ON CONFLICT DO NOTHING",
		map[string]any{
			"$stream_ref": streamRef,
			"$day_index":  dayIndex,
		},
		func(row *common.Row) error {
			return nil
		})

	if err != nil {
		return errors.Wrap(err, "error executing pending_prune_days insert")
	}

	return nil
}

// getStreamRefs retrieves the actual stream refs from the database for the most recently created streams.
func getStreamRefs(ctx context.Context, platform *kwilTesting.Platform, expectedCount int) ([]int, error) {
	kwilPlatform := platform

	// Get the deployer for signing queries
	var deployer util.EthereumAddress
	var err error

	// Try to create from bytes first
	deployer, err = util.NewEthereumAddressFromBytes(kwilPlatform.Deployer)
	if err != nil {
		// Use a default test address as fallback
		defaultAddr := make([]byte, 20)
		for i := range defaultAddr {
			defaultAddr[i] = byte(i % 256)
		}
		deployer, err = util.NewEthereumAddressFromBytes(defaultAddr)
		if err != nil {
			return nil, errors.Wrap(err, "error creating default deployer address")
		}
	}

	// Create transaction context for queries
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         kwilPlatform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true, // Override authorization for system operations in tests
	}

	var streamRefs []int

	// Query for the most recently created streams for this data provider
	// Get the actual id column from the streams table
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		`SELECT id FROM streams
		 WHERE data_provider = $data_provider
		 ORDER BY created_at DESC
		 LIMIT $limit`,
		map[string]any{
			"$data_provider": deployer.Address(),
			"$limit":         expectedCount,
		},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if streamId, ok := row.Values[0].(int64); ok {
					streamRefs = append(streamRefs, int(streamId))
				}
			}
			return nil
		})

	if err != nil {
		return nil, errors.Wrap(err, "error querying stream refs")
	}

	if len(streamRefs) != expectedCount {
		return nil, errors.Errorf("expected %d stream refs, got %d", expectedCount, len(streamRefs))
	}

	// Reverse to get them in ascending order (they were ordered DESC in query)
	for i, j := 0, len(streamRefs)-1; i < j; i, j = i+1, j-1 {
		streamRefs[i], streamRefs[j] = streamRefs[j], streamRefs[i]
	}

	return streamRefs, nil
}

// getStreamRefsUpTo retrieves up to maxCount most recently created stream refs for the current data provider.
// Unlike getStreamRefs, this function does not error if fewer than maxCount streams are available.
func getStreamRefsUpTo(ctx context.Context, platform *kwilTesting.Platform, maxCount int) ([]int, error) {
	kwilPlatform := platform

	// Get the deployer for signing queries
	var deployer util.EthereumAddress
	var err error

	// Try to create from bytes first
	deployer, err = util.NewEthereumAddressFromBytes(kwilPlatform.Deployer)
	if err != nil {
		// Fallback: use a default test address
		defaultAddr := make([]byte, 20)
		for i := range defaultAddr {
			defaultAddr[i] = byte(i % 256)
		}
		deployer, err = util.NewEthereumAddressFromBytes(defaultAddr)
		if err != nil {
			return nil, errors.Wrap(err, "error creating default deployer address")
		}
	}

	// Create transaction context for queries
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         kwilPlatform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true, // Override authorization for system operations in tests
	}

	var streamRefs []int

	// Query for the most recently created streams for this data provider
	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		`SELECT id FROM streams
         WHERE data_provider = $data_provider
         ORDER BY created_at DESC
         LIMIT $limit`,
		map[string]any{
			"$data_provider": deployer.Address(),
			"$limit":         maxCount,
		},
		func(row *common.Row) error {
			if len(row.Values) > 0 {
				if streamId, ok := row.Values[0].(int64); ok {
					streamRefs = append(streamRefs, int(streamId))
				}
			}
			return nil
		})

	if err != nil {
		return nil, errors.Wrap(err, "error querying stream refs")
	}

	// Reverse to get them in ascending order (they were ordered DESC in query)
	for i, j := 0, len(streamRefs)-1; i < j; i, j = i+1, j-1 {
		streamRefs[i], streamRefs[j] = streamRefs[j], streamRefs[i]
	}

	return streamRefs, nil
}

// batchInsertPendingDays efficiently inserts multiple pending days using PostgreSQL UNNEST.
// This replaces individual inserts with a single batch operation for better performance.
func batchInsertPendingDays(ctx context.Context, platform *kwilTesting.Platform, streamRefs []int, dayIdxs []int, signer util.EthereumAddress) error {
	// Create transaction context
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       signer.Bytes(),
		Caller:       signer.Address(),
		TxID:         platform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true, // Override authorization for system operations in tests
	}

	// Convert slices to the format expected by the database
	// PostgreSQL UNNEST expects arrays
	streamRefArray := make([]int64, len(streamRefs))
	dayIndexArray := make([]int64, len(dayIdxs))

	for i := 0; i < len(streamRefs); i++ {
		streamRefArray[i] = int64(streamRefs[i])
		dayIndexArray[i] = int64(dayIdxs[i])
	}

	// Execute the batch insert using UNNEST
	err := platform.Engine.Execute(engineContext, platform.DB,
		`INSERT INTO pending_prune_days (stream_ref, day_index)
		 SELECT * FROM UNNEST($stream_refs, $day_indices) AS t(stream_ref, day_index)
		 ON CONFLICT (stream_ref, day_index) DO NOTHING`,
		map[string]any{
			"$stream_refs": streamRefArray,
			"$day_indices": dayIndexArray,
		},
		func(row *common.Row) error {
			return nil
		})

	if err != nil {
		return errors.Wrap(err, "error executing batch pending_prune_days insert")
	}

	return nil
}

// ClearPendingPruneDays removes all entries from pending_prune_days.
// Useful to prepare a clean slate when reusing a global dataset across cases.
func ClearPendingPruneDays(ctx context.Context, platform *kwilTesting.Platform) error {
	kwilPlatform := platform

	// Get the deployer for signing queries
	deployer, err := GetDeployerOrDefault(kwilPlatform)
	if err != nil {
		return errors.Wrap(err, "failed to get deployer")
	}

	// Create transaction context for queries
	txContext := &common.TxContext{
		Ctx:          ctx,
		BlockContext: &common.BlockContext{Height: 1},
		Signer:       deployer.Bytes(),
		Caller:       deployer.Address(),
		TxID:         kwilPlatform.Txid(),
	}

	engineContext := &common.EngineContext{
		TxContext:     txContext,
		OverrideAuthz: true,
	}

	err = kwilPlatform.Engine.Execute(engineContext, kwilPlatform.DB,
		`DELETE FROM pending_prune_days`,
		map[string]any{},
		func(row *common.Row) error { return nil },
	)
	if err != nil {
		return errors.Wrap(err, "error clearing pending_prune_days")
	}
	return nil
}

// AnalyzeDigestTables performs query planner optimization for digest-related tables.
// This reuses the existing updateQueryPlanner utility.
func AnalyzeDigestTables(ctx context.Context, platform *kwilTesting.Platform) error {
	kwilPlatform := platform

	// Define the digest-related tables that need query planner optimization
	digestTables := []string{
		"primitive_events",
		"primitive_event_type",
		"pending_prune_days",
		"streams", // Also include streams for foreign key relationships
	}

	return analyzeTables(ctx, kwilPlatform, digestTables)
}

// analyzeTables performs ANALYZE on the specified tables for query optimization
func analyzeTables(ctx context.Context, platform *kwilTesting.Platform, tables []string) error {
	// Use the improved AnalyzeTables function from helpers
	return AnalyzeTables(ctx, platform, tables)
}

// RandomPattern implements the "random" data generation pattern.
// Generates uniform random values across each day.
type RandomPattern struct{}

// NewRandomPattern creates a new random pattern generator.
func NewRandomPattern() *RandomPattern {
	return &RandomPattern{}
}

// GenerateRecords generates random records for the random pattern.
func (r *RandomPattern) GenerateRecords(dayStart int64, records int) []InsertRecordInput {
	result := make([]InsertRecordInput, records)

	for i := 0; i < records; i++ {
		// Generate random event time within the day
		eventTime := dayStart + rand.Int63n(DaySeconds)

		// Generate random value between 0 and 1000
		value := rand.Float64() * 1000

		// Generate random created_at for tie-breaking (within reasonable range)
		createdAt := time.Now().Unix() + rand.Int63n(HourSeconds)

		result[i] = InsertRecordInput{
			EventTime: eventTime,
			Value:     value,
			CreatedAt: createdAt,
		}
	}

	return result
}

// GetPatternName returns the pattern name identifier.
func (r *RandomPattern) GetPatternName() string {
	return "random"
}

// Dups50Pattern implements the "dups50" data generation pattern.
// Generates data with 50% duplicate values to stress high/low selection.
type Dups50Pattern struct{}

// NewDups50Pattern creates a new dups50 pattern generator.
func NewDups50Pattern() *Dups50Pattern {
	return &Dups50Pattern{}
}

// GenerateRecords generates records with 50% duplicate values.
func (d *Dups50Pattern) GenerateRecords(dayStart int64, records int) []InsertRecordInput {
	result := make([]InsertRecordInput, records)

	// Pre-generate unique values for duplicates
	uniqueValues := make([]float64, records/2)
	for i := range uniqueValues {
		uniqueValues[i] = rand.Float64() * 1000
	}

	for i := 0; i < records; i++ {
		eventTime := dayStart + rand.Int63n(DaySeconds)
		createdAt := time.Now().Unix() + rand.Int63n(HourSeconds)

		var value float64
		if i%2 == 0 {
			// Use unique value
			value = uniqueValues[i/2]
		} else {
			// Use duplicate of previous unique value
			value = uniqueValues[(i-1)/2]
		}

		result[i] = InsertRecordInput{
			EventTime: eventTime,
			Value:     value,
			CreatedAt: createdAt,
		}
	}

	return result
}

// GetPatternName returns the pattern name identifier.
func (d *Dups50Pattern) GetPatternName() string {
	return "dups50"
}

// MonotonicPattern implements the "monotonic" data generation pattern.
// Generates strictly increasing or decreasing values.
type MonotonicPattern struct {
	increasing bool
}

// NewMonotonicPattern creates a new monotonic pattern generator.
func NewMonotonicPattern() *MonotonicPattern {
	return &MonotonicPattern{increasing: rand.Intn(2) == 0}
}

// GenerateRecords generates monotonic records.
func (m *MonotonicPattern) GenerateRecords(dayStart int64, records int) []InsertRecordInput {
	result := make([]InsertRecordInput, records)
	baseValue := rand.Float64() * 100

	for i := 0; i < records; i++ {
		eventTime := dayStart + int64(i)*DaySeconds/int64(records) // Spread across day
		createdAt := time.Now().Unix() + rand.Int63n(HourSeconds)

		var value float64
		if m.increasing {
			value = baseValue + float64(i)*10 // Increasing
		} else {
			value = baseValue + float64(records-i)*10 // Decreasing
		}

		result[i] = InsertRecordInput{
			EventTime: eventTime,
			Value:     value,
			CreatedAt: createdAt,
		}
	}

	return result
}

// GetPatternName returns the pattern name identifier.
func (m *MonotonicPattern) GetPatternName() string {
	return "monotonic"
}

// EqualPattern implements the "equal" data generation pattern.
// Generates all values identical to test combined flags.
type EqualPattern struct{}

// NewEqualPattern creates a new equal pattern generator.
func NewEqualPattern() *EqualPattern {
	return &EqualPattern{}
}

// GenerateRecords generates records with identical values.
func (e *EqualPattern) GenerateRecords(dayStart int64, records int) []InsertRecordInput {
	result := make([]InsertRecordInput, records)
	value := rand.Float64() * 1000 // Same value for all records

	for i := 0; i < records; i++ {
		eventTime := dayStart + int64(i)*DaySeconds/int64(records) // Spread across day
		createdAt := time.Now().Unix() + rand.Int63n(HourSeconds)

		result[i] = InsertRecordInput{
			EventTime: eventTime,
			Value:     value,
			CreatedAt: createdAt,
		}
	}

	return result
}

// GetPatternName returns the pattern name identifier.
func (e *EqualPattern) GetPatternName() string {
	return "equal"
}

// TimeDupPattern implements the "time_dup" data generation pattern.
// Generates records with same event_time but different created_at for tie-break testing.
type TimeDupPattern struct{}

// NewTimeDupPattern creates a new time_dup pattern generator.
func NewTimeDupPattern() *TimeDupPattern {
	return &TimeDupPattern{}
}

// GenerateRecords generates records with duplicate timestamps but different created_at.
func (t *TimeDupPattern) GenerateRecords(dayStart int64, records int) []InsertRecordInput {
	result := make([]InsertRecordInput, records)

	// Generate unique event times
	uniqueTimes := records / 2
	if uniqueTimes == 0 {
		uniqueTimes = 1
	}

	for i := 0; i < records; i++ {
		// Each event time will be used twice with different created_at
		eventTime := dayStart + int64(i%uniqueTimes)*DaySeconds/int64(uniqueTimes)
		value := rand.Float64() * 1000

		// Vary created_at to test tie-breaking
		var createdAt int64
		if i%2 == 0 {
			createdAt = time.Now().Unix() + 1000 // Earlier
		} else {
			createdAt = time.Now().Unix() + 2000 // Later
		}

		result[i] = InsertRecordInput{
			EventTime: eventTime,
			Value:     value,
			CreatedAt: createdAt,
		}
	}

	return result
}

// GetPatternName returns the pattern name identifier.
func (t *TimeDupPattern) GetPatternName() string {
	return "time_dup"
}

// InsertDataForMultipleStreams efficiently inserts data for multiple streams simultaneously.
// This processes all streams at once since primitives are already batched internally.
func InsertDataForMultipleStreams(ctx context.Context, platform *kwilTesting.Platform, streamLocators []types.StreamLocator, daysPerStream, recordsPerDay int, pattern string) error {
	if len(streamLocators) == 0 {
		return nil
	}

	totalRecords := len(streamLocators) * daysPerStream * recordsPerDay
	fmt.Printf("  📊 Inserting %d total records across %d streams (%d days × %d records/day each)\n",
		totalRecords, len(streamLocators), daysPerStream, recordsPerDay)

	start := time.Now()

	streamData := make([]struct {
		Locator types.StreamLocator
		Records []InsertRecordInput
	}, len(streamLocators))

	// Generate data for all streams in parallel
	for i, stream := range streamLocators {
		var allRecords []InsertRecordInput
		for day := 0; day < daysPerStream; day++ {
			dayStart := int64(day * DaySeconds)
			records := GeneratePatternRecords(pattern, dayStart, recordsPerDay)
			allRecords = append(allRecords, records...)
		}
		streamData[i] = struct {
			Locator types.StreamLocator
			Records []InsertRecordInput
		}{
			Locator: stream,
			Records: allRecords,
		}
	}

	// Insert data for all streams simultaneously
	if err := insertBatchForMultipleStreams(ctx, platform, streamData); err != nil {
		return errors.Wrap(err, "failed to insert data for multiple streams")
	}

	totalDuration := time.Since(start)
	fmt.Printf("  ✅ Data insertion completed in %v (%.0f records/sec)\n",
		totalDuration, float64(totalRecords)/totalDuration.Seconds())

	return nil
}

// insertBatchForMultipleStreams inserts data for multiple streams in a single batch operation.
func insertBatchForMultipleStreams(ctx context.Context, platform *kwilTesting.Platform, streamData []struct {
	Locator types.StreamLocator
	Records []InsertRecordInput
}) error {
	totalStreams := len(streamData)
	totalBatches := 0
	totalRecords := 0

	fmt.Printf("  Preparing batches for %d streams...\n", totalStreams)

	// Create primitive streams with data
	var streams []setup.PrimitiveStreamWithData

	// Progress logging interval - log every 10% or every 1000 streams (whichever is larger)
	logInterval := totalStreams / 10 // 10% progress
	if logInterval < 1000 {
		logInterval = 1000 // Minimum 1000 streams between logs
	}
	if totalStreams < 100 {
		logInterval = totalStreams / 10
		if logInterval == 0 {
			logInterval = 1
		}
	}

	for streamIdx, data := range streamData {
		streamRecords := len(data.Records)
		totalRecords += streamRecords

		// Split records into batches for each stream
		recordBatches := lo.Chunk(data.Records, InsertBatchSize)
		streamBatches := len(recordBatches)
		totalBatches += streamBatches

		// Convert all batches for this stream
		for _, batch := range recordBatches {
			// Convert records to setup format
			var records []setup.InsertRecordInput
			for _, record := range batch {
				records = append(records, setup.InsertRecordInput{
					EventTime: record.EventTime,
					Value:     record.Value,
				})
			}

			// Create primitive stream with data
			primitiveStream := setup.PrimitiveStreamWithData{
				PrimitiveStreamDefinition: setup.PrimitiveStreamDefinition{
					StreamLocator: data.Locator,
				},
				Data: records,
			}

			streams = append(streams, primitiveStream)
		}

		// Progress logging - only log every Nth stream
		if (streamIdx+1)%logInterval == 0 || streamIdx == totalStreams-1 {
			fmt.Printf("    Progress: %d/%d streams processed (%d batches, %d records so far)\n",
				streamIdx+1, totalStreams, totalBatches, totalRecords)
		}
	}

	fmt.Printf("  ✅ Batch preparation complete: %d total primitive batches, %d total records\n",
		totalBatches, totalRecords)

	// Execute all batches in a single multi-stream operation with progress logging
	fmt.Printf("  📦 Executing database insertion (%d primitive batches, %d records)...\n",
		len(streams), totalRecords)
	batchStart := time.Now()

	multiInput := setup.InsertMultiPrimitiveDataInput{
		Platform: platform,
		Height:   1,
		Streams:  streams,
	}

	err := insertMultiPrimitiveDataWithLogging(ctx, multiInput)
	batchDuration := time.Since(batchStart)

	if err != nil {
		fmt.Printf("  ❌ Database insertion failed after %v: %v\n", batchDuration, err)
		return err
	}

	fmt.Printf("  ✅ Database insertion completed in %v (%.0f records/sec)\n",
		batchDuration, float64(totalRecords)/batchDuration.Seconds())
	return nil
}

// insertMultiPrimitiveDataWithLogging inserts data in batches of InsertBatchSize, not by provider
func insertMultiPrimitiveDataWithLogging(ctx context.Context, input setup.InsertMultiPrimitiveDataInput) error {
	if len(input.Streams) == 0 {
		return nil
	}

	// Collect all records with their stream information
	type recordWithStream struct {
		streamId  string
		provider  string
		eventTime int64
		value     float64
	}

	var allRecords []recordWithStream
	totalRecords := 0

	for _, ps := range input.Streams {
		provider := ps.StreamLocator.DataProvider.Address()
		streamId := ps.StreamLocator.StreamId.String()

		for _, rec := range ps.Data {
			allRecords = append(allRecords, recordWithStream{
				streamId:  streamId,
				provider:  provider,
				eventTime: rec.EventTime,
				value:     rec.Value,
			})
		}
		totalRecords += len(ps.Data)
	}

	fmt.Printf("    🔍 Processing %d records in batches of %d...\n", totalRecords, InsertBatchSize)

	// Process records in batches of InsertBatchSize
	batches := lo.Chunk(allRecords, InsertBatchSize)
	totalBatches := len(batches)
	processedRecords := 0

	fmt.Printf("    📦 Created %d batches from %d records\n", totalBatches, totalRecords)

	for i, batch := range batches {
		batchStart := time.Now()

		// Group this batch by provider (each batch might span multiple providers)
		batchProviderGroups := make(map[string]*struct {
			dataProviders []string
			streamIds     []string
			eventTimes    []int64
			values        []*kwilTypes.Decimal
		})

		for _, rec := range batch {
			g, ok := batchProviderGroups[rec.provider]
			if !ok {
				g = &struct {
					dataProviders []string
					streamIds     []string
					eventTimes    []int64
					values        []*kwilTypes.Decimal
				}{}
				batchProviderGroups[rec.provider] = g
			}

			dec, err := kwilTypes.ParseDecimalExplicit(strconv.FormatFloat(rec.value, 'f', -1, 64), 36, 18)
			if err != nil {
				return errors.Wrap(err, "parse decimal in insertMultiPrimitiveDataWithLogging")
			}

			g.dataProviders = append(g.dataProviders, rec.provider)
			g.streamIds = append(g.streamIds, rec.streamId)
			g.eventTimes = append(g.eventTimes, rec.eventTime)
			g.values = append(g.values, dec)
		}

		fmt.Printf("      📤 Batch %d/%d: Processing %d records from %d providers...\n",
			i+1, totalBatches, len(batch), len(batchProviderGroups))

		// Insert this batch using direct database calls
		txid := input.Platform.Txid()

		for provider, g := range batchProviderGroups {
			signerAddr := util.Unsafe_NewEthereumAddressFromString(provider)

			txContext := &common.TxContext{
				Ctx: ctx,
				BlockContext: &common.BlockContext{
					Height: input.Height,
				},
				TxID:   txid,
				Signer: signerAddr.Bytes(),
				Caller: signerAddr.Address(),
			}
			engineContext := &common.EngineContext{TxContext: txContext}

			args := []any{g.dataProviders, g.streamIds, g.eventTimes, g.values}
			r, err := input.Platform.Engine.Call(engineContext, input.Platform.DB, "", "insert_records", args, func(row *common.Row) error { return nil })
			if err != nil {
				fmt.Printf("      ❌ Batch %d/%d provider %s failed: %v\n", i+1, totalBatches, provider[:12], err)
				return errors.Wrapf(err, "insert_records call failed for provider %s", provider)
			}
			if r.Error != nil {
				fmt.Printf("      ❌ Batch %d/%d provider %s result failed: %v\n", i+1, totalBatches, provider[:12], r.Error)
				return errors.Wrapf(r.Error, "insert_records result failed for provider %s", provider)
			}
		}

		batchDuration := time.Since(batchStart)
		processedRecords += len(batch)

		fmt.Printf("      ✅ Batch %d/%d completed in %v (%d/%d total records, %.0f records/sec)\n",
			i+1, totalBatches, batchDuration, processedRecords, totalRecords,
			float64(len(batch))/batchDuration.Seconds())
	}

	fmt.Printf("    🎉 All %d batches processed successfully\n", totalBatches)
	return nil
}

// SetupBenchmarkData is the main setup function that orchestrates the entire data setup process.
// This function follows the Single Responsibility Principle by delegating to specialized functions.
func SetupBenchmarkData(ctx context.Context, input DigestSetupInput) error {
	totalStart := time.Now()
	defer func() {
		fmt.Printf("Total SetupBenchmarkData completed in %v\n", time.Since(totalStart))
	}()

	// 1. Setup data provider
	var dataProviderAddr string
	if input.DataProvider != nil {
		if addr, ok := input.DataProvider.(util.EthereumAddress); ok {
			dataProviderAddr = addr.Address()
		} else {
			// Fallback: use a default test address
			dataProviderAddr = "0x1234567890123456789012345678901234567890"
		}
	} else {
		// Use default test address if no data provider specified
		dataProviderAddr = "0x1234567890123456789012345678901234567890"
	}

	platform, ok := input.Platform.(*kwilTesting.Platform)
	if !ok {
		return errors.New("invalid platform type")
	}

	fmt.Printf("Step 1: Setting up data provider...\n")
	setupStart := time.Now()
	if err := SetupDataProvider(ctx, platform, dataProviderAddr); err != nil {
		return errors.Wrap(err, "failed to setup data provider")
	}
	fmt.Printf("Step 1 completed in %v\n", time.Since(setupStart))

	// 2. Create primitive streams (using batch creation for efficiency)
	fmt.Printf("Step 2: Creating %d primitive streams...\n", input.Case.Streams)
	streamStart := time.Now()

	// Get the deployer for signing
	deployer, err := GetDeployerOrDefault(platform)
	if err != nil {
		return errors.Wrap(err, "failed to get deployer")
	}

	var streamInfos []setup.StreamInfo
	var streamLocators []types.StreamLocator

	// Create stream locators with unique IDs per test run
	// Use timestamp and random component to avoid conflicts between test runs
	timestamp := time.Now().Unix()
	randomSuffix := rand.Intn(10000)
	for i := 0; i < input.Case.Streams; i++ {
		streamId := util.GenerateStreamId(fmt.Sprintf("digest_test_stream_%d_%d_%d", timestamp, randomSuffix, i))

		streamLocator := types.StreamLocator{
			DataProvider: deployer,
			StreamId:     streamId,
		}

		streamInfos = append(streamInfos, setup.StreamInfo{
			Locator: streamLocator,
			Type:    setup.ContractTypePrimitive,
		})

		streamLocators = append(streamLocators, streamLocator)
	}

	// Create all streams in a single batch operation
	err = setup.CreateStreams(ctx, platform, streamInfos)
	if err != nil {
		return errors.Wrap(err, "error creating primitive streams")
	}

	fmt.Printf("Step 2 completed in %v (%d streams created)\n", time.Since(streamStart), len(streamLocators))

	// 3. Generate and insert data for all streams in parallel batches
	fmt.Printf("Step 3: Generating and inserting data for %d streams (parallel batch processing)...\n", len(streamLocators))
	dataStart := time.Now()

	// Use batch insertion for multiple streams simultaneously
	if err := InsertDataForMultipleStreams(ctx, platform, streamLocators, input.Case.DaysPerStream, input.Case.RecordsPerDay, input.Case.Pattern); err != nil {
		return errors.Wrap(err, "failed to insert data for multiple streams")
	}

	totalRecords := len(streamLocators) * input.Case.DaysPerStream * input.Case.RecordsPerDay

	fmt.Printf("Step 3 completed in %v (%d total records)\n", time.Since(dataStart), totalRecords)

	// 4. Setup pending prune days
	fmt.Printf("Step 4: Setting up pending prune days...\n")
	pruneStart := time.Now()

	// Get actual stream refs from the database instead of assuming they start at 1
	actualStreamRefs, err := getStreamRefs(ctx, platform, len(streamLocators))
	if err != nil {
		return errors.Wrap(err, "failed to get actual stream refs")
	}

	// Create pairs for every (stream, day) combination
	totalCandidates := len(actualStreamRefs) * input.Case.DaysPerStream
	streamRefs := make([]int, totalCandidates)
	dayIdxs := make([]int, totalCandidates)

	// Fill stream refs and day indices for all combinations
	idx := 0
	for _, streamRef := range actualStreamRefs {
		for day := 0; day < input.Case.DaysPerStream; day++ {
			streamRefs[idx] = streamRef
			dayIdxs[idx] = day
			idx++
		}
	}

	fmt.Printf("  Inserting %d pending prune day entries...\n", totalCandidates)
	if err := InsertPendingPruneDays(ctx, platform, streamRefs, dayIdxs); err != nil {
		return errors.Wrap(err, "failed to insert pending prune days")
	}

	fmt.Printf("Step 4 completed in %v\n", time.Since(pruneStart))

	// 5. Analyze tables for query optimization (optional)
	fmt.Printf("Step 5: Analyzing tables for query optimization...\n")
	analyzeStart := time.Now()
	if err := AnalyzeDigestTables(ctx, platform); err != nil {
		fmt.Printf("Step 5 failed (non-critical): %v\n", err)
	} else {
		fmt.Printf("Step 5 completed in %v\n", time.Since(analyzeStart))
	}

	return nil
}

// ValidateBenchmarkCase validates the benchmark case configuration for correctness.
func ValidateBenchmarkCase(c DigestBenchmarkCase) error {
	if c.Streams <= 0 {
		return errors.New("streams must be positive")
	}
	if c.DaysPerStream <= 0 {
		return errors.New("days_per_stream must be positive")
	}
	if c.RecordsPerDay <= 0 {
		return errors.New("records_per_day must be positive")
	}

	if c.DeleteCap <= 0 {
		return errors.New("delete_cap must be positive")
	}

	if c.Samples <= 0 {
		return errors.New("samples must be positive")
	}

	validPatterns := map[string]bool{
		"random":    true,
		"dups50":    true,
		"monotonic": true,
		"equal":     true,
		"time_dup":  true,
	}

	if !validPatterns[c.Pattern] {
		return fmt.Errorf("invalid pattern: %s", c.Pattern)
	}

	return nil
}

// CalculateExpectedCandidates calculates the expected number of candidates for a benchmark case.
func CalculateExpectedCandidates(c DigestBenchmarkCase) int {
	return c.Streams * c.DaysPerStream
}

// CalculateExpectedRecords calculates the expected total records for a benchmark case.
func CalculateExpectedRecords(c DigestBenchmarkCase) int {
	return c.Streams * c.DaysPerStream * c.RecordsPerDay
}

// EstimateMemoryUsage estimates the memory usage for a benchmark case.
func EstimateMemoryUsage(c DigestBenchmarkCase) int64 {
	// Rough estimation: each record ~100 bytes + overhead
	bytesPerRecord := int64(100)
	totalRecords := int64(CalculateExpectedRecords(c))

	// Add overhead for processing and temporary data structures
	overheadFactor := float64(1.5)

	return int64(float64(totalRecords*bytesPerRecord) * overheadFactor)
}

// AnalyzeDataPatterns analyzes the statistical properties of generated data patterns.
// This uses the stats library for robust statistical analysis.
func AnalyzeDataPatterns(records []InsertRecordInput) (map[string]float64, error) {
	if len(records) == 0 {
		return nil, errors.New("no records to analyze")
	}

	// Extract values for statistical analysis
	values := lo.Map(records, func(r InsertRecordInput, _ int) float64 { return r.Value })

	// Calculate comprehensive statistics using montanaflynn/stats
	analysis := make(map[string]float64)

	// Basic statistics
	if mean, err := stats.Mean(values); err == nil {
		analysis["mean"] = mean
	}
	if median, err := stats.Median(values); err == nil {
		analysis["median"] = median
	}
	if stdDev, err := stats.StandardDeviation(values); err == nil {
		analysis["std_dev"] = stdDev
	}
	if min, err := stats.Min(values); err == nil {
		analysis["min"] = min
	}
	if max, err := stats.Max(values); err == nil {
		analysis["max"] = max
	}

	// Percentiles for performance analysis
	if p95, err := stats.Percentile(values, 95); err == nil {
		analysis["p95"] = p95
	}
	if p99, err := stats.Percentile(values, 99); err == nil {
		analysis["p99"] = p99
	}

	// Distribution analysis
	// Note: Skewness and Kurtosis not available in montanaflynn/stats package

	// Uniqueness analysis (important for OHLC testing)
	uniqueValues := lo.Uniq(values)
	analysis["unique_ratio"] = float64(len(uniqueValues)) / float64(len(values))

	return analysis, nil
}
