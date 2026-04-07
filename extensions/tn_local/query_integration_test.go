package tn_local

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/trufnetwork/kwil-db/core/log"
)

// These integration tests validate the actual SQL queries run against a real PostgreSQL.
// They require TEST_PG_DATABASE to be explicitly set to opt in (prevents accidental runs
// against production databases). Skip if unavailable.
//
// Run with:
//   TEST_PG_DATABASE=kwild go test -v ./extensions/tn_local/ -run TestIntegration -count=1
//
// Configure via environment variables:
//   TEST_PG_DATABASE (required — must be set to run)
//   TEST_PG_HOST (default: localhost)
//   TEST_PG_PORT (default: 5432)
//   TEST_PG_USER (default: kwild)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func setupIntegrationDB(t *testing.T) *Extension {
	t.Helper()

	// Require explicit env config to avoid accidentally running against production.
	// At least TEST_PG_DATABASE must be set to opt in.
	dbName := os.Getenv("TEST_PG_DATABASE")
	if dbName == "" {
		t.Skip("skipping integration test: TEST_PG_DATABASE not set (set it to opt in)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	host := envOrDefault("TEST_PG_HOST", "localhost")
	port := envOrDefault("TEST_PG_PORT", "5432")
	user := envOrDefault("TEST_PG_USER", "kwild")

	connStr := fmt.Sprintf("host=%s port=%s user=%s database=%s sslmode=disable", host, port, user, dbName)
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Skipf("skipping integration test: cannot connect to PostgreSQL: %v", err)
	}

	// Ping to verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping integration test: PostgreSQL ping failed: %v", err)
	}

	// Clean up any previous test schema
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+SchemaName+" CASCADE")

	wrapper := NewPoolDBWrapper(pool)
	logger := log.New(log.WithLevel(log.LevelInfo))
	localDB := NewLocalDB(wrapper, logger)

	if err := localDB.SetupSchema(ctx); err != nil {
		pool.Close()
		t.Fatalf("failed to setup schema: %v", err)
	}

	ext := &Extension{
		logger: logger,
		db:     wrapper,
	}
	ext.isEnabled.Store(true)

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+SchemaName+" CASCADE")
		pool.Close()
	})

	return ext
}

func TestIntegration_PrimitiveGetRecord(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	// Create a primitive stream
	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		StreamType:   "primitive",
	})
	require.Nil(t, rpcErr, "create stream failed: %v", rpcErr)

	// Insert records
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		DataProvider: []string{testDP, testDP, testDP, testDP, testDP},
		StreamID:     []string{testSID, testSID, testSID, testSID, testSID},
		EventTime:    []int64{1000, 2000, 3000, 4000, 5000},
		Value:        []string{"10.5", "20.0", "30.75", "40.0", "50.25"},
	})
	require.Nil(t, rpcErr, "insert records failed: %v", rpcErr)

	// Test 1: Latest record (both nil)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     testSID,
	})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1, "latest record should return 1 row")
	require.Equal(t, int64(5000), resp.Records[0].EventTime)

	// Test 2: Time range query (from=1500, to=4000)
	// Anchor: last event at or before 1500 → 1000
	// Interval: event_time > 1500 AND event_time <= 4000 → 2000, 3000, 4000
	from := int64(1500)
	to := int64(4000)
	resp, rpcErr = ext.GetRecord(ctx, &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
	})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 4, "should have anchor + 3 interval records")
	expectedTimes := []int64{1000, 2000, 3000, 4000}
	for i, et := range expectedTimes {
		require.Equal(t, et, resp.Records[i].EventTime, "record %d event_time mismatch", i)
	}

	// Test 3: Range beyond data (from=9000, to=9999) — only anchor at 5000
	from = int64(9000)
	to = int64(9999)
	resp, rpcErr = ext.GetRecord(ctx, &GetRecordRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
	})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1, "should have exactly 1 anchor record")
	require.Equal(t, int64(5000), resp.Records[0].EventTime)
}

func TestIntegration_PrimitiveGetIndex(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		StreamType:   "primitive",
	})
	require.Nil(t, rpcErr)

	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		DataProvider: []string{testDP, testDP, testDP},
		StreamID:     []string{testSID, testSID, testSID},
		EventTime:    []int64{1000, 2000, 3000},
		Value:        []string{"10.0", "20.0", "30.0"},
	})
	require.Nil(t, rpcErr)

	// Test 1: Default base_time (first event = 1000, base = 10.0)
	// Index = (value / base) * 100 → 100, 200, 300
	from := int64(500)
	to := int64(5000)
	resp, rpcErr := ext.GetIndex(ctx, &GetIndexRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
	})
	require.Nil(t, rpcErr, "get_index failed: %v", rpcErr)
	require.Len(t, resp.Records, 3)
	require.Equal(t, int64(1000), resp.Records[0].EventTime)
	require.Equal(t, "100.000000000000000000", resp.Records[0].Value)
	require.Equal(t, int64(2000), resp.Records[1].EventTime)
	require.Equal(t, "200.000000000000000000", resp.Records[1].Value)
	require.Equal(t, int64(3000), resp.Records[2].EventTime)
	require.Equal(t, "300.000000000000000000", resp.Records[2].Value)

	// Test 2: Explicit base_time = 2000 (base value = 20.0)
	// Index = (value / 20) * 100 → 50, 100, 150
	baseTime := int64(2000)
	resp, rpcErr = ext.GetIndex(ctx, &GetIndexRequest{
		DataProvider: testDP,
		StreamID:     testSID,
		FromTime:     &from,
		ToTime:       &to,
		BaseTime:     &baseTime,
	})
	require.Nil(t, rpcErr, "get_index with base_time failed: %v", rpcErr)
	require.Len(t, resp.Records, 3)
	require.Equal(t, int64(1000), resp.Records[0].EventTime)
	require.Equal(t, "50.000000000000000000", resp.Records[0].Value)
	require.Equal(t, int64(2000), resp.Records[1].EventTime)
	require.Equal(t, "100.000000000000000000", resp.Records[1].Value)
	require.Equal(t, int64(3000), resp.Records[2].EventTime)
	require.Equal(t, "150.000000000000000000", resp.Records[2].Value)
}

func TestIntegration_ListStreams(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	// Initially empty
	resp, rpcErr := ext.ListStreams(ctx, &ListStreamsRequest{})
	require.Nil(t, rpcErr)
	require.Empty(t, resp.Streams)

	// Create some streams
	streams := []struct {
		dp, sid, stype string
	}{
		{testDP, "st00000000000000000000000000aaaa", "primitive"},
		{testDP, "st00000000000000000000000000bbbb", "composed"},
		{testDP, "st00000000000000000000000000cccc", "primitive"},
	}
	for _, s := range streams {
		_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
			DataProvider: s.dp, StreamID: s.sid, StreamType: s.stype,
		})
		require.Nil(t, rpcErr, "create stream %s failed", s.sid)
	}

	resp, rpcErr = ext.ListStreams(ctx, &ListStreamsRequest{})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Streams, 3)

	// Verify all stream types present
	types := make(map[string]int)
	for _, s := range resp.Streams {
		types[s.StreamType]++
	}
	require.Equal(t, 2, types["primitive"])
	require.Equal(t, 1, types["composed"])
}

func TestIntegration_GapFilling(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		DataProvider: testDP, StreamID: testSID, StreamType: "primitive",
	})
	require.Nil(t, rpcErr)

	// Insert sparse data: values at 1000, 3000, 5000
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		DataProvider: []string{testDP, testDP, testDP},
		StreamID:     []string{testSID, testSID, testSID},
		EventTime:    []int64{1000, 3000, 5000},
		Value:        []string{"10.0", "30.0", "50.0"},
	})
	require.Nil(t, rpcErr)

	// Query range that starts between data points
	// from=2000, to=4000 → should get anchor at 1000, then 3000
	from := int64(2000)
	to := int64(4000)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		DataProvider: testDP, StreamID: testSID,
		FromTime: &from, ToTime: &to,
	})
	require.Nil(t, rpcErr)
	require.NotEmpty(t, resp.Records)

	// First record should be the anchor (event_time=1000, carried forward)
	t.Logf("Gap-filling results:")
	for _, r := range resp.Records {
		t.Logf("  event_time=%d value=%s", r.EventTime, r.Value)
	}
	require.Equal(t, int64(1000), resp.Records[0].EventTime, "first record should be anchor")
	if len(resp.Records) > 1 {
		require.Equal(t, int64(3000), resp.Records[1].EventTime, "second record should be 3000")
	}
}

func TestIntegration_HeightAsCreatedAt(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	// Simulate block height updates
	ext.height.Store(100)

	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		DataProvider: testDP, StreamID: testSID, StreamType: "primitive",
	})
	require.Nil(t, rpcErr)

	// Insert at height 100
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		DataProvider: []string{testDP},
		StreamID:     []string{testSID},
		EventTime:    []int64{1000},
		Value:        []string{"10.0"},
	})
	require.Nil(t, rpcErr)

	// Advance height and insert updated value for same event_time
	ext.height.Store(200)
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		DataProvider: []string{testDP},
		StreamID:     []string{testSID},
		EventTime:    []int64{1000},
		Value:        []string{"99.0"},
	})
	require.Nil(t, rpcErr)

	// Query: should get latest value (created_at=200 wins over 100)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		DataProvider: testDP, StreamID: testSID,
	})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1)
	require.Equal(t, "99.000000000000000000", resp.Records[0].Value,
		"should return latest version (created_at=200)")

	// Verify stream's created_at is the block height
	streams, rpcErr := ext.ListStreams(ctx, &ListStreamsRequest{})
	require.Nil(t, rpcErr)
	require.Len(t, streams.Streams, 1)
	require.Equal(t, int64(100), streams.Streams[0].CreatedAt,
		"stream created_at should be block height 100")
}

func TestIntegration_ComposedGetRecord(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()
	dp := testDP

	// Create two primitive child streams and one composed parent (exactly 32 chars each)
	childSID1 := "st000000000000000000000000child1"
	childSID2 := "st000000000000000000000000child2"
	composedSID := "st0000000000000000000000000comp1"

	for _, sid := range []string{childSID1, childSID2, composedSID} {
		stype := "primitive"
		if sid == composedSID {
			stype = "composed"
		}
		_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
			DataProvider: dp, StreamID: sid, StreamType: stype,
		})
		require.Nil(t, rpcErr, "create stream %s failed: %v", sid, rpcErr)
	}

	// child1: 10, 20, 30 at times 1000, 2000, 3000
	_, rpcErr := ext.InsertRecords(ctx, &InsertRecordsRequest{
		DataProvider: []string{dp, dp, dp},
		StreamID:     []string{childSID1, childSID1, childSID1},
		EventTime:    []int64{1000, 2000, 3000},
		Value:        []string{"10.0", "20.0", "30.0"},
	})
	require.Nil(t, rpcErr)

	// child2: 100, 200, 300 at times 1000, 2000, 3000
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		DataProvider: []string{dp, dp, dp},
		StreamID:     []string{childSID2, childSID2, childSID2},
		EventTime:    []int64{1000, 2000, 3000},
		Value:        []string{"100.0", "200.0", "300.0"},
	})
	require.Nil(t, rpcErr)

	// Taxonomy: composed = 0.5 * child1 + 0.5 * child2
	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		DataProvider:       dp,
		StreamID:           composedSID,
		ChildDataProviders: []string{dp, dp},
		ChildStreamIDs:     []string{childSID1, childSID2},
		Weights:            []string{"0.5", "0.5"},
		StartDate:          0,
	})
	require.Nil(t, rpcErr, "insert taxonomy failed: %v", rpcErr)

	// Query composed stream: weighted avg = (0.5*10+0.5*100)/1.0 = 55, etc.
	from := int64(500)
	to := int64(5000)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		DataProvider: dp, StreamID: composedSID,
		FromTime: &from, ToTime: &to,
	})
	require.Nil(t, rpcErr, "get_record composed failed: %v", rpcErr)
	require.Len(t, resp.Records, 3)
	require.Equal(t, "55.000000000000000000", resp.Records[0].Value)
	require.Equal(t, "110.000000000000000000", resp.Records[1].Value)
	require.Equal(t, "165.000000000000000000", resp.Records[2].Value)

	// Latest record
	resp, rpcErr = ext.GetRecord(ctx, &GetRecordRequest{
		DataProvider: dp, StreamID: composedSID,
	})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1)
	require.Equal(t, int64(3000), resp.Records[0].EventTime)
	require.Equal(t, "165.000000000000000000", resp.Records[0].Value)
}

func TestIntegration_ComposedGetIndex(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()
	dp := testDP

	childSID := "st0000000000000000000000000chld1"
	composedSID := "st0000000000000000000000000cmpsd"

	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{DataProvider: dp, StreamID: childSID, StreamType: "primitive"})
	require.Nil(t, rpcErr)
	_, rpcErr = ext.CreateStream(ctx, &CreateStreamRequest{DataProvider: dp, StreamID: composedSID, StreamType: "composed"})
	require.Nil(t, rpcErr)

	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		DataProvider: []string{dp, dp, dp},
		StreamID:     []string{childSID, childSID, childSID},
		EventTime:    []int64{1000, 2000, 3000},
		Value:        []string{"50.0", "75.0", "100.0"},
	})
	require.Nil(t, rpcErr)

	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		DataProvider:       dp,
		StreamID:           composedSID,
		ChildDataProviders: []string{dp},
		ChildStreamIDs:     []string{childSID},
		Weights:            []string{"1.0"},
		StartDate:          0,
	})
	require.Nil(t, rpcErr)

	// Index with default base_time: base=50, index = (val/50)*100
	from := int64(500)
	to := int64(5000)
	resp, rpcErr := ext.GetIndex(ctx, &GetIndexRequest{
		DataProvider: dp, StreamID: composedSID,
		FromTime: &from, ToTime: &to,
	})
	require.Nil(t, rpcErr, "get_index composed failed: %v", rpcErr)
	require.Len(t, resp.Records, 3)
	require.Equal(t, "100.000000000000000000", resp.Records[0].Value)
	require.Equal(t, "150.000000000000000000", resp.Records[1].Value)
	require.Equal(t, "200.000000000000000000", resp.Records[2].Value)
}

func TestIntegration_NestedComposed(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()
	dp := testDP

	// Hierarchy: root (composed) → mid (composed) → leaf (primitive)
	leafSID := "st000000000000000000000000leaf01"
	midSID := "st000000000000000000000000midi01"
	rootSID := "st000000000000000000000000root01"

	_, err := ext.CreateStream(ctx, &CreateStreamRequest{DataProvider: dp, StreamID: leafSID, StreamType: "primitive"})
	require.Nil(t, err)
	_, err = ext.CreateStream(ctx, &CreateStreamRequest{DataProvider: dp, StreamID: midSID, StreamType: "composed"})
	require.Nil(t, err)
	_, err = ext.CreateStream(ctx, &CreateStreamRequest{DataProvider: dp, StreamID: rootSID, StreamType: "composed"})
	require.Nil(t, err)

	_, err = ext.InsertRecords(ctx, &InsertRecordsRequest{
		DataProvider: []string{dp, dp, dp},
		StreamID:     []string{leafSID, leafSID, leafSID},
		EventTime:    []int64{1000, 2000, 3000},
		Value:        []string{"100.0", "200.0", "300.0"},
	})
	require.Nil(t, err)

	// mid = 1.0 * leaf
	_, err = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		DataProvider: dp, StreamID: midSID,
		ChildDataProviders: []string{dp}, ChildStreamIDs: []string{leafSID},
		Weights: []string{"1.0"}, StartDate: 0,
	})
	require.Nil(t, err)

	// root = 0.5 * mid
	_, err = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		DataProvider: dp, StreamID: rootSID,
		ChildDataProviders: []string{dp}, ChildStreamIDs: []string{midSID},
		Weights: []string{"0.5"}, StartDate: 0,
	})
	require.Nil(t, err)

	// Weighted average with single component: SUM(0.5*v)/SUM(0.5) = v
	from := int64(500)
	to := int64(5000)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		DataProvider: dp, StreamID: rootSID,
		FromTime: &from, ToTime: &to,
	})
	require.Nil(t, rpcErr, "nested composed query failed: %v", rpcErr)
	require.Len(t, resp.Records, 3)
	require.Equal(t, "100.000000000000000000", resp.Records[0].Value)
	require.Equal(t, "200.000000000000000000", resp.Records[1].Value)
	require.Equal(t, "300.000000000000000000", resp.Records[2].Value)
}

