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
		logger:      logger,
		db:          wrapper,
		nodeAddress: testNodeAddress,
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
		StreamID:   testSID,
		StreamType: "primitive",
	})
	require.Nil(t, rpcErr, "create stream failed: %v", rpcErr)

	// Insert records
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{testSID, testSID, testSID, testSID, testSID},
		EventTime: []int64{1000, 2000, 3000, 4000, 5000},
		Value:     []string{"10.5", "20.0", "30.75", "40.0", "50.25"},
	})
	require.Nil(t, rpcErr, "insert records failed: %v", rpcErr)

	// Test 1: Latest record (both nil)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		StreamID: testSID,
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
		StreamID: testSID,
		FromTime: &from,
		ToTime:   &to,
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
		StreamID: testSID,
		FromTime: &from,
		ToTime:   &to,
	})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1, "should have exactly 1 anchor record")
	require.Equal(t, int64(5000), resp.Records[0].EventTime)
}

func TestIntegration_PrimitiveGetIndex(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		StreamID:   testSID,
		StreamType: "primitive",
	})
	require.Nil(t, rpcErr)

	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{testSID, testSID, testSID},
		EventTime: []int64{1000, 2000, 3000},
		Value:     []string{"10.0", "20.0", "30.0"},
	})
	require.Nil(t, rpcErr)

	// Test 1: Default base_time (first event = 1000, base = 10.0)
	// Index = (value / base) * 100 → 100, 200, 300
	from := int64(500)
	to := int64(5000)
	resp, rpcErr := ext.GetIndex(ctx, &GetIndexRequest{
		StreamID: testSID,
		FromTime: &from,
		ToTime:   &to,
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
		StreamID: testSID,
		FromTime: &from,
		ToTime:   &to,
		BaseTime: &baseTime,
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
		sid, stype string
	}{
		{"st00000000000000000000000000aaaa", "primitive"},
		{"st00000000000000000000000000bbbb", "composed"},
		{"st00000000000000000000000000cccc", "primitive"},
	}
	for _, s := range streams {
		_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
			StreamID: s.sid, StreamType: s.stype,
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
		// list_streams response mirrors consensus shape — DataProvider is always
		// the node's own address (redundant but kept for parity).
		require.Equal(t, testNodeAddress, s.DataProvider)
	}
	require.Equal(t, 2, types["primitive"])
	require.Equal(t, 1, types["composed"])
}

func TestIntegration_GapFilling(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		StreamID: testSID, StreamType: "primitive",
	})
	require.Nil(t, rpcErr)

	// Insert sparse data: values at 1000, 3000, 5000
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{testSID, testSID, testSID},
		EventTime: []int64{1000, 3000, 5000},
		Value:     []string{"10.0", "30.0", "50.0"},
	})
	require.Nil(t, rpcErr)

	// Query range that starts between data points
	// from=2000, to=4000 → should get anchor at 1000, then 3000
	from := int64(2000)
	to := int64(4000)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		StreamID: testSID,
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
		StreamID: testSID, StreamType: "primitive",
	})
	require.Nil(t, rpcErr)

	// Insert at height 100
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{testSID},
		EventTime: []int64{1000},
		Value:     []string{"10.0"},
	})
	require.Nil(t, rpcErr)

	// Advance height and insert updated value for same event_time
	ext.height.Store(200)
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{testSID},
		EventTime: []int64{1000},
		Value:     []string{"99.0"},
	})
	require.Nil(t, rpcErr)

	// Query: should get latest value (created_at=200 wins over 100)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		StreamID: testSID,
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
			StreamID: sid, StreamType: stype,
		})
		require.Nil(t, rpcErr, "create stream %s failed: %v", sid, rpcErr)
	}

	// child1: 10, 20, 30 at times 1000, 2000, 3000
	_, rpcErr := ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{childSID1, childSID1, childSID1},
		EventTime: []int64{1000, 2000, 3000},
		Value:     []string{"10.0", "20.0", "30.0"},
	})
	require.Nil(t, rpcErr)

	// child2: 100, 200, 300 at times 1000, 2000, 3000
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{childSID2, childSID2, childSID2},
		EventTime: []int64{1000, 2000, 3000},
		Value:     []string{"100.0", "200.0", "300.0"},
	})
	require.Nil(t, rpcErr)

	// Taxonomy: composed = 0.5 * child1 + 0.5 * child2
	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID:       composedSID,
		ChildStreamIDs: []string{childSID1, childSID2},
		Weights:        []string{"0.5", "0.5"},
		StartDate:      0,
	})
	require.Nil(t, rpcErr, "insert taxonomy failed: %v", rpcErr)

	// Query composed stream: weighted avg = (0.5*10+0.5*100)/1.0 = 55, etc.
	from := int64(500)
	to := int64(5000)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		StreamID: composedSID,
		FromTime: &from, ToTime: &to,
	})
	require.Nil(t, rpcErr, "get_record composed failed: %v", rpcErr)
	require.Len(t, resp.Records, 3)
	require.Equal(t, "55.000000000000000000", resp.Records[0].Value)
	require.Equal(t, "110.000000000000000000", resp.Records[1].Value)
	require.Equal(t, "165.000000000000000000", resp.Records[2].Value)

	// Latest record
	resp, rpcErr = ext.GetRecord(ctx, &GetRecordRequest{
		StreamID: composedSID,
	})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1)
	require.Equal(t, int64(3000), resp.Records[0].EventTime)
	require.Equal(t, "165.000000000000000000", resp.Records[0].Value)
}

func TestIntegration_ComposedGetIndex(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	childSID := "st0000000000000000000000000chld1"
	composedSID := "st0000000000000000000000000cmpsd"

	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{StreamID: childSID, StreamType: "primitive"})
	require.Nil(t, rpcErr)
	_, rpcErr = ext.CreateStream(ctx, &CreateStreamRequest{StreamID: composedSID, StreamType: "composed"})
	require.Nil(t, rpcErr)

	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{childSID, childSID, childSID},
		EventTime: []int64{1000, 2000, 3000},
		Value:     []string{"50.0", "75.0", "100.0"},
	})
	require.Nil(t, rpcErr)

	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID:       composedSID,
		ChildStreamIDs: []string{childSID},
		Weights:        []string{"1.0"},
		StartDate:      0,
	})
	require.Nil(t, rpcErr)

	// Index with default base_time: base=50, index = (val/50)*100
	from := int64(500)
	to := int64(5000)
	resp, rpcErr := ext.GetIndex(ctx, &GetIndexRequest{
		StreamID: composedSID,
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

	// Hierarchy: root (composed) → mid (composed) → leaf (primitive)
	leafSID := "st000000000000000000000000leaf01"
	midSID := "st000000000000000000000000midi01"
	rootSID := "st000000000000000000000000root01"

	_, err := ext.CreateStream(ctx, &CreateStreamRequest{StreamID: leafSID, StreamType: "primitive"})
	require.Nil(t, err)
	_, err = ext.CreateStream(ctx, &CreateStreamRequest{StreamID: midSID, StreamType: "composed"})
	require.Nil(t, err)
	_, err = ext.CreateStream(ctx, &CreateStreamRequest{StreamID: rootSID, StreamType: "composed"})
	require.Nil(t, err)

	_, err = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{leafSID, leafSID, leafSID},
		EventTime: []int64{1000, 2000, 3000},
		Value:     []string{"100.0", "200.0", "300.0"},
	})
	require.Nil(t, err)

	// mid = 1.0 * leaf
	_, err = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID: midSID,
		ChildStreamIDs: []string{leafSID},
		Weights: []string{"1.0"}, StartDate: 0,
	})
	require.Nil(t, err)

	// root = 0.5 * mid
	_, err = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID: rootSID,
		ChildStreamIDs: []string{midSID},
		Weights: []string{"0.5"}, StartDate: 0,
	})
	require.Nil(t, err)

	// Weighted average with single component: SUM(0.5*v)/SUM(0.5) = v
	from := int64(500)
	to := int64(5000)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		StreamID: rootSID,
		FromTime: &from, ToTime: &to,
	})
	require.Nil(t, rpcErr, "nested composed query failed: %v", rpcErr)
	require.Len(t, resp.Records, 3)
	require.Equal(t, "100.000000000000000000", resp.Records[0].Value)
	require.Equal(t, "200.000000000000000000", resp.Records[1].Value)
	require.Equal(t, "300.000000000000000000", resp.Records[2].Value)
}

// TestIntegration_TaxonomyReplacementGroup exercises the taxonomy "replacement
// group" path. A second InsertTaxonomy call REMOVES one of the original children
// (B) and adds a new one (C) with new weights at a later StartDate. The fixed
// active_taxonomy CTE must close child B's segment at the new group's start_time,
// not extend it to maxInt8.
//
// Pre-fix bug: LEAD partitioned by (parent, child) couldn't see B's removal
// because there's no later (parent, B) row, so B's end_time stayed maxInt8 and
// B kept contributing to the weighted average past the boundary.
func TestIntegration_TaxonomyReplacementGroup(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	// Three primitive children + one composed parent (32-char SIDs).
	childA := "st0000000000000000000000000reptA"
	childB := "st0000000000000000000000000reptB"
	childC := "st0000000000000000000000000reptC"
	composedSID := "st00000000000000000000000replcmp"

	for _, sid := range []string{childA, childB, childC, composedSID} {
		stype := "primitive"
		if sid == composedSID {
			stype = "composed"
		}
		_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
			StreamID: sid, StreamType: stype,
		})
		require.Nil(t, rpcErr, "create stream %s failed: %v", sid, rpcErr)
	}

	// Insert leaf events that span the taxonomy boundary at start_time=200.
	// Each leaf has a constant value to make weighted-avg arithmetic obvious.
	// A: value 10, B: value 100, C: value 1000.
	_, rpcErr := ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{childA, childA, childB, childB, childC, childC},
		EventTime: []int64{100, 300, 100, 300, 100, 300},
		Value:     []string{"10.0", "10.0", "100.0", "100.0", "1000.0", "1000.0"},
	})
	require.Nil(t, rpcErr)

	// Group 1 @ start_time=0: { A: 0.5, B: 0.5 }
	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID:       composedSID,
		ChildStreamIDs: []string{childA, childB},
		Weights:        []string{"0.5", "0.5"},
		StartDate:      0,
	})
	require.Nil(t, rpcErr, "insert taxonomy group 1 failed: %v", rpcErr)

	// Group 2 @ start_time=200: { A: 0.7, C: 0.3 } — B is REMOVED.
	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID:       composedSID,
		ChildStreamIDs: []string{childA, childC},
		Weights:        []string{"0.7", "0.3"},
		StartDate:      200,
	})
	require.Nil(t, rpcErr, "insert taxonomy group 2 failed: %v", rpcErr)

	// Query across the boundary: from=50 to=400 should produce 2 distinct values.
	from := int64(50)
	to := int64(400)
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		StreamID: composedSID,
		FromTime: &from, ToTime: &to,
	})
	require.Nil(t, rpcErr, "get_record across replacement boundary failed: %v", rpcErr)
	require.NotEmpty(t, resp.Records)

	// Build a time→value map for assertions.
	values := make(map[int64]string, len(resp.Records))
	for _, r := range resp.Records {
		values[r.EventTime] = r.Value
		t.Logf("composed[%d] = %s", r.EventTime, r.Value)
	}

	// At time 100 (group 1 active): 0.5*10 + 0.5*100 = 55
	require.Contains(t, values, int64(100))
	require.Equal(t, "55.000000000000000000", values[100],
		"at time 100 (group 1) value should be 0.5*10 + 0.5*100 = 55")

	// At time 200 (group 2 boundary, no leaf event but taxonomy change):
	// LOCF brings A=10 and C=1000 (C had event at 100). New weights apply.
	// Expected: 0.7*10 + 0.3*1000 = 7 + 300 = 307
	// (B is properly closed at 199 by the parent_next_starts fix.)
	require.Contains(t, values, int64(200), "boundary time should appear in results")
	require.Equal(t, "307.000000000000000000", values[200],
		"at time 200 (group 2 boundary) value should be 0.7*10 + 0.3*1000 = 307 — "+
			"if you see 53.5 (= 0.5*10+0.5*100/1.0 with weights divided), B's segment "+
			"was not closed; if you see 0.5*10+0.5*100=55 with mixed contributions, "+
			"the fix is incomplete")

	// At time 300 (group 2 active, both A and C have new events at 300):
	// 0.7*10 + 0.3*1000 = 307
	require.Contains(t, values, int64(300))
	require.Equal(t, "307.000000000000000000", values[300],
		"at time 300 (group 2) value should be 0.7*10 + 0.3*1000 = 307")

	// "Latest" lookup should also use the new weights, not the stale ones.
	latestResp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{
		StreamID: composedSID,
	})
	require.Nil(t, rpcErr)
	require.Len(t, latestResp.Records, 1)
	require.Equal(t, int64(300), latestResp.Records[0].EventTime)
	require.Equal(t, "307.000000000000000000", latestResp.Records[0].Value,
		"latest record should reflect group 2 weights")

	// GetIndex with explicit base_time before the boundary should also work.
	baseTime := int64(100)
	indexResp, rpcErr := ext.GetIndex(ctx, &GetIndexRequest{
		StreamID: composedSID,
		FromTime: &from, ToTime: &to, BaseTime: &baseTime,
	})
	require.Nil(t, rpcErr, "get_index across replacement boundary failed: %v", rpcErr)
	require.NotEmpty(t, indexResp.Records)
	indexValues := make(map[int64]string, len(indexResp.Records))
	for _, r := range indexResp.Records {
		indexValues[r.EventTime] = r.Value
	}
	// Index at time 100 = (55 / 55) * 100 = 100
	require.Equal(t, "100.000000000000000000", indexValues[100])
	// Index at time 300 = (307 / 55) * 100 ≈ 558.181818...
	require.Contains(t, indexValues, int64(300))
}

func TestIntegration_DeleteStream_Primitive(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	// Create a primitive stream and insert records.
	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		StreamID: testSID, StreamType: "primitive",
	})
	require.Nil(t, rpcErr)

	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{testSID, testSID},
		EventTime: []int64{1000, 2000},
		Value:     []string{"10.0", "20.0"},
	})
	require.Nil(t, rpcErr)

	// Verify data exists before delete.
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{StreamID: testSID})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1, "should have latest record")

	// Delete the stream — ON DELETE CASCADE should remove records too.
	_, rpcErr = ext.DeleteStream(ctx, &DeleteStreamRequest{StreamID: testSID})
	require.Nil(t, rpcErr)

	// Verify stream is gone: GetRecord should return "stream not found".
	_, rpcErr = ext.GetRecord(ctx, &GetRecordRequest{StreamID: testSID})
	require.NotNil(t, rpcErr, "expected error after deleting stream")
	require.Contains(t, rpcErr.Message, "not found")

	// Verify ListStreams no longer includes it.
	listResp, rpcErr := ext.ListStreams(ctx, &ListStreamsRequest{})
	require.Nil(t, rpcErr)
	require.Empty(t, listResp.Streams, "deleted stream should not appear in list")

	// Deleting again should fail with "not found".
	_, rpcErr = ext.DeleteStream(ctx, &DeleteStreamRequest{StreamID: testSID})
	require.NotNil(t, rpcErr)
	require.Contains(t, rpcErr.Message, "not found")
}

func TestIntegration_DeleteStream_Composed(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	childA := "st000000000000000000000000delchA"
	childB := "st000000000000000000000000delchB"
	composedSID := "st000000000000000000000000delcmp"

	// Create children and composed parent.
	for _, sid := range []string{childA, childB} {
		_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
			StreamID: sid, StreamType: "primitive",
		})
		require.Nil(t, rpcErr)
	}
	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		StreamID: composedSID, StreamType: "composed",
	})
	require.Nil(t, rpcErr)

	// Insert child records and taxonomy.
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{childA, childB},
		EventTime: []int64{100, 100},
		Value:     []string{"50.0", "50.0"},
	})
	require.Nil(t, rpcErr)

	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID:       composedSID,
		ChildStreamIDs: []string{childA, childB},
		Weights:        []string{"0.5", "0.5"},
		StartDate:      0,
	})
	require.Nil(t, rpcErr)

	// Verify composed stream works.
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{StreamID: composedSID})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1)
	require.Equal(t, "50.000000000000000000", resp.Records[0].Value)

	// Delete composed parent — cascades taxonomy rows but children survive.
	_, rpcErr = ext.DeleteStream(ctx, &DeleteStreamRequest{StreamID: composedSID})
	require.Nil(t, rpcErr)

	// Children should still exist.
	childResp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{StreamID: childA})
	require.Nil(t, rpcErr)
	require.Len(t, childResp.Records, 1, "child stream should survive parent deletion")

	// ListStreams should show only the two children.
	listResp, rpcErr := ext.ListStreams(ctx, &ListStreamsRequest{})
	require.Nil(t, rpcErr)
	require.Len(t, listResp.Streams, 2, "only child streams should remain")
}

func TestIntegration_DisableTaxonomy(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	childA := "st000000000000000000000000dschA0"
	childB := "st000000000000000000000000dschB0"
	childC := "st000000000000000000000000dschC0"
	composedSID := "st00000000000000000000000dscomp0"

	// Create children and composed parent.
	for _, sid := range []string{childA, childB, childC} {
		_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
			StreamID: sid, StreamType: "primitive",
		})
		require.Nil(t, rpcErr)
	}
	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		StreamID: composedSID, StreamType: "composed",
	})
	require.Nil(t, rpcErr)

	// Insert leaf data: A=10, B=100, C=1000.
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{childA, childB, childC},
		EventTime: []int64{100, 100, 100},
		Value:     []string{"10.0", "100.0", "1000.0"},
	})
	require.Nil(t, rpcErr)

	// Group 0: { A: 0.5, B: 0.5 } @ start_time=0.
	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID:       composedSID,
		ChildStreamIDs: []string{childA, childB},
		Weights:        []string{"0.5", "0.5"},
		StartDate:      0,
	})
	require.Nil(t, rpcErr)

	// Verify composed value before disable: 0.5*10 + 0.5*100 = 55.
	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{StreamID: composedSID})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1)
	require.Equal(t, "55.000000000000000000", resp.Records[0].Value)

	// Disable group 0.
	_, rpcErr = ext.DisableTaxonomy(ctx, &DisableTaxonomyRequest{
		StreamID:      composedSID,
		GroupSequence: 1,
	})
	require.Nil(t, rpcErr)

	// After disabling the only taxonomy group, composed stream should have no data.
	resp, rpcErr = ext.GetRecord(ctx, &GetRecordRequest{StreamID: composedSID})
	require.Nil(t, rpcErr)
	require.Empty(t, resp.Records, "composed stream with disabled taxonomy should return no records")

	// Disabling again should fail — already disabled.
	_, rpcErr = ext.DisableTaxonomy(ctx, &DisableTaxonomyRequest{
		StreamID:      composedSID,
		GroupSequence: 1,
	})
	require.NotNil(t, rpcErr, "expected error when disabling already-disabled group")
	require.Contains(t, rpcErr.Message, "not found or already disabled")
}

func TestIntegration_DisableTaxonomy_OnPrimitive(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	// DisableTaxonomy should reject primitive streams.
	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		StreamID: testSID, StreamType: "primitive",
	})
	require.Nil(t, rpcErr)

	_, rpcErr = ext.DisableTaxonomy(ctx, &DisableTaxonomyRequest{
		StreamID:      testSID,
		GroupSequence: 1,
	})
	require.NotNil(t, rpcErr, "expected error on primitive stream")
	require.Contains(t, rpcErr.Message, "not a composed stream")
}

func TestIntegration_DisableTaxonomy_ThenReplace(t *testing.T) {
	ext := setupIntegrationDB(t)
	ctx := context.Background()

	childA := "st0000000000000000000000dtrrchA0"
	childB := "st0000000000000000000000dtrrchB0"
	childC := "st0000000000000000000000dtrrchC0"
	composedSID := "st000000000000000000000dtrrcmp00"

	for _, sid := range []string{childA, childB, childC} {
		_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
			StreamID: sid, StreamType: "primitive",
		})
		require.Nil(t, rpcErr)
	}
	_, rpcErr := ext.CreateStream(ctx, &CreateStreamRequest{
		StreamID: composedSID, StreamType: "composed",
	})
	require.Nil(t, rpcErr)

	// Insert child data: A=10, B=100, C=1000.
	_, rpcErr = ext.InsertRecords(ctx, &InsertRecordsRequest{
		StreamID:  []string{childA, childB, childC},
		EventTime: []int64{100, 100, 100},
		Value:     []string{"10.0", "100.0", "1000.0"},
	})
	require.Nil(t, rpcErr)

	// Group 0: { A: 0.5, B: 0.5 } → value = 55.
	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID:       composedSID,
		ChildStreamIDs: []string{childA, childB},
		Weights:        []string{"0.5", "0.5"},
		StartDate:      0,
	})
	require.Nil(t, rpcErr)

	// Disable group 0 — composed returns no data.
	_, rpcErr = ext.DisableTaxonomy(ctx, &DisableTaxonomyRequest{
		StreamID:      composedSID,
		GroupSequence: 1,
	})
	require.Nil(t, rpcErr)

	resp, rpcErr := ext.GetRecord(ctx, &GetRecordRequest{StreamID: composedSID})
	require.Nil(t, rpcErr)
	require.Empty(t, resp.Records, "disabled taxonomy should yield no records")

	// Insert replacement group 1: { A: 0.3, C: 0.7 } @ start_time=200.
	_, rpcErr = ext.InsertTaxonomy(ctx, &InsertTaxonomyRequest{
		StreamID:       composedSID,
		ChildStreamIDs: []string{childA, childC},
		Weights:        []string{"0.3", "0.7"},
		StartDate:      200,
	})
	require.Nil(t, rpcErr)

	// Now composed stream should use group 1 weights:
	// LOCF carries A=10 and C=1000 → 0.3*10 + 0.7*1000 = 3 + 700 = 703.
	resp, rpcErr = ext.GetRecord(ctx, &GetRecordRequest{StreamID: composedSID})
	require.Nil(t, rpcErr)
	require.Len(t, resp.Records, 1)
	require.Equal(t, "703.000000000000000000", resp.Records[0].Value,
		"after disabling group 0 and adding group 1: 0.3*10 + 0.7*1000 = 703")
}
