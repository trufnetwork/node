package tn_local

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
)

// pgUniqueViolation is the PostgreSQL error code for unique_violation.
const pgUniqueViolation = "23505"

var ethAddrRegex = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// validateStreamID checks that stream_id is 32 chars and starts with "st".
func validateStreamID(streamID string) error {
	if len(streamID) != 32 {
		return fmt.Errorf("stream_id must be exactly 32 characters, got %d", len(streamID))
	}
	if !strings.HasPrefix(streamID, "st") {
		return fmt.Errorf("stream_id must start with 'st'")
	}
	return nil
}

// validateStreamType checks that stream_type is "primitive" or "composed".
func validateStreamType(streamType string) error {
	if streamType != "primitive" && streamType != "composed" {
		return fmt.Errorf("stream_type must be 'primitive' or 'composed', got %q", streamType)
	}
	return nil
}

// validateDataProvider checks that data_provider is a valid Ethereum address (0x + 40 hex).
func validateDataProvider(dataProvider string) error {
	if !ethAddrRegex.MatchString(dataProvider) {
		return fmt.Errorf("data_provider must be a valid Ethereum address (0x + 40 hex chars)")
	}
	return nil
}

// validateWeight checks that a weight value is a valid non-negative number.
func validateWeight(weight string) error {
	f, err := strconv.ParseFloat(weight, 64)
	if err != nil {
		return fmt.Errorf("weight must be a valid number")
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("weight must be a finite number")
	}
	if f < 0 {
		return fmt.Errorf("weight must be non-negative")
	}
	return nil
}

// generateTaxonomyID creates a unique UUID from taxonomy components.
// Mirrors consensus: uuid_generate_kwil(@txid||$data_provider||...).
// Since local operations have no @txid, we include UnixNano for per-call
// uniqueness (analogous to how @txid differs per transaction in consensus).
func generateTaxonomyID(dataProvider, streamID, childDP, childSID string, index int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%s|%d|%d", dataProvider, streamID, childDP, childSID, index, time.Now().UnixNano())))
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		h[0:4], h[4:6], h[6:8], h[8:10], h[10:16])
}

// isDuplicateKeyError checks if err is a PostgreSQL unique constraint violation (23505).
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation
	}
	// Fallback for non-pgx drivers (e.g. mocks): case-insensitive string match.
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") || strings.Contains(msg, "unique constraint")
}

// CreateStream creates a local stream.
func (ext *Extension) CreateStream(ctx context.Context, req *CreateStreamRequest) (*CreateStreamResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}

	// Normalize data_provider to lowercase to match consensus behavior
	// (consensus uses LOWER() in 001-common-actions.sql before insertion).
	dataProvider := strings.ToLower(req.DataProvider)

	if err := validateDataProvider(dataProvider); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}
	if err := validateStreamID(req.StreamID); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}
	if err := validateStreamType(req.StreamType); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}

	if err := ext.dbCreateStream(ctx, dataProvider, req.StreamID, req.StreamType); err != nil {
		if isDuplicateKeyError(err) {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream already exists: %s/%s", dataProvider, req.StreamID), nil)
		}
		ext.logger.Error("failed to create local stream", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to create stream", nil)
	}

	return &CreateStreamResponse{}, nil
}

// InsertRecords inserts records into local primitive streams.
// Mirrors the consensus insert_records action (003-primitive-insertion.sql):
//   - Parallel arrays: data_provider[], stream_id[], event_time[], value[]
//   - Zero values are silently filtered (WHERE value != 0)
//   - Multiple rows per (stream_ref, event_time) allowed (created_at versioning)
//   - Returns empty response (consensus returns nothing)
func (ext *Extension) InsertRecords(ctx context.Context, req *InsertRecordsRequest) (*InsertRecordsResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}

	n := len(req.DataProvider)
	if n == 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "records must not be empty", nil)
	}
	if n != len(req.StreamID) || n != len(req.EventTime) || n != len(req.Value) {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "array lengths mismatch", nil)
	}

	// Normalize data_providers to lowercase (consensus uses LOWER() in 001-common-actions.sql).
	for i := range req.DataProvider {
		req.DataProvider[i] = strings.ToLower(req.DataProvider[i])
	}

	// Validate all inputs upfront.
	for i := 0; i < n; i++ {
		if err := validateDataProvider(req.DataProvider[i]); err != nil {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("record %d: %v", i, err), nil)
		}
		if err := validateStreamID(req.StreamID[i]); err != nil {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("record %d: %v", i, err), nil)
		}
		f, parseErr := strconv.ParseFloat(req.Value[i], 64)
		if parseErr != nil {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("invalid record value at index %d: %v", i, parseErr), nil)
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("invalid record value at index %d: must be a finite number", i), nil)
		}
	}

	// Resolve stream refs for unique (data_provider, stream_id) pairs.
	type streamKey struct{ dp, sid string }
	streamRefMap := make(map[streamKey]int64)
	for i := 0; i < n; i++ {
		key := streamKey{req.DataProvider[i], req.StreamID[i]}
		if _, ok := streamRefMap[key]; ok {
			continue
		}
		ref, stype, err := ext.dbLookupStreamRef(ctx, key.dp, key.sid)
		if err != nil {
			ext.logger.Error("failed to look up stream", "error", err)
			return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to look up stream", nil)
		}
		if ref == 0 {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream not found: %s/%s", key.dp, key.sid), nil)
		}
		if stype != "primitive" {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream %s/%s is not a primitive stream", key.dp, key.sid), nil)
		}
		streamRefMap[key] = ref
	}

	// Build resolved records, filtering zero values (mirrors consensus WHERE value != 0).
	streamRefs := make([]int64, 0, n)
	eventTimes := make([]int64, 0, n)
	values := make([]string, 0, n)
	for i := 0; i < n; i++ {
		f, _ := strconv.ParseFloat(req.Value[i], 64)
		if f == 0 {
			continue
		}
		key := streamKey{req.DataProvider[i], req.StreamID[i]}
		streamRefs = append(streamRefs, streamRefMap[key])
		eventTimes = append(eventTimes, req.EventTime[i])
		values = append(values, req.Value[i])
	}

	if len(streamRefs) > 0 {
		if err := ext.dbInsertRecords(ctx, streamRefs, eventTimes, values); err != nil {
			ext.logger.Error("failed to insert records", "error", err)
			return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to insert records", nil)
		}
	}

	return &InsertRecordsResponse{}, nil
}

// InsertTaxonomy adds a taxonomy group to a local composed stream.
// Mirrors the consensus insert_taxonomy action (004-composed-taxonomy.sql):
//   - Parallel arrays: child_data_providers[], child_stream_ids[], weights[]
//   - All children must exist in local storage (no cross-storage references)
//   - Increments group_sequence (MAX+1)
//   - Returns empty response (consensus returns nothing)
func (ext *Extension) InsertTaxonomy(ctx context.Context, req *InsertTaxonomyRequest) (*InsertTaxonomyResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}

	n := len(req.ChildStreamIDs)
	if n == 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "there must be at least 1 child", nil)
	}
	if n != len(req.ChildDataProviders) || n != len(req.Weights) {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "child array lengths mismatch", nil)
	}

	// Normalize parent data_provider to lowercase (consensus: LOWER()).
	dataProvider := strings.ToLower(req.DataProvider)
	if err := validateDataProvider(dataProvider); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}
	if err := validateStreamID(req.StreamID); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}
	if req.StartDate < 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "start_date must be >= 0", nil)
	}

	// Normalize child data_providers to lowercase.
	for i := range req.ChildDataProviders {
		req.ChildDataProviders[i] = strings.ToLower(req.ChildDataProviders[i])
	}

	// Validate all children upfront.
	for i := 0; i < n; i++ {
		if err := validateDataProvider(req.ChildDataProviders[i]); err != nil {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("child %d: %v", i, err), nil)
		}
		if err := validateStreamID(req.ChildStreamIDs[i]); err != nil {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("child %d: %v", i, err), nil)
		}
		if err := validateWeight(req.Weights[i]); err != nil {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("child %d: %v", i, err), nil)
		}
	}

	// Verify parent stream exists and is composed.
	parentRef, parentType, err := ext.dbLookupStreamRef(ctx, dataProvider, req.StreamID)
	if err != nil {
		ext.logger.Error("failed to look up parent stream", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to look up parent stream", nil)
	}
	if parentRef == 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("parent stream not found: %s/%s", dataProvider, req.StreamID), nil)
	}
	if parentType != "composed" {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream %s/%s is not a composed stream", dataProvider, req.StreamID), nil)
	}

	// Resolve all child stream refs (must exist in local storage).
	childRefs := make([]int64, n)
	for i := 0; i < n; i++ {
		ref, _, lookupErr := ext.dbLookupStreamRef(ctx, req.ChildDataProviders[i], req.ChildStreamIDs[i])
		if lookupErr != nil {
			ext.logger.Error("failed to look up child stream", "error", lookupErr)
			return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to look up child stream", nil)
		}
		if ref == 0 {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("child stream not found in local storage: %s/%s", req.ChildDataProviders[i], req.ChildStreamIDs[i]), nil)
		}
		childRefs[i] = ref
	}

	// Default start_date to 0 if not provided (mirrors consensus: if $start_date IS NULL { $start_date := 0; }).
	startDate := req.StartDate

	// Begin transaction for group_sequence + inserts.
	tx, err := ext.db.BeginTx(ctx)
	if err != nil {
		ext.logger.Error("failed to begin transaction", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to begin transaction", nil)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	groupSeq, err := ext.dbGetNextGroupSequence(ctx, tx, parentRef)
	if err != nil {
		ext.logger.Error("failed to get next group sequence", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to get group sequence", nil)
	}

	createdAt := time.Now().Unix()
	for i := 0; i < n; i++ {
		taxonomyID := generateTaxonomyID(dataProvider, req.StreamID, req.ChildDataProviders[i], req.ChildStreamIDs[i], i)
		if insertErr := ext.dbInsertTaxonomyRow(ctx, tx, taxonomyID, parentRef, childRefs[i], req.Weights[i], startDate, groupSeq, createdAt); insertErr != nil {
			ext.logger.Error("failed to insert taxonomy row", "error", insertErr)
			return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to insert taxonomy", nil)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		ext.logger.Error("failed to commit transaction", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to commit taxonomy", nil)
	}

	return &InsertTaxonomyResponse{}, nil
}

// GetRecord queries records from a local primitive stream. (Task 6)
func (ext *Extension) GetRecord(ctx context.Context, req *GetRecordRequest) (*GetRecordResponse, *jsonrpc.Error) {
	return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "not implemented", nil)
}

// GetIndex queries computed index values from a local stream. (Task 6)
func (ext *Extension) GetIndex(ctx context.Context, req *GetIndexRequest) (*GetIndexResponse, *jsonrpc.Error) {
	return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "not implemented", nil)
}

// ListStreams lists all local streams. (Task 6)
func (ext *Extension) ListStreams(ctx context.Context, req *ListStreamsRequest) (*ListStreamsResponse, *jsonrpc.Error) {
	return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "not implemented", nil)
}
