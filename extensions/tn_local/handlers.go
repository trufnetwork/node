package tn_local

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shopspring/decimal"
	jsonrpc "github.com/trufnetwork/kwil-db/core/rpc/json"
)

// pgUniqueViolation is the PostgreSQL error code for unique_violation.
const pgUniqueViolation = "23505"

// disabledError is the error every handler returns when the extension is not
// enabled (e.g. the node has no secp256k1 operator key). Local stream
// operations require a stable operator identity to claim ownership of, so
// they cannot run on read-only or ed25519-only nodes.
func disabledError() *jsonrpc.Error {
	return jsonrpc.NewError(jsonrpc.ErrorInternal,
		"tn_local is disabled: this node has no secp256k1 operator key", nil)
}

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

// validateWeight checks that a weight value fits NUMERIC(36,18):
// non-negative, at most 18 decimal places, at most 18 integral digits (36 total - 18 scale).
func validateWeight(weight string) error {
	d, err := decimal.NewFromString(weight)
	if err != nil {
		return fmt.Errorf("weight must be a valid decimal number")
	}
	if d.IsNegative() {
		return fmt.Errorf("weight must be non-negative")
	}
	// NUMERIC(36,18): up to 18 fractional digits, up to 18 integral digits.
	// Exponent returns (coeff, exp) where value = coeff * 10^exp.
	// Negative exponent = number of fractional digits.
	exp := d.Exponent()
	if exp < -18 {
		return fmt.Errorf("weight exceeds NUMERIC(36,18): more than 18 decimal places")
	}
	// Check integral digits: remove trailing zeros, count digits left of decimal point.
	// NumDigits gives total significant digits in the coefficient.
	intPart := d.Truncate(0).Abs()
	if intPart.NumDigits() > 18 {
		return fmt.Errorf("weight exceeds NUMERIC(36,18): more than 18 integral digits")
	}
	return nil
}

// generateTaxonomyID creates a unique UUID from taxonomy components.
// Mirrors consensus: uuid_generate_kwil(@txid||$data_provider||$stream_id||$child||$i).
// Since local operations have no @txid, we use groupSeq — an int assigned by
// dbGetNextGroupSequence under an advisory lock, guaranteed unique per
// (parent stream, insertion call). Together with index (unique per child in
// a single call), this makes IDs deterministic and collision-free without
// depending on wall-clock time.
func generateTaxonomyID(dataProvider, streamID, childSID string, groupSeq, index int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%d", dataProvider, streamID, childSID, groupSeq, index)))
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

// CreateStream creates a local stream owned by the node operator.
func (ext *Extension) CreateStream(ctx context.Context, req *CreateStreamRequest) (*CreateStreamResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}
	if !ext.isEnabled.Load() {
		return nil, disabledError()
	}
	if authErr := ext.checkAuth(ctx, MethodCreateStream, req); authErr != nil {
		return nil, authErr
	}

	if err := validateStreamID(req.StreamID); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}
	if err := validateStreamType(req.StreamType); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}

	if err := ext.dbCreateStream(ctx, ext.nodeAddress, req.StreamID, req.StreamType); err != nil {
		if isDuplicateKeyError(err) {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream already exists: %s", req.StreamID), nil)
		}
		ext.logger.Error("failed to create local stream", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to create stream", nil)
	}

	return &CreateStreamResponse{}, nil
}

// InsertRecords inserts records into local primitive streams owned by the node.
// Mirrors the consensus insert_records action (003-primitive-insertion.sql):
//   - Parallel arrays: stream_id[], event_time[], value[]
//   - Zero values are silently filtered (WHERE value != 0)
//   - Multiple rows per (stream_ref, event_time) allowed (created_at versioning)
//   - Returns empty response (consensus returns nothing)
//
// All records implicitly target streams owned by this node — no data_provider
// on the wire.
func (ext *Extension) InsertRecords(ctx context.Context, req *InsertRecordsRequest) (*InsertRecordsResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}
	if !ext.isEnabled.Load() {
		return nil, disabledError()
	}
	if authErr := ext.checkAuth(ctx, MethodInsertRecords, req); authErr != nil {
		return nil, authErr
	}

	n := len(req.StreamID)
	if n == 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "records must not be empty", nil)
	}
	if n != len(req.EventTime) || n != len(req.Value) {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "array lengths mismatch", nil)
	}

	// Validate all inputs upfront.
	for i := 0; i < n; i++ {
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

	// Resolve stream refs for unique stream_ids (all owned by ext.nodeAddress).
	streamRefMap := make(map[string]int64)
	for i := 0; i < n; i++ {
		sid := req.StreamID[i]
		if _, ok := streamRefMap[sid]; ok {
			continue
		}
		ref, stype, err := ext.dbLookupStreamRef(ctx, ext.nodeAddress, sid)
		if err != nil {
			ext.logger.Error("failed to look up stream", "error", err)
			return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to look up stream", nil)
		}
		if ref == 0 {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream not found: %s", sid), nil)
		}
		if stype != "primitive" {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream %s is not a primitive stream", sid), nil)
		}
		streamRefMap[sid] = ref
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
		streamRefs = append(streamRefs, streamRefMap[req.StreamID[i]])
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
//   - Parallel arrays: child_stream_ids[], weights[]
//   - All children are local streams owned by this node (no cross-DP children)
//   - Increments group_sequence (MAX+1)
//   - Returns empty response (consensus returns nothing)
func (ext *Extension) InsertTaxonomy(ctx context.Context, req *InsertTaxonomyRequest) (*InsertTaxonomyResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}
	if !ext.isEnabled.Load() {
		return nil, disabledError()
	}
	if authErr := ext.checkAuth(ctx, MethodInsertTaxonomy, req); authErr != nil {
		return nil, authErr
	}

	n := len(req.ChildStreamIDs)
	if n == 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "there must be at least 1 child", nil)
	}
	if n != len(req.Weights) {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "child array lengths mismatch", nil)
	}

	if err := validateStreamID(req.StreamID); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}
	if req.StartDate < 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "start_date must be >= 0", nil)
	}

	// Validate all children upfront. Children share the node's data_provider,
	// so dedup is purely by stream_id.
	seenChildren := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		if err := validateStreamID(req.ChildStreamIDs[i]); err != nil {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("child %d: %v", i, err), nil)
		}
		if err := validateWeight(req.Weights[i]); err != nil {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("child %d: %v", i, err), nil)
		}
		if _, exists := seenChildren[req.ChildStreamIDs[i]]; exists {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("child %d: duplicate child_stream_id", i), nil)
		}
		seenChildren[req.ChildStreamIDs[i]] = struct{}{}
	}

	// Verify parent stream exists and is composed.
	parentRef, parentType, err := ext.dbLookupStreamRef(ctx, ext.nodeAddress, req.StreamID)
	if err != nil {
		ext.logger.Error("failed to look up parent stream", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to look up parent stream", nil)
	}
	if parentRef == 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("parent stream not found: %s", req.StreamID), nil)
	}
	if parentType != "composed" {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream %s is not a composed stream", req.StreamID), nil)
	}

	// Resolve all child stream refs (must exist in local storage).
	childRefs := make([]int64, n)
	for i := 0; i < n; i++ {
		ref, _, lookupErr := ext.dbLookupStreamRef(ctx, ext.nodeAddress, req.ChildStreamIDs[i])
		if lookupErr != nil {
			ext.logger.Error("failed to look up child stream", "error", lookupErr)
			return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to look up child stream", nil)
		}
		if ref == 0 {
			return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("child stream not found in local storage: %s", req.ChildStreamIDs[i]), nil)
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

	createdAt := ext.currentHeight()
	for i := 0; i < n; i++ {
		taxonomyID := generateTaxonomyID(ext.nodeAddress, req.StreamID, req.ChildStreamIDs[i], groupSeq, i)
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

// GetRecord queries records from a local stream (primitive or composed).
// Mirrors consensus get_record_primitive/get_record_composed:
//   - Both from/to nil: return latest record
//   - Gap-filling with anchor record
//   - Dedup by created_at DESC
//   - LIMIT 10000
func (ext *Extension) GetRecord(ctx context.Context, req *GetRecordRequest) (*GetRecordResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}
	if !ext.isEnabled.Load() {
		return nil, disabledError()
	}
	if authErr := ext.checkAuth(ctx, MethodGetRecord, req); authErr != nil {
		return nil, authErr
	}

	if err := validateStreamID(req.StreamID); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}

	// Validate time range
	if req.FromTime != nil && req.ToTime != nil && *req.FromTime > *req.ToTime {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "from_time must be <= to_time", nil)
	}

	ref, stype, err := ext.dbLookupStreamRef(ctx, ext.nodeAddress, req.StreamID)
	if err != nil {
		ext.logger.Error("failed to look up stream", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to look up stream", nil)
	}
	if ref == 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream not found: %s", req.StreamID), nil)
	}

	var records []RecordOutput
	switch stype {
	case "primitive":
		records, err = ext.dbGetRecordPrimitive(ctx, ref, req.FromTime, req.ToTime)
	case "composed":
		records, err = ext.dbGetRecordComposed(ctx, ref, req.FromTime, req.ToTime)
	default:
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, fmt.Sprintf("unknown stream type: %s", stype), nil)
	}
	if err != nil {
		ext.logger.Error("failed to query records", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to query records", nil)
	}

	if records == nil {
		records = []RecordOutput{}
	}

	return &GetRecordResponse{Records: records}, nil
}

// GetIndex queries computed index values from a local stream.
// Index = (value / base_value) * 100.
// Default base_time is the earliest event_time in the stream.
func (ext *Extension) GetIndex(ctx context.Context, req *GetIndexRequest) (*GetIndexResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}
	if !ext.isEnabled.Load() {
		return nil, disabledError()
	}
	if authErr := ext.checkAuth(ctx, MethodGetIndex, req); authErr != nil {
		return nil, authErr
	}

	if err := validateStreamID(req.StreamID); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}

	// Validate time range
	if req.FromTime != nil && req.ToTime != nil && *req.FromTime > *req.ToTime {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "from_time must be <= to_time", nil)
	}

	ref, stype, err := ext.dbLookupStreamRef(ctx, ext.nodeAddress, req.StreamID)
	if err != nil {
		ext.logger.Error("failed to look up stream", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to look up stream", nil)
	}
	if ref == 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream not found: %s", req.StreamID), nil)
	}

	// Resolve base_time: default to first event_time in the stream
	baseTime := req.BaseTime
	if baseTime == nil {
		if stype == "primitive" {
			firstET, lookupErr := ext.dbGetFirstEventTime(ctx, ref)
			if lookupErr != nil {
				ext.logger.Error("failed to get first event time", "error", lookupErr)
				return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to determine base time", nil)
			}
			if firstET == nil {
				return &GetIndexResponse{Records: []IndexOutput{}}, nil
			}
			baseTime = firstET
		} else {
			firstET, lookupErr := ext.dbGetFirstComposedEventTime(ctx, ref)
			if lookupErr != nil {
				ext.logger.Error("failed to get first composed event time", "error", lookupErr)
				return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to determine base time", nil)
			}
			if firstET == nil {
				return &GetIndexResponse{Records: []IndexOutput{}}, nil
			}
			baseTime = firstET
		}
	}

	// Get the base value at base_time — both branches use a single-row lookup
	// to avoid the 10000-row LIMIT prefix bug for long histories.
	var baseRecord *RecordOutput
	switch stype {
	case "primitive":
		baseRecord, err = ext.dbGetRecordAtOrBefore(ctx, ref, *baseTime)
	case "composed":
		baseRecord, err = ext.dbGetComposedRecordAtOrBefore(ctx, ref, *baseTime)
	}
	if err != nil {
		ext.logger.Error("failed to get base value", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to get base value", nil)
	}
	if baseRecord == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "no data at base time", nil)
	}

	baseValue, parseErr := decimal.NewFromString(baseRecord.Value)
	if parseErr != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to parse base value", nil)
	}
	if baseValue.IsZero() {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "base value is 0", nil)
	}

	// Get all records for the requested range
	var records []RecordOutput
	switch stype {
	case "primitive":
		records, err = ext.dbGetRecordPrimitive(ctx, ref, req.FromTime, req.ToTime)
	case "composed":
		records, err = ext.dbGetRecordComposed(ctx, ref, req.FromTime, req.ToTime)
	}
	if err != nil {
		ext.logger.Error("failed to query records for index", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to query records", nil)
	}

	// Calculate index: (value / base_value) * 100
	hundred := decimal.NewFromInt(100)
	indexRecords := make([]IndexOutput, 0, len(records))
	for _, r := range records {
		val, valErr := decimal.NewFromString(r.Value)
		if valErr != nil {
			continue
		}
		indexed := val.Mul(hundred).Div(baseValue)
		indexRecords = append(indexRecords, IndexOutput{
			EventTime: r.EventTime,
			Value:     indexed.StringFixed(18),
		})
	}

	return &GetIndexResponse{Records: indexRecords}, nil
}

// DeleteStream removes a local stream and all associated data (records, taxonomies).
// Mirrors consensus delete_stream: ON DELETE CASCADE removes child rows.
func (ext *Extension) DeleteStream(ctx context.Context, req *DeleteStreamRequest) (*DeleteStreamResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}
	if !ext.isEnabled.Load() {
		return nil, disabledError()
	}
	if authErr := ext.checkAuth(ctx, MethodDeleteStream, req); authErr != nil {
		return nil, authErr
	}

	if err := validateStreamID(req.StreamID); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}

	deleted, err := ext.dbDeleteStream(ctx, ext.nodeAddress, req.StreamID)
	if err != nil {
		ext.logger.Error("failed to delete local stream", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to delete stream", nil)
	}
	if !deleted {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream not found: %s", req.StreamID), nil)
	}

	return &DeleteStreamResponse{}, nil
}

// DisableTaxonomy disables a taxonomy group on a local composed stream.
// Mirrors consensus disable_taxonomy: sets disabled_at = current block height.
func (ext *Extension) DisableTaxonomy(ctx context.Context, req *DisableTaxonomyRequest) (*DisableTaxonomyResponse, *jsonrpc.Error) {
	if req == nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "missing request", nil)
	}
	if !ext.isEnabled.Load() {
		return nil, disabledError()
	}
	if authErr := ext.checkAuth(ctx, MethodDisableTaxonomy, req); authErr != nil {
		return nil, authErr
	}

	if err := validateStreamID(req.StreamID); err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}
	if req.GroupSequence < 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, "group_sequence must be >= 0", nil)
	}

	// Verify stream exists and is composed.
	ref, stype, err := ext.dbLookupStreamRef(ctx, ext.nodeAddress, req.StreamID)
	if err != nil {
		ext.logger.Error("failed to look up stream", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to look up stream", nil)
	}
	if ref == 0 {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream not found: %s", req.StreamID), nil)
	}
	if stype != "composed" {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, fmt.Sprintf("stream %s is not a composed stream", req.StreamID), nil)
	}

	disabled, err := ext.dbDisableTaxonomy(ctx, ref, req.GroupSequence)
	if err != nil {
		ext.logger.Error("failed to disable taxonomy", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to disable taxonomy", nil)
	}
	if !disabled {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams,
			fmt.Sprintf("taxonomy group %d not found or already disabled on stream %s", req.GroupSequence, req.StreamID), nil)
	}

	return &DisableTaxonomyResponse{}, nil
}

// ListStreams lists all local streams owned by this node.
func (ext *Extension) ListStreams(ctx context.Context, req *ListStreamsRequest) (*ListStreamsResponse, *jsonrpc.Error) {
	if !ext.isEnabled.Load() {
		return nil, disabledError()
	}
	if authErr := ext.checkAuth(ctx, MethodListStreams, req); authErr != nil {
		return nil, authErr
	}
	streams, err := ext.dbListStreams(ctx)
	if err != nil {
		ext.logger.Error("failed to list streams", "error", err)
		return nil, jsonrpc.NewError(jsonrpc.ErrorInternal, "failed to list streams", nil)
	}

	if streams == nil {
		streams = []StreamInfo{}
	}

	return &ListStreamsResponse{Streams: streams}, nil
}
