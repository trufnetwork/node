package tn_local

import (
	"context"
	"fmt"

	"github.com/trufnetwork/kwil-db/node/types/sql"
)

const maxQueryResults = 10000
const maxInt8 = int64(9223372036854775000)

// dbGetRecordPrimitive retrieves time series data for a primitive stream.
// Mirrors consensus get_record_primitive (005-primitive-query.sql):
// - Gap-fills with anchor record (last value at or before $from)
// - Deduplicates by created_at DESC (latest version wins)
// - LIMIT 10000 to prevent unbounded results
func (ext *Extension) dbGetRecordPrimitive(ctx context.Context, streamRef int64, from, to *int64) ([]RecordOutput, error) {
	// Both nil: return latest record (consensus: get_last_record_primitive)
	if from == nil && to == nil {
		return ext.dbGetLastRecordPrimitive(ctx, streamRef)
	}

	effectiveFrom := int64(0)
	if from != nil {
		effectiveFrom = *from
	}
	effectiveTo := maxInt8
	if to != nil {
		effectiveTo = *to
	}

	query := fmt.Sprintf(`
		WITH
		-- Get records within time range, dedup by latest created_at
		interval_records AS (
			SELECT
				pe.event_time,
				pe.value,
				ROW_NUMBER() OVER (
					PARTITION BY pe.event_time
					ORDER BY pe.created_at DESC
				) as rn
			FROM %[1]s.primitive_events pe
			WHERE pe.stream_ref = $1
				AND pe.event_time > $2
				AND pe.event_time <= $3
		),
		-- Anchor: last value at or before $from for gap-filling
		anchor_record AS (
			SELECT pe.event_time, pe.value
			FROM %[1]s.primitive_events pe
			WHERE pe.stream_ref = $1
				AND pe.event_time <= $2
			ORDER BY pe.event_time DESC, pe.created_at DESC
			LIMIT 1
		),
		combined_results AS (
			SELECT event_time, value FROM anchor_record
			UNION ALL
			SELECT event_time, value FROM interval_records WHERE rn = 1
		)
		SELECT event_time, value::TEXT FROM combined_results
		ORDER BY event_time ASC
		LIMIT %[2]d`, SchemaName, maxQueryResults)

	rs, err := ext.db.Execute(ctx, query, streamRef, effectiveFrom, effectiveTo)
	if err != nil {
		return nil, fmt.Errorf("query primitive records: %w", err)
	}

	return resultSetToRecords(rs)
}

// dbGetLastRecordPrimitive returns the most recent record for a primitive stream.
// Mirrors consensus get_last_record_primitive (005-primitive-query.sql).
func (ext *Extension) dbGetLastRecordPrimitive(ctx context.Context, streamRef int64) ([]RecordOutput, error) {
	query := fmt.Sprintf(`
		SELECT pe.event_time, pe.value::TEXT
		FROM %s.primitive_events pe
		WHERE pe.stream_ref = $1
		ORDER BY pe.event_time DESC, pe.created_at DESC
		LIMIT 1`, SchemaName)

	rs, err := ext.db.Execute(ctx, query, streamRef)
	if err != nil {
		return nil, fmt.Errorf("query last primitive record: %w", err)
	}

	return resultSetToRecords(rs)
}

// dbGetFirstEventTime returns the earliest event_time for a stream.
// Used as the default base_time for index calculations.
func (ext *Extension) dbGetFirstEventTime(ctx context.Context, streamRef int64) (*int64, error) {
	query := fmt.Sprintf(`
		SELECT pe.event_time
		FROM %s.primitive_events pe
		WHERE pe.stream_ref = $1
		ORDER BY pe.event_time ASC, pe.created_at DESC
		LIMIT 1`, SchemaName)

	rs, err := ext.db.Execute(ctx, query, streamRef)
	if err != nil {
		return nil, fmt.Errorf("query first event time: %w", err)
	}
	if len(rs.Rows) == 0 {
		return nil, nil
	}

	et, ok := toInt64Val(rs.Rows[0][0])
	if !ok {
		return nil, fmt.Errorf("unexpected event_time type: %T", rs.Rows[0][0])
	}
	return &et, nil
}

// dbListStreams returns all local streams.
func (ext *Extension) dbListStreams(ctx context.Context) ([]StreamInfo, error) {
	query := fmt.Sprintf(`
		SELECT data_provider, stream_id, stream_type, created_at
		FROM %s.streams
		ORDER BY created_at ASC, data_provider ASC, stream_id ASC`, SchemaName)

	rs, err := ext.db.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list streams: %w", err)
	}

	streams := make([]StreamInfo, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		dp, _ := row[0].(string)
		sid, _ := row[1].(string)
		stype, _ := row[2].(string)
		createdAt, _ := toInt64Val(row[3])
		streams = append(streams, StreamInfo{
			DataProvider: dp,
			StreamID:     sid,
			StreamType:   stype,
			CreatedAt:    createdAt,
		})
	}
	return streams, nil
}

// resultSetToRecords converts a SQL ResultSet with (event_time, value::TEXT) rows to RecordOutput slice.
func resultSetToRecords(rs *sql.ResultSet) ([]RecordOutput, error) {
	if rs == nil || len(rs.Rows) == 0 {
		return nil, nil
	}

	records := make([]RecordOutput, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		if len(row) < 2 {
			continue
		}
		et, ok := toInt64Val(row[0])
		if !ok {
			return nil, fmt.Errorf("unexpected event_time type: %T", row[0])
		}
		val, ok := row[1].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type: %T", row[1])
		}
		records = append(records, RecordOutput{
			EventTime: et,
			Value:     val,
		})
	}
	return records, nil
}
