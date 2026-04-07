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

// dbGetRecordAtOrBefore returns the single most recent record at or before the given time.
// Used for base value lookup in index calculations — avoids fetching a large prefix
// which could hit LIMIT 10000 and return wrong last element.
func (ext *Extension) dbGetRecordAtOrBefore(ctx context.Context, streamRef int64, atOrBefore int64) (*RecordOutput, error) {
	query := fmt.Sprintf(`
		SELECT pe.event_time, pe.value::TEXT
		FROM %s.primitive_events pe
		WHERE pe.stream_ref = $1
			AND pe.event_time <= $2
		ORDER BY pe.event_time DESC, pe.created_at DESC
		LIMIT 1`, SchemaName)

	rs, err := ext.db.Execute(ctx, query, streamRef, atOrBefore)
	if err != nil {
		return nil, fmt.Errorf("query record at or before %d: %w", atOrBefore, err)
	}
	if rs == nil || len(rs.Rows) == 0 {
		return nil, nil
	}

	records, err := resultSetToRecords(rs)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	return &records[0], nil
}

// composedQueryCTEs returns the shared CTE prefix used by all composed-stream
// queries. The prefix produces a `weighted_avg(event_time, value)` relation
// containing one row per change-point (leaf primitive event or taxonomy
// boundary) with the LOCF-filled weighted average across all active leaves.
//
// Callers append their own SELECT clause referencing `weighted_avg` and pass
// $1 = parent stream_ref (additional placeholders are caller-defined).
//
// Mirrors consensus 006-composed-query.sql for correct overshadowing:
//   - parent_next_starts: LEAD over per-PARENT distinct start_times so children
//     removed in later group_sequences are properly closed at the next parent
//     boundary (NOT per-child, which would leave removed children open forever).
//   - all_times: UNION of (a) leaf events filtered to their active windows
//     and (b) taxonomy boundary times, so weight changes that don't coincide
//     with a leaf event still produce a result row.
func composedQueryCTEs() string {
	return fmt.Sprintf(`
		WITH RECURSIVE
		-- Step 1a: distinct start_times per parent (basis for LEAD boundary calc)
		parent_distinct_starts AS (
			SELECT DISTINCT stream_ref, start_time
			FROM %[1]s.taxonomies
			WHERE disabled_at IS NULL
		),
		-- Step 1b: next start_time per parent — closes out children removed in
		-- subsequent group_sequences, regardless of whether the same child reappears.
		parent_next_starts AS (
			SELECT
				stream_ref,
				start_time,
				LEAD(start_time) OVER (PARTITION BY stream_ref ORDER BY start_time) AS next_start_time
			FROM parent_distinct_starts
		),
		-- Step 1c: max group_sequence per (parent, start_time) — overshadowing winner.
		max_gs_per_start AS (
			SELECT stream_ref, start_time, MAX(group_sequence) AS max_gs
			FROM %[1]s.taxonomies
			WHERE disabled_at IS NULL
			GROUP BY stream_ref, start_time
		),
		-- Step 2: Active taxonomy = winning rows joined to their parent's next boundary.
		active_taxonomy AS (
			SELECT
				t.stream_ref AS parent_ref,
				t.child_stream_ref,
				t.weight,
				t.start_time,
				COALESCE(pns.next_start_time, %[2]d) - 1 AS end_time
			FROM %[1]s.taxonomies t
			JOIN max_gs_per_start mgs
				ON t.stream_ref = mgs.stream_ref
				AND t.start_time = mgs.start_time
				AND t.group_sequence = mgs.max_gs
			JOIN parent_next_starts pns
				ON t.stream_ref = pns.stream_ref
				AND t.start_time = pns.start_time
			WHERE t.disabled_at IS NULL
		),
		-- Step 3: Recursive hierarchy — expand to leaf primitives with cumulative weights.
		hierarchy AS (
			SELECT
				at.child_stream_ref AS leaf_ref,
				at.weight AS cumulative_weight,
				at.start_time AS path_start,
				at.end_time AS path_end,
				1 AS level
			FROM active_taxonomy at
			WHERE at.parent_ref = $1

			UNION ALL

			SELECT
				at.child_stream_ref,
				(h.cumulative_weight * at.weight)::NUMERIC(36,18),
				GREATEST(h.path_start, at.start_time),
				LEAST(h.path_end, at.end_time),
				h.level + 1
			FROM hierarchy h
			JOIN active_taxonomy at ON at.parent_ref = h.leaf_ref
			WHERE h.level < 100
				AND GREATEST(h.path_start, at.start_time) <= LEAST(h.path_end, at.end_time)
		),
		leaf_primitives AS (
			SELECT leaf_ref, cumulative_weight, path_start, path_end
			FROM hierarchy h
			WHERE NOT EXISTS (
				SELECT 1 FROM active_taxonomy at2 WHERE at2.parent_ref = h.leaf_ref
			)
		),
		-- Step 4: Get all primitive events for active leaves, deduped by latest created_at.
		leaf_events AS (
			SELECT
				pe.stream_ref, pe.event_time, pe.value,
				ROW_NUMBER() OVER (
					PARTITION BY pe.stream_ref, pe.event_time
					ORDER BY pe.created_at DESC
				) AS rn
			FROM %[1]s.primitive_events pe
			WHERE pe.stream_ref IN (SELECT DISTINCT leaf_ref FROM leaf_primitives)
		),
		deduped_events AS (
			SELECT stream_ref, event_time, value FROM leaf_events WHERE rn = 1
		),
		-- Step 5: All change-points = leaf events restricted to their active window
		--         UNION taxonomy boundary times. Mirrors consensus cleaned_event_times
		--         so weight changes between leaf events still emit a result row.
		all_times AS (
			SELECT DISTINCT event_time FROM (
				SELECT de.event_time
				FROM deduped_events de
				JOIN leaf_primitives lp ON de.stream_ref = lp.leaf_ref
				WHERE de.event_time >= lp.path_start
					AND de.event_time <= lp.path_end
				UNION ALL
				SELECT path_start AS event_time FROM leaf_primitives
			) combined
		),
		-- Step 6: At each change-point, find each leaf's LOCF value.
		time_leaf_values AS (
			SELECT
				t.event_time,
				lp.leaf_ref,
				lp.cumulative_weight,
				lp.path_start,
				lp.path_end,
				(
					SELECT de.value FROM deduped_events de
					WHERE de.stream_ref = lp.leaf_ref
						AND de.event_time <= t.event_time
					ORDER BY de.event_time DESC
					LIMIT 1
				) AS locf_value
			FROM all_times t
			CROSS JOIN leaf_primitives lp
		),
		-- Step 7: Weighted average = SUM(weight * value) / SUM(weight) over active leaves.
		weighted_avg AS (
			SELECT
				event_time,
				CASE
					WHEN SUM(CASE WHEN locf_value IS NOT NULL
						AND event_time >= path_start AND event_time <= path_end
						THEN cumulative_weight ELSE 0::NUMERIC(36,18) END) = 0::NUMERIC(36,18)
					THEN NULL
					ELSE (
						SUM(CASE WHEN locf_value IS NOT NULL
							AND event_time >= path_start AND event_time <= path_end
							THEN cumulative_weight * locf_value ELSE 0::NUMERIC(36,18) END)
						/
						SUM(CASE WHEN locf_value IS NOT NULL
							AND event_time >= path_start AND event_time <= path_end
							THEN cumulative_weight ELSE 0::NUMERIC(36,18) END)
					)::NUMERIC(36,18)
				END AS value
			FROM time_leaf_values
			GROUP BY event_time
			HAVING SUM(CASE WHEN locf_value IS NOT NULL
				AND event_time >= path_start AND event_time <= path_end
				THEN 1 ELSE 0 END) > 0
		)`, SchemaName, maxInt8)
}

// dbGetRecordComposed retrieves the calculated time series for a composed stream
// over an explicit range, using anchor + interval semantics like the primitive query.
func (ext *Extension) dbGetRecordComposed(ctx context.Context, streamRef int64, from, to *int64) ([]RecordOutput, error) {
	// Both nil: return latest record (avoids the LIMIT-prefix bug for long histories).
	if from == nil && to == nil {
		return ext.dbGetLastRecordComposed(ctx, streamRef)
	}

	effectiveFrom := int64(0)
	if from != nil {
		effectiveFrom = *from
	}
	effectiveTo := maxInt8
	if to != nil {
		effectiveTo = *to
	}

	query := composedQueryCTEs() + fmt.Sprintf(`,
		anchor AS (
			SELECT event_time, value FROM weighted_avg
			WHERE event_time <= $2 AND value IS NOT NULL
			ORDER BY event_time DESC LIMIT 1
		),
		range_results AS (
			SELECT event_time, value FROM anchor
			UNION ALL
			SELECT event_time, value FROM weighted_avg
			WHERE event_time > $2 AND event_time <= $3 AND value IS NOT NULL
		)
		SELECT DISTINCT event_time, value::TEXT FROM range_results
		ORDER BY event_time ASC
		LIMIT %d`, maxQueryResults)

	rs, err := ext.db.Execute(ctx, query, streamRef, effectiveFrom, effectiveTo)
	if err != nil {
		return nil, fmt.Errorf("query composed records: %w", err)
	}

	return resultSetToRecords(rs)
}

// dbGetLastRecordComposed returns the most recent calculated value for a composed
// stream via a single-row DESC query — does NOT pull the full history through
// the 10000-row prefix (which would return the wrong record for long histories).
func (ext *Extension) dbGetLastRecordComposed(ctx context.Context, streamRef int64) ([]RecordOutput, error) {
	query := composedQueryCTEs() + `
		SELECT event_time, value::TEXT FROM weighted_avg
		WHERE value IS NOT NULL
		ORDER BY event_time DESC
		LIMIT 1`

	rs, err := ext.db.Execute(ctx, query, streamRef)
	if err != nil {
		return nil, fmt.Errorf("query last composed record: %w", err)
	}
	return resultSetToRecords(rs)
}

// dbGetComposedRecordAtOrBefore returns the single most recent composed record
// at or before the given time. Mirrors dbGetRecordAtOrBefore for primitives —
// avoids the 10000-row prefix bug when history exceeds maxQueryResults.
func (ext *Extension) dbGetComposedRecordAtOrBefore(ctx context.Context, streamRef int64, atOrBefore int64) (*RecordOutput, error) {
	query := composedQueryCTEs() + `
		SELECT event_time, value::TEXT FROM weighted_avg
		WHERE value IS NOT NULL AND event_time <= $2
		ORDER BY event_time DESC
		LIMIT 1`

	rs, err := ext.db.Execute(ctx, query, streamRef, atOrBefore)
	if err != nil {
		return nil, fmt.Errorf("query composed record at or before %d: %w", atOrBefore, err)
	}
	if rs == nil || len(rs.Rows) == 0 {
		return nil, nil
	}
	records, err := resultSetToRecords(rs)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	return &records[0], nil
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
	for i, row := range rs.Rows {
		if len(row) < 4 {
			return nil, fmt.Errorf("list streams: row %d has %d columns, expected 4", i, len(row))
		}
		dp, ok := row[0].(string)
		if !ok {
			return nil, fmt.Errorf("list streams: row %d: unexpected data_provider type: %T", i, row[0])
		}
		sid, ok := row[1].(string)
		if !ok {
			return nil, fmt.Errorf("list streams: row %d: unexpected stream_id type: %T", i, row[1])
		}
		stype, ok := row[2].(string)
		if !ok {
			return nil, fmt.Errorf("list streams: row %d: unexpected stream_type type: %T", i, row[2])
		}
		createdAt, ok := toInt64Val(row[3])
		if !ok {
			return nil, fmt.Errorf("list streams: row %d: unexpected created_at type: %T", i, row[3])
		}
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
	for i, row := range rs.Rows {
		if len(row) < 2 {
			return nil, fmt.Errorf("row %d has %d columns, expected at least 2", i, len(row))
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
