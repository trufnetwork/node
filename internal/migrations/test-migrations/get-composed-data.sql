/**
 * get_composed_stream_data: Returns event-time data from all "primitive" child streams
 * that are active under a composed parent stream in the specified [from..to] window.
 *
 * Motivation & Key Behaviors:
 *  1. We use a "taxonomy group_sequence" approach to determine which substreams are active
 *     at any point overlapping [from..to]. If `from` is NULL, we include all group_sequences
 *     from the earliest start_time; if `to` is NULL, no upper cutoff applies.
 *  2. We retrieve:
 *       - One "anchor" record if it exists at or below `from` (to fill in a gap).
 *       - All actual events in (from..to].
 *  3. If multiple records exist at the same (stream, event_time), we take only the
 *     "latest" by `created_at` (group_sequence dimension).
 *  4. The final result includes anchor rows only if there's no actual event at `from`
 *     or no event in-range at all for that substream.
 */

CREATE OR REPLACE ACTION get_composed_stream_data(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PUBLIC VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18),
    stream_id TEXT,
    data_provider TEXT
) {
    ---------------------------------------------------------------------------
    -- 1) Defaults & Basic Checks
    ---------------------------------------------------------------------------
    IF $frozen_at IS NULL {
        $frozen_at := 0;
    }
    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        error(format('from: %s > to: %s', $from, $to));
    }

    RETURN WITH RECURSIVE

    ----------------------------------------------------------------------------
    -- A) Pick the "anchor_time" to locate the earliest relevant taxonomy group_sequence
    ----------------------------------------------------------------------------
    anchor_taxonomy AS (
        SELECT CASE
            WHEN $from IS NULL THEN
                -- No lower bound => start from the earliest taxonomy group_sequence
                (SELECT MIN(start_time)
                 FROM taxonomies
                 WHERE data_provider = $data_provider
                   AND stream_id     = $stream_id
                   AND disabled_at IS NULL)
            ELSE
                -- If there's a lower bound, pick the latest group_sequence <= from,
                -- or fallback to the earliest group_sequence if none qualifies
                COALESCE(
                  (SELECT MAX(start_time)
                   FROM taxonomies
                   WHERE data_provider = $data_provider
                     AND stream_id     = $stream_id
                     AND disabled_at IS NULL
                     AND start_time <= $from),
                  (SELECT MIN(start_time)
                   FROM taxonomies
                   WHERE data_provider = $data_provider
                     AND stream_id     = $stream_id
                     AND disabled_at IS NULL
                     AND $from IS NOT NULL)
                )
        END AS anchor_time
    ),

    ----------------------------------------------------------------------------
    -- B) Find all taxonomy group_sequences from that anchor_time up to $to
    ----------------------------------------------------------------------------
    relevant_taxonomies AS (
        SELECT t.*
        FROM taxonomies t
        JOIN anchor_taxonomy a
          ON t.start_time >= a.anchor_time
        WHERE t.data_provider = $data_provider
          AND t.stream_id     = $stream_id
          AND t.disabled_at IS NULL
          AND ($to IS NULL OR t.start_time <= $to)
    ),

    ----------------------------------------------------------------------------
    -- C) Recursively gather substreams (including nested children).
    --    Then restrict to "primitive" leaves that store actual events.
    ----------------------------------------------------------------------------
    all_substreams AS (
        SELECT s.data_provider, s.stream_id
        FROM streams s
        WHERE s.data_provider = $data_provider
          AND s.stream_id     = $stream_id

        UNION

        SELECT rt.child_data_provider, rt.child_stream_id
        FROM relevant_taxonomies rt
        JOIN all_substreams parent
          ON parent.data_provider = rt.data_provider
         AND parent.stream_id    = rt.stream_id
    ),
    primitive_substreams AS (
        SELECT asb.data_provider, asb.stream_id
        FROM all_substreams asb
        JOIN streams s
          ON s.data_provider = asb.data_provider
         AND s.stream_id    = asb.stream_id
        WHERE s.stream_type = 'primitive'
    ),

    ----------------------------------------------------------------------------
    -- D) Identify anchor row (if any) and future events:
    --    anchor_events => single greatest time <= from
    --    future_events => distinct times > from and <= to
    ----------------------------------------------------------------------------
    anchor_events AS (
        SELECT
            ps.data_provider,
            ps.stream_id,
            MAX(pe.event_time) AS anchor_et
        FROM primitive_substreams ps
        JOIN primitive_events pe
          ON pe.data_provider = ps.data_provider
         AND pe.stream_id    = ps.stream_id
        WHERE $from IS NOT NULL
          AND pe.event_time <= $from
          AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
        GROUP BY ps.data_provider, ps.stream_id
    ),
    future_events AS (
        SELECT DISTINCT
            ps.data_provider,
            ps.stream_id,
            pe.event_time
        FROM primitive_substreams ps
        JOIN primitive_events pe
          ON pe.data_provider = ps.data_provider
         AND pe.stream_id    = ps.stream_id
        WHERE ($frozen_at = 0 OR pe.created_at <= $frozen_at)
          AND ($from IS NULL OR pe.event_time > $from)
          AND ($to   IS NULL OR pe.event_time <= $to)
    ),

    ----------------------------------------------------------------------------
    -- E) Merge anchor + future => "effective_events"
    ----------------------------------------------------------------------------
    effective_events AS (
        SELECT data_provider, stream_id, anchor_et AS event_time
        FROM anchor_events
        WHERE anchor_et IS NOT NULL

        UNION

        SELECT data_provider, stream_id, event_time
        FROM future_events
    ),

    ----------------------------------------------------------------------------
    -- F) For each (stream, event_time), pick the most recent record by created_at
    ----------------------------------------------------------------------------
    raw_candidates AS (
        SELECT
            pe.data_provider,
            pe.stream_id,
            pe.event_time,
            pe.value,
            ROW_NUMBER() OVER (
                PARTITION BY pe.data_provider, pe.stream_id, pe.event_time
                ORDER BY pe.created_at DESC
            ) AS rn
        FROM primitive_events pe
        JOIN effective_events ee
          ON ee.data_provider = pe.data_provider
         AND ee.stream_id    = pe.stream_id
         AND ee.event_time   = pe.event_time
        WHERE ($frozen_at = 0 OR pe.created_at <= $frozen_at)
    ),
    final_candidates AS (
        SELECT data_provider, stream_id, event_time, value
        FROM raw_candidates
        WHERE rn = 1
    ),

    ----------------------------------------------------------------------------
    -- G) Apply gap-fill logic: anchor rows vs. true in-range rows
    ----------------------------------------------------------------------------
    anchor_results AS (
        SELECT fc.*
        FROM final_candidates fc
        JOIN anchor_events ae
          ON fc.data_provider = ae.data_provider
         AND fc.stream_id    = ae.stream_id
         AND fc.event_time   = ae.anchor_et
    ),
    in_range_results AS (
        SELECT fc.*
        FROM final_candidates fc
        WHERE ($from IS NULL OR fc.event_time >= $from)
    ),

    ----------------------------------------------------------------------------
    -- H) Include anchor rows if there's no overlapping in-range event at the same time,
    --    or if that substream has no in-range events at all.
    ----------------------------------------------------------------------------
    combined_results AS (
        SELECT a.*
        FROM anchor_results a
        WHERE
            (
                SELECT COUNT(*) FROM in_range_results r
                 WHERE r.data_provider = a.data_provider
                   AND r.stream_id    = a.stream_id
            ) = 0
            OR
            (
                SELECT MIN(r2.event_time) FROM in_range_results r2
                 WHERE r2.data_provider = a.data_provider
                   AND r2.stream_id    = a.stream_id
            ) > a.event_time

        UNION ALL

        SELECT * FROM in_range_results
    )

    ----------------------------------------------------------------------------
    -- I) Return final sorted results
    ----------------------------------------------------------------------------
    SELECT
        cr.event_time,
        cr.value,
        cr.stream_id,
        cr.data_provider
    FROM combined_results cr;
};
