/**
 * get_composed_stream_data: Placeholder for getting composed stream data.
 * Will get all primitive streams involved in a composed stream query, and get their data.
 * To get all the primitive streams, it uses taxonomies table, which we can't forget that has
 * start and end times for taxonomies.
 * But for simplicity, we will assume that any stream in that period to be queried for the whole duration.
 *
 * About data fetching:
 * - we try to get data in the period asked, but also one record before if there's no data in that exact lower bound.
 *
 * The query will return a table with the following columns:
 * - event_time: The timestamp of the composed stream data.
 * - value: The value of the composed stream data.
 * - stream_id: The id of the stream.
 * - data_provider: The data provider of the stream.

 * About the query:
 * - If there's no upper bound, it means no upper time filter is applied
 * - If there's no lower bound, it means no lower time filter is applied
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
    -- 1) Defaults
    ---------------------------------------------------------------------------
    IF $frozen_at IS NULL {
        $frozen_at := 0;
    }

    RETURN WITH RECURSIVE

    ----------------------------------------------------------------------------
    -- A) Identify relevant taxonomy via “anchor approach”, so we only pick substreams
    --    that apply from that anchor up to $to.
    ----------------------------------------------------------------------------
    anchor_taxonomy AS (
        SELECT COALESCE(
            (
                -- If we have a version <= $from, pick the greatest
                SELECT MAX(start_time)
                FROM taxonomies
                WHERE data_provider = $data_provider
                  AND stream_id     = $stream_id
                  AND disabled_at IS NULL
                  AND start_time <= $from
            ),
            (
                -- Otherwise pick the earliest version after $from (if $from is not null)
                SELECT MIN(start_time)
                FROM taxonomies
                WHERE data_provider = $data_provider
                  AND stream_id     = $stream_id
                  AND disabled_at IS NULL
                  AND $from IS NOT NULL
            )
        ) AS anchor_time
    ),
    relevant_taxonomies AS (
        SELECT t.*
        FROM taxonomies t
        JOIN anchor_taxonomy a
          ON t.start_time >= a.anchor_time
        WHERE t.data_provider = $data_provider
          AND t.stream_id    = $stream_id
          AND t.disabled_at IS NULL
          AND ($to IS NULL OR t.start_time <= $to)
    ),

    ----------------------------------------------------------------------------
    -- B) Recursively gather substreams, then filter to "primitive" leaves.
    ----------------------------------------------------------------------------
    all_substreams AS (
        -- anchor: the parent
        SELECT s.data_provider, s.stream_id
        FROM streams s
        WHERE s.data_provider = $data_provider
          AND s.stream_id     = $stream_id

        UNION

        -- children
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
    -- C) For each primitive substream, find:
    --    1) A single anchor event_time <= $to  (if $from is not null)
    --    2) All “future” event_times in ($from, $to] (discrete integer times)
    ----------------------------------------------------------------------------

    -- 1) anchor_events: for each substream, the single greatest event_time <= $from
    --    We only pick the event_time, ignoring the actual "value" for now.
    anchor_events AS (
        SELECT
            ps.data_provider,
            ps.stream_id,
            MAX(pe.event_time) AS anchor_et
        FROM primitive_substreams ps
        JOIN primitive_events pe
          ON pe.data_provider = ps.data_provider
         AND pe.stream_id    = ps.stream_id
        WHERE 
            $from IS NOT NULL
            AND pe.event_time <= $from
            AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
        GROUP BY ps.data_provider, ps.stream_id
    ),

    -- 2) future_events: all event_times strictly greater than $from, up to $to
    future_events AS (
        SELECT DISTINCT
            ps.data_provider,
            ps.stream_id,
            pe.event_time
        FROM primitive_substreams ps
        JOIN primitive_events pe
          ON pe.data_provider = ps.data_provider
         AND pe.stream_id    = ps.stream_id
        WHERE 
            ($frozen_at = 0 OR pe.created_at <= $frozen_at)
            AND pe.event_time > $from
            AND ($to IS NULL OR pe.event_time <= $to)
    ),

    ----------------------------------------------------------------------------
    -- D) Combine the anchor time + future times to get "effective events" to consider.
    ----------------------------------------------------------------------------
    effective_events AS (
        -- 1) anchor if it exists
        SELECT
            ae.data_provider,
            ae.stream_id,
            ae.anchor_et AS event_time
        FROM anchor_events ae
        WHERE ae.anchor_et IS NOT NULL

        UNION

        -- 2) all future times
        SELECT
            fe.data_provider,
            fe.stream_id,
            fe.event_time
        FROM future_events fe
    ),

    ----------------------------------------------------------------------------
    -- E) Now for each (data_provider, stream_id, event_time) in effective_events,
    --    pick the *latest* record by created_at. This is like ROW_NUMBER partition logic.
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

    ----------------------------------------------------------------------------
    -- F) Filter to rn = 1 => the single best record per event_time
    ----------------------------------------------------------------------------
    final_candidates AS (
        SELECT
            rc.data_provider,
            rc.stream_id,
            rc.event_time,
            rc.value
        FROM raw_candidates rc
        WHERE rc.rn = 1
    ),

    ----------------------------------------------------------------------------
    -- G) Separate anchor from "true in-range" times to replicate the "gap fill" logic
    --    The anchor event_time might be strictly < $from, or exactly = $from
    ----------------------------------------------------------------------------
    anchor_results AS (
        SELECT
            fc.data_provider,
            fc.stream_id,
            fc.event_time,
            fc.value
        FROM final_candidates fc
        JOIN anchor_events ae
          ON ae.data_provider = fc.data_provider
         AND ae.stream_id    = fc.stream_id
         AND ae.anchor_et    = fc.event_time
    ),
    in_range_results AS (
        SELECT
            fc.data_provider,
            fc.stream_id,
            fc.event_time,
            fc.value
        FROM final_candidates fc
        WHERE ($from IS NULL OR fc.event_time >= $from)
    ),

    ----------------------------------------------------------------------------
    -- H) Decide whether to include the anchor row. It's included if:
    --   1) There's no in-range record at all, or
    --   2) The earliest in-range event_time is strictly > anchor's event_time
    ----------------------------------------------------------------------------
    combined_results AS (
        SELECT a.*
        FROM anchor_results a
        WHERE
            (
                -- No in-range record
                (SELECT COUNT(*) FROM in_range_results r
                 WHERE r.data_provider = a.data_provider
                   AND r.stream_id    = a.stream_id
                ) = 0
            )
            OR
            (
                -- earliest in-range event_time is strictly > anchor
                (SELECT MIN(r2.event_time) FROM in_range_results r2
                 WHERE r2.data_provider = a.data_provider
                   AND r2.stream_id    = a.stream_id
                ) > a.event_time
            )

        UNION ALL

        SELECT * FROM in_range_results
    )

    ----------------------------------------------------------------------------
    -- I) Final output
    ----------------------------------------------------------------------------
    SELECT
        cr.event_time,
        cr.value,
        cr.stream_id,
        cr.data_provider
    FROM combined_results cr;
};
