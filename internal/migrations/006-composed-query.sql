/**
 * get_record_composed: Placeholder for composed stream data retrieval.
 * Will aggregate child stream values based on taxonomy weights.
 */
CREATE OR REPLACE ACTION get_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PRIVATE VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
)  {
    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('from: %s > to: %s', $from, $to));
    }

    RETURN WITH RECURSIVE

    -- get the taxonomy versions for the time range
    selected_taxonomy_versions AS (
        SELECT 
            t.data_provider,
            t.stream_id,
            t.start_time,
            t.version,
            ROW_NUMBER() OVER (PARTITION BY t.start_time ORDER BY t.version DESC) AS rn
        FROM taxonomies t
        WHERE t.disabled_at IS NULL
          AND t.data_provider = $data_provider
          AND t.stream_id = $stream_id
          AND (
              -- for anchor taxonomy (before or at $from)
              ($from IS NOT NULL AND t.start_time = (
                  SELECT MAX(start_time)
                  FROM taxonomies
                  WHERE data_provider = $data_provider
                      AND stream_id = $stream_id
                      AND disabled_at IS NULL
                      AND start_time <= $from
              ))
              OR
              -- for future taxonomies (after $from and before or at $to)
              ($from IS NULL OR t.start_time > $from)
              AND ($to IS NULL OR t.start_time <= $to)
          )
    ),

    -- Select latest version for each start_time
    latest_versions AS (
        SELECT
            data_provider,
            stream_id,
            start_time,
            version
        FROM selected_taxonomy_versions
        WHERE rn = 1
    ),

    -- Get all taxonomy entries for the selected versions
    all_taxonomies AS (
        SELECT
            t.data_provider,
            t.stream_id,
            t.start_time AS version_start,
            t.weight,
            t.version,
            t.child_data_provider,
            t.child_stream_id
        FROM taxonomies t
        JOIN latest_versions lv
          ON t.data_provider = lv.data_provider
         AND t.stream_id = lv.stream_id
         AND t.start_time = lv.start_time
         AND t.version = lv.version
        WHERE t.disabled_at IS NULL
    ),

    main_versions AS (
        SELECT 
            data_provider,
            stream_id,
            version_start,
            COALESCE(
                LEAD(version_start) OVER (
                    PARTITION BY data_provider, stream_id
                    ORDER BY version_start
                ) - 1,
                9223372036854775000
            ) AS version_end,
            SUM(weight) AS sibling_total
        FROM all_taxonomies
        GROUP BY data_provider, stream_id, version_start
    ),

    main_direct_children AS (
        SELECT
            t.data_provider,
            t.stream_id,
            m.version_start,
            m.version_end,
            t.child_data_provider,
            t.child_stream_id,
            t.weight,
            m.sibling_total
        FROM all_taxonomies t
        JOIN main_versions m
        ON t.data_provider = m.data_provider 
        AND t.stream_id = m.stream_id 
        AND t.version_start = m.version_start
    ),

    hierarchy AS (
        -- Anchor (top-level normalization only)
        SELECT
            m.child_data_provider AS data_provider,
            m.child_stream_id AS stream_id,
            m.weight AS raw_weight,
            m.version_start AS version_start,
            m.version_end AS version_end
        FROM main_direct_children m
        WHERE m.data_provider = $data_provider
          AND m.stream_id = $stream_id

        UNION ALL

        -- Recursive step (no double-normalization)
        SELECT
            c.child_data_provider,
            c.child_stream_id,
            (parent.raw_weight * c.weight)::NUMERIC(36,18) AS raw_weight,
            GREATEST(parent.version_start, c.version_start) AS version_start,
            LEAST(parent.version_end, c.version_end) AS version_end
        FROM hierarchy parent
        INNER JOIN main_direct_children c
          ON c.data_provider = parent.data_provider
         AND c.stream_id    = parent.stream_id
         AND c.version_start  <= parent.version_end
         AND c.version_end >= parent.version_start
        WHERE parent.version_start <= parent.version_end
    ),

    primitive_weights AS (
        SELECT h.*
        FROM hierarchy h
        WHERE NOT EXISTS (
            SELECT 1
            FROM taxonomies tx
            WHERE tx.data_provider = h.data_provider
              AND tx.stream_id    = h.stream_id
              AND tx.disabled_at IS NULL
              AND tx.start_time <= h.version_end
        )
        AND h.version_start <= h.version_end
    ),

    -- If no event times exist in the range, manually create the requested range
    query_times AS (
        SELECT $from AS event_time
        WHERE $from IS NOT NULL
        
        UNION
        
        -- Get all distinct event times in our query range
        SELECT DISTINCT event_time
        FROM primitive_events
        WHERE ($from IS NULL OR event_time >= $from)
          AND ($to IS NULL OR event_time <= $to)
    ),

    -- Unique primitive streams
    substreams AS (
        SELECT DISTINCT data_provider, stream_id
        FROM primitive_weights
    ),

    -- Get most recent event for each stream at each event_time
    stream_values AS (
        SELECT
            qt.event_time,
            ss.data_provider,
            ss.stream_id,
            (
                SELECT pe.value
                FROM primitive_events pe
                WHERE pe.data_provider = ss.data_provider
                  AND pe.stream_id = ss.stream_id
                  AND pe.event_time <= qt.event_time
                  AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
                ORDER BY pe.event_time DESC, pe.created_at DESC
                LIMIT 1
            ) AS value
        FROM query_times qt
        JOIN substreams ss ON 1=1
    ),

    -- Join with weights and filter streams that have no data
    weighted_values AS (
        SELECT
            sv.event_time,
            (sv.value * pw.raw_weight)::NUMERIC(36,18) AS weighted_value,
            pw.raw_weight
        FROM stream_values sv
        JOIN primitive_weights pw
          ON sv.data_provider = pw.data_provider
         AND sv.stream_id = pw.stream_id
         AND sv.event_time BETWEEN pw.version_start AND pw.version_end
        WHERE sv.value IS NOT NULL
    ),

    -- Aggregate and normalize by event_time
    aggregated AS (
        SELECT
            event_time,
            CASE WHEN SUM(raw_weight)::NUMERIC(36,18) = 0::NUMERIC(36,18)
                 THEN 0::NUMERIC(36,18)
                 ELSE SUM(weighted_value)::NUMERIC(36,18) / SUM(raw_weight)::NUMERIC(36,18)
            END AS value
        FROM weighted_values
        GROUP BY event_time
    )

    SELECT
        event_time,
        value::NUMERIC(36,18)
    FROM aggregated
    ORDER BY event_time;
};
/**
 * get_last_record_composed: Placeholder for finding last record in composed stream.
 * Will determine the last record based on child stream values and weights.
 */
CREATE OR REPLACE ACTION get_last_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    ERROR('Composed stream query implementation is missing');
};

/**
 * get_first_record_composed: Placeholder for finding first record in composed stream.
 * Will determine the first record based on child stream values and weights.
 */
CREATE OR REPLACE ACTION get_first_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    ERROR('Composed stream query implementation is missing');
};

/**
 * get_base_value_composed: Placeholder for finding base value in composed stream.
 * Will calculate base value from child streams at the specified time.
 */
CREATE OR REPLACE ACTION get_base_value_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $base_time INT8,
    $frozen_at INT8
) PRIVATE view returns (value NUMERIC(36,18)) {
    ERROR('Composed stream query implementation is missing');
};

/**
 * get_index_composed: Placeholder for index calculation in composed streams.
 * Will calculate index values using the formula: (current_value/base_value)*100
 */
CREATE OR REPLACE ACTION get_index_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $base_time INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    ERROR('Composed stream query implementation is missing');
};

