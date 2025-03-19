/**
 * Efficiently retrieves and aggregates data from a composed stream hierarchy.
 * 
 * This function traverses a hierarchical taxonomy structure to:
 * 1. Find all primitive streams that contribute to the composed stream
 * 2. Apply time-specific weights based on taxonomy configurations
 * 3. Use gap-filling to handle missing data points
 * 4. Calculate weighted averages of values across the hierarchy
 */
CREATE OR REPLACE ACTION get_record_composed(
    $data_provider TEXT,  -- Stream owner
    $stream_id TEXT,      -- Target composed stream
    $from INT8,           -- Start of time range (inclusive)
    $to INT8,             -- End of time range (inclusive)
    $frozen_at INT8       -- Version cutoff timestamp
) PRIVATE VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
)  {
    -- Define constants to improve readability and avoid repeated literals
    $max_int8 := 9223372036854775000;  -- INT8 max for "infinity"

    -- Validate time range
    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('from: %s > to: %s', $from, $to));
    }

    -- Check read permissions
    if !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }

    -- Check compose permissions
    if !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
        ERROR('Not allowed to compose stream');
    }

    RETURN WITH RECURSIVE

    -- Find taxonomy versions that apply to our query window
    -- Early filtering reduces data processing significantly
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
          AND t.start_time <= COALESCE($to, $max_int8)
          AND (
              -- Include the anchor taxonomy (active at $from)
              ($from IS NOT NULL AND t.start_time = (
                  SELECT MAX(start_time)
                  FROM taxonomies
                  WHERE data_provider = $data_provider
                      AND stream_id = $stream_id
                      AND disabled_at IS NULL
                      AND start_time <= $from
              ))
              OR
              -- Include taxonomies that become active during our query window
              ($from IS NULL OR t.start_time > $from)
              AND ($to IS NULL OR t.start_time <= $to)
          )
    ),

    -- Keep only the latest version for each time point
    -- This handles cases where multiple versions exist at the same start_time
    latest_versions AS (
        SELECT
            data_provider,
            stream_id,
            start_time,
            version
        FROM selected_taxonomy_versions
        WHERE rn = 1
    ),

    -- Get the child stream references from selected taxonomy versions
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
    ),

    -- Calculate validity periods for each taxonomy version
    -- A taxonomy version is valid from its start_time until the next version begins
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
                $max_int8
            ) AS version_end
        FROM all_taxonomies
        GROUP BY data_provider, stream_id, version_start
    ),

    -- Connect child streams with their validity periods
    main_direct_children AS (
        SELECT
            t.data_provider,
            t.stream_id,
            m.version_start,
            m.version_end,
            t.child_data_provider,
            t.child_stream_id,
            t.weight
        FROM all_taxonomies t
        JOIN main_versions m
        ON t.data_provider = m.data_provider 
        AND t.stream_id = m.stream_id 
        AND t.version_start = m.version_start
    ),

    -- Recursively traverse the hierarchy to find all primitive streams
    -- This builds a flattened view of the entire hierarchy with calculated weights
    hierarchy AS (
        -- Base case: direct children of target stream
        -- Filter by time range early to minimize recursion depth
        SELECT
            m.child_data_provider AS data_provider,
            m.child_stream_id AS stream_id,
            m.weight AS raw_weight,
            m.version_start AS version_start,
            m.version_end AS version_end
        FROM main_direct_children m
        WHERE m.data_provider = $data_provider
          AND m.stream_id = $stream_id
          AND m.version_end >= COALESCE($from, 0)
          AND m.version_start <= COALESCE($to, $max_int8)

        UNION ALL

        -- Recursive step: follow each branch down to its leaves
        -- Weights multiply down the hierarchy (child weight = parent weight * child weight)
        SELECT
            c.child_data_provider,
            c.child_stream_id,
            (parent.raw_weight * c.weight)::NUMERIC(36,18) AS raw_weight,
            GREATEST(parent.version_start, c.version_start) AS version_start,
            LEAST(parent.version_end, c.version_end) AS version_end
        FROM hierarchy parent
        INNER JOIN main_direct_children c
          ON c.data_provider = parent.data_provider
         AND c.stream_id = parent.stream_id
         -- Only follow connections with overlapping validity periods
         AND c.version_start <= parent.version_end
         AND c.version_end >= parent.version_start
        WHERE parent.version_start <= parent.version_end
          -- Early time filtering reduces unnecessary recursion
          AND c.version_end >= COALESCE($from, 0)
          AND c.version_start <= COALESCE($to, $max_int8)
    ),

    -- Filter to only leaf nodes (primitive streams) in the hierarchy
    primitive_weights AS (
        SELECT h.*
        FROM hierarchy h
        WHERE NOT EXISTS (
            SELECT 1
            FROM taxonomies tx
            WHERE tx.data_provider = h.data_provider
              AND tx.stream_id = h.stream_id
              AND tx.disabled_at IS NULL
              AND tx.start_time <= h.version_end
        )
        AND h.version_start <= h.version_end
    ),

    -- Extract unique primitive streams from the hierarchy
    effective_streams AS (
        SELECT DISTINCT data_provider, stream_id
        FROM primitive_weights
    ),

    -- For gap filling: find the most recent value before $from for each stream
    -- These anchor points provide values when no data exists at the exact time point
    anchor_events AS (
        SELECT
            es.data_provider,
            es.stream_id,
            (
                SELECT MAX(pe.event_time)
                FROM primitive_events pe
                WHERE pe.data_provider = es.data_provider
                  AND pe.stream_id = es.stream_id
                  AND ($from IS NOT NULL AND pe.event_time < $from)
                  AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
            ) AS event_time
        FROM effective_streams es
        WHERE $from IS NOT NULL
    ),

    -- Determine all time points that need evaluation in our final result
    query_times AS (
        -- Always include $from if specified (for exact boundary evaluation)
        SELECT $from AS event_time
        WHERE $from IS NOT NULL
        
        UNION
        
        -- Include all times with actual data in our range
        SELECT pe.event_time
        FROM primitive_events pe
        JOIN effective_streams es
          ON pe.data_provider = es.data_provider
         AND pe.stream_id = es.stream_id
        WHERE ($from IS NULL OR pe.event_time >= $from)
          AND ($to IS NULL OR pe.event_time <= $to)
          AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
        GROUP BY pe.event_time  -- More efficient than DISTINCT
        
        UNION
        
        -- Include anchor times for gap filling
        SELECT ae.event_time
        FROM anchor_events ae
        WHERE ae.event_time IS NOT NULL
    ),

    -- Pre-filter to only the events needed for calculation
    -- This drastically reduces the dataset before expensive operations
    relevant_events AS (
        -- Events within our requested range
        SELECT 
            pe.data_provider,
            pe.stream_id,
            pe.event_time,
            pe.value,
            pe.created_at
        FROM primitive_events pe
        JOIN effective_streams es
          ON pe.data_provider = es.data_provider
         AND pe.stream_id = es.stream_id
        JOIN query_times qt
          ON pe.event_time = qt.event_time
        WHERE ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
        
        UNION
        
        -- Anchor events needed for gap filling
        SELECT 
            pe.data_provider,
            pe.stream_id,
            pe.event_time,
            pe.value,
            pe.created_at
        FROM primitive_events pe
        JOIN anchor_events ae
          ON pe.data_provider = ae.data_provider
         AND pe.stream_id = ae.stream_id
         AND pe.event_time = ae.event_time
        WHERE ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
    ),

    -- Get only the latest version of each event (if events were updated)
    -- Using subquery with window function avoids repeated correlated subqueries
    latest_events AS (
        SELECT data_provider, stream_id, event_time, value 
        FROM (
            SELECT 
                data_provider, 
                stream_id, 
                event_time, 
                value,
                ROW_NUMBER() OVER (
                    PARTITION BY data_provider, stream_id, event_time
                    ORDER BY created_at DESC
                ) AS rn
            FROM relevant_events
        ) sub 
        WHERE rn = 1
    ),

    -- Pre-calculate the latest event time for each stream at each query time
    -- This optimizes gap-filling by avoiding expensive repeated subqueries
    latest_event_times AS (
        SELECT
            qt.event_time AS query_time,
            es.data_provider,
            es.stream_id,
            MAX(le.event_time) AS latest_event_time
        FROM query_times qt
        JOIN effective_streams es ON 1=1
        LEFT JOIN latest_events le 
          ON le.data_provider = es.data_provider
         AND le.stream_id = es.stream_id
         AND le.event_time <= qt.event_time
        WHERE ($from IS NULL OR qt.event_time >= $from)
          AND ($to IS NULL OR qt.event_time <= $to)
        GROUP BY qt.event_time, es.data_provider, es.stream_id
    ),

    -- Join to get gap-filled values using the pre-calculated latest event times
    -- This completes the gap-filling process with a simple join instead of correlated subqueries
    stream_values AS (
        SELECT
            let.query_time AS event_time,
            let.data_provider,
            let.stream_id,
            le.value
        FROM latest_event_times let
        LEFT JOIN latest_events le 
          ON le.data_provider = let.data_provider
         AND le.stream_id = let.stream_id
         AND le.event_time = let.latest_event_time
    ),

    -- Apply weights based on topology and time validity
    -- Only includes values for streams that are active at the given event_time
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

    -- Calculate weighted average for each time point
    -- Formula: sum(value*weight)/sum(weight), with divide-by-zero protection
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
 * get_last_record_composed: Retrieves the most recent data point from a composed stream.
 * Uses the same hierarchy traversal and aggregation logic as get_record_composed.
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

