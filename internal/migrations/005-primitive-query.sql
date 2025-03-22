/**
 * get_record_primitive: Retrieves time series data for primitive streams.
 * Handles gap filling by using the last value before the requested range.
 * Validates read permissions and supports time-based filtering.
 */
CREATE OR REPLACE ACTION get_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read access first
    if is_allowed_to_read($data_provider, $stream_id, @caller, $from, $to) == false {
        ERROR('wallet not allowed to read');
    }

    RETURN WITH 
    -- Get base records within time range
    base_records AS (
        SELECT 
            pe.event_time,
            pe.value,
            ROW_NUMBER() OVER (
                PARTITION BY pe.event_time 
                ORDER BY pe.created_at DESC
            ) as rn
        FROM primitive_events pe
        WHERE pe.data_provider = $data_provider
            AND pe.stream_id = $stream_id
            AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
            AND ($from IS NULL OR pe.event_time >= $from)
            AND ($to IS NULL OR pe.event_time <= $to)
    ),
    
    -- Get potential gap filler before from date
    gap_filler AS (
        SELECT pe.event_time, pe.value
        FROM primitive_events pe
        WHERE $from IS NOT NULL
            AND pe.data_provider = $data_provider
            AND pe.stream_id = $stream_id
            AND pe.event_time < $from
            AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1
    ),
    
    -- Combine results with gap filling logic
    combined_results AS (
        -- Add gap filler if needed
        SELECT event_time, value FROM gap_filler
        WHERE (
            (SELECT COUNT(*) FROM (SELECT * FROM base_records WHERE rn = 1) AS br_sub) = 0
            OR (
                $from IS NOT NULL 
                AND (SELECT MIN(event_time) FROM (SELECT * FROM base_records WHERE rn = 1) AS br_min) > $from
            )
        )
        
        UNION ALL
        
        -- Add filtered base records
        SELECT event_time, value 
        FROM base_records 
        WHERE rn = 1
    )

    -- Final selection with fallback
    SELECT event_time, value FROM combined_results
    UNION ALL
    SELECT event_time, value FROM gap_filler
    WHERE (SELECT COUNT(*) FROM combined_results) = 0
    ORDER BY event_time ASC;
};

/**
 * get_last_record_primitive: Finds the most recent record before a timestamp.
 * Validates read permissions and respects frozen_at parameter.
 */
CREATE OR REPLACE ACTION get_last_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read access, since we're querying directly from the primitive_events table
    if is_allowed_to_read($data_provider, $stream_id, @caller, NULL, $before) == false {
        ERROR('wallet not allowed to read');
    }

    for $row in SELECT pe.event_time, pe.value 
        FROM primitive_events pe
        WHERE pe.data_provider = $data_provider 
        AND pe.stream_id = $stream_id
        AND pe.event_time < $before
        AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1 {
        RETURN NEXT $row.event_time, $row.value;
    }
};

/**
 * get_first_record_primitive: Finds the earliest record after a timestamp.
 * Validates read permissions and respects frozen_at parameter.
 */
CREATE OR REPLACE ACTION get_first_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read access, since we're querying directly from the primitive_events table
    if is_allowed_to_read($data_provider, $stream_id, @caller, $after, NULL) == false {
        ERROR('wallet not allowed to read');
    }
    
    RETURN SELECT pe.event_time, pe.value 
        FROM primitive_events pe
        WHERE pe.data_provider = $data_provider 
        AND pe.stream_id = $stream_id
        AND ($after IS NULL OR pe.event_time >= $after)
        AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
        ORDER BY pe.event_time ASC, pe.created_at DESC
        LIMIT 1;
};