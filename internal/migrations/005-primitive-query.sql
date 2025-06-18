/**
 * get_record_primitive: Retrieves time series data for primitive streams.
 * Handles gap filling by using the last value before the requested range.
 * Validates read permissions and supports time-based filtering.
 * Uses truflation_created_at for frozen mechanism on specific data provider.
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
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $frozen_data_provider := '0x4710a8d8f0d845da110086812a32de6d90d7ff5c';
    
    -- Check read access first
    if is_allowed_to_read($data_provider, $stream_id, $lower_caller, $from, $to) == false {
        ERROR('wallet not allowed to read');
    }

    $max_int8 INT8 := 9223372036854775000;
    $effective_from INT8 := COALESCE($from, 0);
    $effective_to INT8 := COALESCE($to, $max_int8);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    -- for historical consistency, if both from and to are omitted, return the latest record
    if $from IS NULL AND $to IS NULL {
        FOR $row IN get_last_record_primitive($data_provider, $stream_id, NULL, $effective_frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
        RETURN;
    }

    RETURN WITH
    -- Get base records within time range with frozen mechanism
    interval_records AS (
        SELECT
            pe.event_time,
            pe.value,
            CASE 
                WHEN pe.data_provider = $frozen_data_provider THEN
                    ROW_NUMBER() OVER (
                        PARTITION BY pe.event_time
                        ORDER BY 
                            -- First, records after frozen_at come first
                            CASE WHEN parse_unix_timestamp(pe.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN 0 ELSE 1 END,
                            -- For records after frozen_at: ascending (oldest first)
                            CASE 
                                WHEN parse_unix_timestamp(pe.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at 
                                THEN parse_unix_timestamp(pe.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8
                                ELSE NULL
                            END ASC,
                            -- For records at/before frozen_at: descending (newest first)
                            CASE 
                                WHEN parse_unix_timestamp(pe.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 <= $effective_frozen_at 
                                THEN parse_unix_timestamp(pe.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8
                                ELSE NULL
                            END DESC
                    )
                ELSE
                    ROW_NUMBER() OVER (
                        PARTITION BY pe.event_time
                        ORDER BY pe.created_at DESC
                    )
            END as rn
        FROM primitive_events pe
        WHERE pe.data_provider = $data_provider
            AND pe.stream_id = $stream_id
            AND (pe.data_provider = $frozen_data_provider OR pe.created_at <= $effective_frozen_at)
            AND pe.event_time > $effective_from
            AND pe.event_time <= $effective_to
    ),

    -- get anchor at or before from date with frozen mechanism
    anchor_record AS (
        SELECT pe.event_time, pe.value
        FROM (
            SELECT 
                event_time,
                value,
                CASE 
                    WHEN data_provider = $frozen_data_provider THEN
                        ROW_NUMBER() OVER (
                            ORDER BY 
                                event_time DESC,
                                -- First, records after frozen_at come first
                                CASE WHEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN 0 ELSE 1 END,
                                -- For records after frozen_at: ascending (oldest first)
                                CASE 
                                    WHEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at 
                                    THEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8
                                    ELSE NULL
                                END ASC,
                                -- For records at/before frozen_at: descending (newest first)
                                CASE 
                                    WHEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 <= $effective_frozen_at 
                                    THEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8
                                    ELSE NULL
                                END DESC
                        )
                    ELSE
                        ROW_NUMBER() OVER (ORDER BY event_time DESC, created_at DESC)
                END as rn
            FROM primitive_events
            WHERE 
                data_provider = $data_provider
                AND stream_id = $stream_id
                AND event_time <= $effective_from
                AND (data_provider = $frozen_data_provider OR created_at <= $effective_frozen_at)
        ) pe
        WHERE rn = 1
        LIMIT 1
    ),

    -- Combine results with gap filling logic
    combined_results AS (
        -- Add gap filler if needed
        SELECT event_time, value FROM anchor_record
        UNION ALL
        -- Add filtered base records
        SELECT event_time, value FROM interval_records
        WHERE rn = 1
    )
    -- Final selection with fallback
    SELECT event_time, value FROM combined_results
    ORDER BY event_time ASC;
};

/**
 * get_last_record_primitive: Finds the most recent record before a timestamp.
 * Validates read permissions and respects frozen_at parameter.
 * Uses truflation_created_at for frozen mechanism on specific data provider.
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
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $frozen_data_provider := '0x4710a8d8f0d845da110086812a32de6d90d7ff5c';

    -- Check read access, since we're querying directly from the primitive_events table
    if is_allowed_to_read($data_provider, $stream_id, $lower_caller, NULL, $before) == false {
        ERROR('wallet not allowed to read');
    }

    $max_int8 INT8 := 9223372036854775000;
    $effective_before INT8 := COALESCE($before, $max_int8);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    RETURN SELECT pe.event_time, pe.value
        FROM (
            SELECT 
                event_time,
                value,
                CASE 
                    WHEN data_provider = $frozen_data_provider THEN
                        ROW_NUMBER() OVER (
                            ORDER BY 
                                event_time DESC,
                                -- First, records after frozen_at come first
                                CASE WHEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN 0 ELSE 1 END,
                                -- For records after frozen_at: ascending (oldest first)
                                CASE 
                                    WHEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at 
                                    THEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8
                                    ELSE NULL
                                END ASC,
                                -- For records at/before frozen_at: descending (newest first)
                                CASE 
                                    WHEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 <= $effective_frozen_at 
                                    THEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8
                                    ELSE NULL
                                END DESC
                        )
                    ELSE
                        ROW_NUMBER() OVER (ORDER BY event_time DESC, created_at DESC)
                END as rn
            FROM primitive_events
            WHERE data_provider = $data_provider
                AND stream_id = $stream_id
                AND event_time < $effective_before
                AND (data_provider = $frozen_data_provider OR created_at <= $effective_frozen_at)
        ) pe
        WHERE pe.rn = 1
        LIMIT 1;
};

/**
 * get_first_record_primitive: Finds the earliest record after a timestamp.
 * Validates read permissions and respects frozen_at parameter.
 * Uses truflation_created_at for frozen mechanism on specific data provider.
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
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $frozen_data_provider := '0x4710a8d8f0d845da110086812a32de6d90d7ff5c';
    
    -- Check read access, since we're querying directly from the primitive_events table
    if is_allowed_to_read($data_provider, $stream_id, $lower_caller, $after, NULL) == false {
        ERROR('wallet not allowed to read');
    }

    $max_int8 INT8 := 9223372036854775000;
    $effective_after INT8 := COALESCE($after, 0);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    RETURN SELECT pe.event_time, pe.value
        FROM (
            SELECT 
                event_time,
                value,
                CASE 
                    WHEN data_provider = $frozen_data_provider THEN
                        ROW_NUMBER() OVER (
                            ORDER BY 
                                event_time ASC,
                                -- First, records after frozen_at come first
                                CASE WHEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN 0 ELSE 1 END,
                                -- For records after frozen_at: ascending (oldest first)
                                CASE 
                                    WHEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at 
                                    THEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8
                                    ELSE NULL
                                END ASC,
                                -- For records at/before frozen_at: descending (newest first)
                                CASE 
                                    WHEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 <= $effective_frozen_at 
                                    THEN parse_unix_timestamp(truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8
                                    ELSE NULL
                                END DESC
                        )
                    ELSE
                        ROW_NUMBER() OVER (ORDER BY event_time ASC, created_at DESC)
                END as rn
            FROM primitive_events
            WHERE data_provider = $data_provider
                AND stream_id = $stream_id
                AND event_time >= $effective_after
                AND (data_provider = $frozen_data_provider OR created_at <= $effective_frozen_at)
        ) pe
        WHERE pe.rn = 1
        LIMIT 1;
};

/**
 * get_index_primitive: Calculates indexed values relative to a base value.
 * Uses the modified primitive queries which include frozen mechanism.
 */
CREATE OR REPLACE ACTION get_index_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $base_time INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider := LOWER($data_provider);

    -- Check read permissions
    if !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }

    $max_int8 INT8 := 9223372036854775000;
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);
    
    -- If base_time is not provided, try to get it from metadata
    $effective_base_time INT8 := $base_time;
    if $effective_base_time IS NULL {
        $found_metadata := FALSE;
        for $row in SELECT value_i 
            FROM metadata 
            WHERE data_provider = $data_provider 
            AND stream_id = $stream_id 
            AND metadata_key = 'default_base_time' 
            AND disabled_at IS NULL
            ORDER BY created_at DESC 
            LIMIT 1 {
            $effective_base_time := $row.value_i;
            $found_metadata := TRUE;
            break;
        }
    }

    -- Get the base value (will use frozen mechanism through get_base_value)
    $base_value NUMERIC(36,18) := get_base_value($data_provider, $stream_id, $effective_base_time, $frozen_at);

    -- Check if base value is zero to avoid division by zero
    if $base_value = 0::NUMERIC(36,18) {
        ERROR('base value is 0');
    }

    -- for historical consistency, if both from and to are omitted, return the latest record
    if $from IS NULL AND $to IS NULL {
        for $row in get_last_record_primitive($data_provider, $stream_id, NULL, $effective_frozen_at) {
            $indexed_value NUMERIC(36,18) := ($row.value * 100::NUMERIC(36,18)) / $base_value;
            RETURN NEXT $row.event_time, $indexed_value;
        }
        RETURN;
    }

    -- Calculate the index for each record using the modified get_record_primitive
    for $record in get_record_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
        $indexed_value NUMERIC(36,18) := ($record.value * 100::NUMERIC(36,18)) / $base_value;
        RETURN NEXT $record.event_time, $indexed_value;
    }
};