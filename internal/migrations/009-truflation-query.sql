/**
 * truflation_last_deployed_date: Returns the last deployed date of the Truflation data provider.
 * This action checks if the caller has read access to the specified stream and ensures that the stream is a primitive stream.
 * If both conditions are met, it retrieves the last deployed date from the primitive_events table.
 */
CREATE OR REPLACE ACTION truflation_last_deployed_date(
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns table(
       value TEXT
) {
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    -- Fail fast if stream doesn't exist
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Check read access first
    if !is_allowed_to_read_core($stream_ref, $lower_caller, 0, 0) {
        ERROR('wallet not allowed to read');
    }

    -- Ensure that the stream is a primitive stream
    if !is_primitive_stream($data_provider, $stream_id) {
        ERROR('stream is not a primitive stream');
    }

    RETURN SELECT truflation_created_at
           FROM primitive_events
           WHERE stream_ref = $stream_ref
           ORDER BY truflation_created_at DESC LIMIT 1;
};

/**
 * truflation_insert_records: Adds multiple new data points to a primitive stream in batch.
 * Validates write permissions and stream existence for each record before insertion.
 * This action is specifically designed for the Truflation data provider as it requires the truflation_created_at field.
 */
CREATE OR REPLACE ACTION truflation_insert_records(
    $data_provider TEXT[],
    $stream_id TEXT[],
    $event_time INT8[],
    $value NUMERIC(36,18)[],
    $truflation_created_at TEXT[]
) PUBLIC {
    -- Use helper function to avoid expensive for-loop roundtrips
    $data_providers := helper_lowercase_array($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $num_records INT := array_length($data_provider);
    if $num_records != array_length($stream_id) or $num_records != array_length($event_time) or $num_records != array_length($value) or $num_records != array_length($truflation_created_at) {
        ERROR('array lengths mismatch');
    }

    $current_block INT := @height;

    -- Get stream reference for all streams (get_stream_ids returns NULL for non-existent streams)
    $stream_refs := get_stream_ids($data_providers, $stream_id);

    -- Check stream existence using stream refs (NULL values indicate non-existent streams)
    for $i in 1..array_length($stream_refs) {
        if $stream_refs[$i] IS NULL {
            ERROR('stream does not exist: data_provider=' || $data_provider[$i] || ', stream_id=' || $stream_id[$i]);
        }
    }

    -- Check if streams are primitive in batch
    for $row in is_primitive_stream_batch($data_providers, $stream_id) {
        if !$row.is_primitive {
            ERROR('stream is not a primitive stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Validate that the wallet is allowed to write to each stream
    for $row in is_wallet_allowed_to_write_batch($data_providers, $stream_id, $lower_caller) {
        if !$row.is_allowed {
            ERROR('wallet not allowed to write to stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Insert all records using WITH RECURSIVE pattern to avoid round trips
    WITH RECURSIVE
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < $num_records
    ),
    record_arrays AS (
        SELECT
            $event_time AS event_times,
            $value AS values_array,
            $truflation_created_at AS truflation_created_at_array,
            $stream_refs AS stream_refs_array
    ),
    arguments AS (
        SELECT
            record_arrays.event_times[idx] AS event_time,
            record_arrays.values_array[idx] AS value,
            record_arrays.truflation_created_at_array[idx] AS truflation_created_at,
            record_arrays.stream_refs_array[idx] AS stream_ref
        FROM indexes
        JOIN record_arrays ON 1=1
    )
    INSERT INTO primitive_events (event_time, value, created_at, truflation_created_at, stream_ref)
    SELECT
        event_time,
        value,
        $current_block,
        truflation_created_at,
        stream_ref
    FROM arguments;

    -- Enqueue days for pruning (idempotent, distinct), filtering out zero values
    helper_enqueue_prune_days(
        $stream_refs,
        $event_time,
        $value
    );
};


/**
 * truflation_get_record_primitive: Retrieves time series data for primitive streams.
 * Handles gap filling by using the last value before the requested range.
 * Validates read permissions and supports time-based filtering.
 */
CREATE OR REPLACE ACTION truflation_get_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Note: No cache; direct queries without computation
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);

    $stream_ref := get_stream_id($data_provider, $stream_id);

    -- Fail fast if stream doesn't exist
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Check read access first
    if is_allowed_to_read_core($stream_ref, $lower_caller, $from, $to) == false {
        ERROR('wallet not allowed to read');
    }
    $max_int8 INT8 := 9223372036854775000;
    $effective_from INT8 := COALESCE($from, 0::INT8);
    $effective_to INT8 := COALESCE($to, $max_int8);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    -- for historical consistency, if both from and to are omitted, return the latest record
    if $from IS NULL AND $to IS NULL {
        FOR $row IN truflation_last_rc_primitive($data_provider, $stream_id, NULL, $effective_frozen_at) {
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
                    END DESC, pe.created_at DESC
            ) AS rn
        FROM primitive_events pe
        WHERE pe.stream_ref = $stream_ref
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
                        END DESC, created_at DESC
                ) as rn
            FROM primitive_events
            WHERE 
                stream_ref = $stream_ref
                AND event_time <= $effective_from
        ) pe
        WHERE pe.rn = 1 
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
 * truflation_last_rc_primitive: Finds the most recent record before a timestamp.
 * Validates read permissions and respects frozen_at parameter.
 */
CREATE OR REPLACE ACTION truflation_last_rc_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Note: No cache; direct queries without computation
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);

    $stream_ref := get_stream_id($data_provider, $stream_id);

    -- Fail fast if stream doesn't exist
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Check read access, since we're querying directly from the primitive_events table
    if is_allowed_to_read_core($stream_ref, $lower_caller, NULL, $before) == false {
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
                        END DESC, created_at DESC
                ) as rn
            FROM primitive_events
            WHERE stream_ref = $stream_ref
                AND event_time < $effective_before
        ) pe
        WHERE pe.rn = 1
        LIMIT 1;
};

/**
 * truflation_first_record_primitive: Finds the earliest record after a timestamp.
 * Validates read permissions and respects frozen_at parameter.
 */
CREATE OR REPLACE ACTION truflation_first_rc_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
   -- Note: No cache; direct queries without computation
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);

    $stream_ref := get_stream_id($data_provider, $stream_id);

    -- Fail fast if stream doesn't exist
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Check read access, since we're querying directly from the primitive_events table
    if is_allowed_to_read_core($stream_ref, $lower_caller, $after, NULL) == false {
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
                        END DESC, created_at DESC
                ) AS rn
            FROM primitive_events
            WHERE stream_ref = $stream_ref
                AND event_time >= $effective_after
        ) pe
        WHERE pe.rn = 1
        LIMIT 1;
};

CREATE OR REPLACE ACTION truflation_get_index(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $base_time INT8,
    $use_cache BOOL DEFAULT false
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        -- Primitives: No cache (direct queries, no computation)
        for $row in truflation_get_index_primitive($data_provider, $stream_id, $from, $to, $frozen_at, $base_time) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        for $row in truflation_get_index_composed($data_provider, $stream_id, $from, $to, $frozen_at, $base_time, $use_cache) {
            RETURN NEXT $row.event_time, $row.value;
        }
    }
};

/**
 * truflation_get_index_primitive: Calculates indexed values relative to a base value.
 */
CREATE OR REPLACE ACTION truflation_get_index_primitive(
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
    -- Note: No cache; direct queries without computation
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    -- Fail fast if stream doesn't exist
    IF $stream_ref IS NULL {
        ERROR(
          'Stream does not exist: data_provider=' 
          || $data_provider 
          || ' stream_id=' 
          || $stream_id
        );
    }

    -- Check read permissions
    if !is_allowed_to_read_all_core($stream_ref, LOWER(@caller), $from, $to) {
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
            WHERE stream_ref = $stream_ref 
            AND metadata_key = 'default_base_time' 
            AND disabled_at IS NULL
            ORDER BY created_at DESC 
            LIMIT 1 {
            $effective_base_time := $row.value_i;
            $found_metadata := TRUE;
            break;
        }
    }

    -- Get the base value (will use frozen mechanism through truflation_get_base_value)
    $base_value NUMERIC(36,18) := truflation_get_base_value($data_provider, $stream_id, $effective_base_time, $frozen_at, false);

    -- Check if base value is zero to avoid division by zero
    if $base_value = 0::NUMERIC(36,18) {
        ERROR('base value is 0');
    }

    -- for historical consistency, if both from and to are omitted, return the latest record
    if $from IS NULL AND $to IS NULL {
        for $row in truflation_last_rc_primitive($data_provider, $stream_id, NULL, $effective_frozen_at) {
            $indexed_value NUMERIC(36,18) := ($row.value * 100::NUMERIC(36,18)) / $base_value;
            RETURN NEXT $row.event_time, $indexed_value;
        }
        RETURN;
    }

    -- Calculate the index for each record using the modified get_record_primitive
    for $record in truflation_get_record_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
        $indexed_value NUMERIC(36,18) := ($record.value * 100::NUMERIC(36,18)) / $base_value;
        RETURN NEXT $record.event_time, $indexed_value;
    }
};

/**
 * get_truflation_record: Public facade for retrieving time series data.
 * Routes to primitive or composed implementation based on stream type.
 */
CREATE OR REPLACE ACTION truflation_get_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $use_cache BOOL DEFAULT false
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        -- Primitives: No cache (direct queries, no computation)
        for $row in truflation_get_record_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        for $row in truflation_get_record_composed($data_provider, $stream_id, $from, $to, $frozen_at, $use_cache) {
            RETURN NEXT $row.event_time, $row.value;
        }
    }
};

/**
 * get_last_record: Retrieves the most recent record before a timestamp.
 * Routes to primitive or composed implementation based on stream type.
 */
CREATE OR REPLACE ACTION truflation_get_last_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8,
    $use_cache BOOL DEFAULT false
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        -- Primitives: No cache (direct queries, no computation)
        -- unfortunately, using the query directly creates error, then we use return next
        for $row in truflation_last_rc_primitive($data_provider, $stream_id, $before, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        -- unfortunately, using the query directly creates error, then we use return next
        for $row in truflation_last_rc_composed($data_provider, $stream_id, $before, $frozen_at, $use_cache) {
            RETURN NEXT $row.event_time, $row.value;
        }
    }
};

/**
 * get_first_record: Retrieves the earliest record after a timestamp.
 * Routes to primitive or composed implementation based on stream type.
 */
CREATE OR REPLACE ACTION truflation_get_first_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,
    $frozen_at INT8,
    $use_cache BOOL DEFAULT false
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);

    -- Route to the appropriate internal action
    if $is_primitive {
        -- Primitives: No cache (direct queries, no computation)
        for $row in truflation_first_rc_primitive($data_provider, $stream_id, $after, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        for $row in truflation_first_rc_composed($data_provider, $stream_id, $after, $frozen_at, $use_cache) {
            RETURN NEXT $row.event_time, $row.value;
        }
    }
};

/**
 * truflation_get_base_value: Retrieves reference value for index calculations.
 * Routes to primitive or composed implementation based on stream type.
 */
CREATE OR REPLACE ACTION truflation_get_base_value(
    $data_provider TEXT,
    $stream_id TEXT,
    $base_time INT8,
    $frozen_at INT8,
    $use_cache BOOL DEFAULT false
) PUBLIC view returns (value NUMERIC(36,18)) {
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    -- Fail fast if stream doesn't exist
    IF $stream_ref IS NULL {
        ERROR(
            'Stream does not exist: data_provider=' || $data_provider
            || ' stream_id=' || $stream_id
        );
    }

    -- Check read permissions
    if !is_allowed_to_read_all_core($stream_ref, $lower_caller, NULL, $base_time) {
        ERROR('Not allowed to read stream');
    }

    -- If base_time is null, try to get it from metadata
    $effective_base_time INT8 := $base_time;
    if $effective_base_time IS NULL {
        -- First try to get base_time from metadata
        $found_metadata := FALSE;
        for $row in SELECT value_i 
            FROM metadata 
            WHERE stream_ref = $stream_ref 
            AND metadata_key = 'default_base_time' 
            AND disabled_at IS NULL
            ORDER BY created_at DESC 
            LIMIT 1 {
            $effective_base_time := $row.value_i;
            $found_metadata := TRUE;
            break;
        }
        
        -- If still null after checking metadata, get the first ever record
        if !$found_metadata OR $effective_base_time IS NULL {
            $found_value NUMERIC(36,18);
            $found := FALSE;
            
            -- Execute the function and store results in variables
            $first_time INT8;
            $first_value NUMERIC(36,18);
            for $record in truflation_get_first_record($data_provider, $stream_id, NULL, $frozen_at, $use_cache) {
                $first_time := $record.event_time;
                $first_value := $record.value;
                $found := TRUE;
                break;
            }
            
            if $found {
                return $first_value;
            } else {
                -- If no values found, error out
                ERROR('no base value found: no records in stream');
            }
        }
    }
    
    -- Try to find an exact match at base_time
    $found_exact := FALSE;
    $exact_value NUMERIC(36,18);
    for $row in truflation_get_record($data_provider, $stream_id, $effective_base_time, $effective_base_time, $frozen_at, $use_cache) {
        $exact_value := $row.value;
        $found_exact := TRUE;
        break;
    }
    
    if $found_exact {
        return $exact_value;
    }
    
    -- If no exact match, try to find the closest value before base_time
    $found_before := FALSE;
    $before_value NUMERIC(36,18);
    for $row in truflation_get_last_record($data_provider, $stream_id, $effective_base_time, $frozen_at, $use_cache) {
        $before_value := $row.value;
        $found_before := TRUE;
        break;
    }
    
    if $found_before {
        return $before_value;
    }
    
    -- If no value before, try to find the closest value after base_time
    $found_after := FALSE;
    $after_value NUMERIC(36,18);
    for $row in truflation_get_first_record($data_provider, $stream_id, $effective_base_time, $frozen_at, $use_cache) {
        $after_value := $row.value;
        $found_after := TRUE;
        break;
    }
    
    if $found_after {
        return $after_value;
    }
    
    -- If no value is found at all, return an error
    ERROR('no base value found');
};

CREATE OR REPLACE ACTION truflation_get_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,         
    $to INT8,
    $frozen_at INT8,
    $use_cache BOOL DEFAULT false
)  PUBLIC VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
)  {
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    -- Define boundary defaults and effective values
    $stream_ref := get_stream_id($data_provider, $stream_id);
    $max_int8 := 9223372036854775000;          -- "Infinity" sentinel for INT8
    $effective_from := COALESCE($from, 0);      -- Lower bound, default 0
    $effective_to := COALESCE($to, $max_int8);  -- Upper bound, default "infinity"
    $effective_frozen_at := COALESCE($frozen_at, $max_int8);

    -- Validate time range
    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('Invalid time range: from (%s) > to (%s)', $from, $to));
    }

    -- Check permissions; raises error if unauthorized
    IF !is_allowed_to_read_all_core($stream_ref, $lower_caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }
    IF !is_allowed_to_compose_all_core($stream_ref, $from, $to) {
        ERROR('Not allowed to compose stream');
    }

    -- Set default value for use_cache
    $effective_enable_cache := COALESCE($use_cache, false);
    $effective_enable_cache := $effective_enable_cache AND $frozen_at IS NULL; -- frozen queries bypass cache

    if $effective_enable_cache {
        $effective_enable_cache := helper_check_cache($data_provider, $stream_id, $from, $to, NULL);
    }
    
    if $effective_enable_cache {
        for $row in tn_cache.get_cached_data($data_provider, $stream_id, $from, $to, NULL) {
            RETURN NEXT $row.event_time, $row.value;
        }
        return;
    }

    -- for historical consistency, if both from and to are omitted, return the latest record
    if $from IS NULL AND $to IS NULL {
        FOR $row IN truflation_last_rc_composed($data_provider, $stream_id, NULL, $effective_frozen_at, $use_cache) {
            RETURN NEXT $row.event_time, $row.value;
        }
        RETURN;
    }

    RETURN WITH RECURSIVE
    /*----------------------------------------------------------------------
    * ANCHOR CTE: Compute the anchor time once for reuse throughout the query
    *
    * Purpose: Find the latest taxonomy start_time at or before $effective_from
    * for the target stream to establish a baseline for filtering.
    *---------------------------------------------------------------------*/
    anchor AS (
        SELECT COALESCE(
            (SELECT t_anchor_base.start_time FROM taxonomies t_anchor_base
             WHERE t_anchor_base.stream_ref = $stream_ref
               AND t_anchor_base.disabled_at IS NULL AND t_anchor_base.start_time <= $effective_from
             ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC LIMIT 1),
            0 -- Default anchor to 0
        ) AS anchor_time
    ),

    /*----------------------------------------------------------------------
    * RELEVANT_PARENTS CTE: Find all streams in the subtree of the target stream
    *---------------------------------------------------------------------*/
    relevant_parents AS (
        -- Base case: the target stream itself
        SELECT $stream_ref AS stream_ref
        UNION ALL
        -- Recursive case: traverse descendants (children of already discovered streams)
        SELECT DISTINCT t.child_stream_ref
        FROM taxonomies t
        JOIN relevant_parents rp ON t.stream_ref = rp.stream_ref
        WHERE t.disabled_at IS NULL
    ),

    -- Parent distinct start times and other CTEs remain the same...
    parent_distinct_start_times AS (
        SELECT DISTINCT
            t.stream_ref,
            t.start_time
        FROM taxonomies t
        JOIN relevant_parents rp ON t.stream_ref = rp.stream_ref
        WHERE t.disabled_at IS NULL
    ),

    parent_next_starts AS (
        SELECT
            stream_ref,
            start_time,
            LEAD(start_time) OVER (PARTITION BY stream_ref ORDER BY start_time) as next_start_time
        FROM parent_distinct_start_times
    ),

    taxonomy_true_segments AS (
        SELECT
            t.stream_ref,
            t.child_stream_ref,
            t.weight AS weight_for_segment,
            t.start_time AS segment_start,
            COALESCE(pns.next_start_time, $max_int8) - 1 AS segment_end
        FROM (
            SELECT
                tx.stream_ref,
                tx.child_stream_ref,
                tx.weight,
                tx.start_time
            FROM taxonomies tx
            JOIN (
                SELECT
                    t.stream_ref, t.start_time,
                    MAX(t.group_sequence) as max_gs
                FROM taxonomies t
                WHERE t.disabled_at IS NULL
                GROUP BY t.stream_ref, t.start_time
            ) max_gs_filter
            ON tx.stream_ref = max_gs_filter.stream_ref
           AND tx.start_time = max_gs_filter.start_time
           AND tx.group_sequence = max_gs_filter.max_gs
            WHERE tx.disabled_at IS NULL
        ) t
        JOIN parent_next_starts pns
          ON t.stream_ref = pns.stream_ref
         AND t.start_time = pns.start_time
    ),

    hierarchy AS (
      SELECT
          tts.stream_ref AS root_stream_ref,
          tts.child_stream_ref AS descendant_stream_ref,
          tts.weight_for_segment AS raw_weight,
          tts.segment_start AS path_start,
          tts.segment_end AS path_end,
          1 AS level
      FROM taxonomy_true_segments tts
      WHERE tts.stream_ref = $stream_ref
        AND tts.segment_end >= (SELECT anchor_time FROM anchor)
        AND tts.segment_start <= $effective_to

      UNION ALL

      SELECT
          h.root_stream_ref,
          tts.child_stream_ref AS descendant_stream_ref,
          (h.raw_weight * tts.weight_for_segment)::NUMERIC(36,18) AS raw_weight,
          GREATEST(h.path_start, tts.segment_start) AS path_start,
          LEAST(h.path_end, tts.segment_end) AS path_end,
          h.level + 1
      FROM
          hierarchy h
      JOIN taxonomy_true_segments tts
          ON h.descendant_stream_ref = tts.stream_ref
      WHERE
          GREATEST(h.path_start, tts.segment_start) <= LEAST(h.path_end, tts.segment_end)
          AND LEAST(h.path_end, tts.segment_end) >= (SELECT anchor_time FROM anchor)
          AND GREATEST(h.path_start, tts.segment_start) <= $effective_to
          AND h.level < 100 -- Recursion depth limit to prevent taxonomy recursion attacks
    ),

    hierarchy_primitive_paths AS (
      SELECT
          h.descendant_stream_ref AS primitive_stream_ref,
          h.raw_weight,
          h.path_start,
          h.path_end
      FROM hierarchy h
      WHERE EXISTS (
          SELECT 1 FROM streams s
          WHERE s.id = h.descendant_stream_ref
            AND s.stream_type = 'primitive'
      )
    ),

    primitive_weights AS (
      SELECT
          hpp.primitive_stream_ref,
          hpp.raw_weight,
          hpp.path_start AS group_sequence_start,
          hpp.path_end AS group_sequence_end
      FROM hierarchy_primitive_paths hpp
    ),

    cleaned_event_times AS (
        SELECT DISTINCT event_time
        FROM (
            -- 1. Primitive event times strictly within the requested range
            SELECT pe.event_time
            FROM primitive_events pe
            JOIN primitive_weights pwr
              ON pe.stream_ref = pwr.primitive_stream_ref
             AND pe.event_time >= pwr.group_sequence_start
             AND pe.event_time <= pwr.group_sequence_end
            WHERE pe.event_time > $effective_from
              AND pe.event_time <= $effective_to

            UNION

            -- 2. Taxonomy start times (weight changes) strictly within the range
            SELECT pwr.group_sequence_start AS event_time
            FROM primitive_weights pwr
            WHERE pwr.group_sequence_start > $effective_from
              AND pwr.group_sequence_start <= $effective_to
        ) all_times_in_range

        UNION

        -- 4. Anchor Point
        SELECT event_time FROM (
            SELECT event_time
            FROM (
                SELECT pe.event_time
                FROM primitive_events pe
                JOIN primitive_weights pwr
                  ON pe.stream_ref = pwr.primitive_stream_ref
                 AND pe.event_time >= pwr.group_sequence_start
                 AND pe.event_time <= pwr.group_sequence_end
                WHERE pe.event_time <= $effective_from

                UNION

                SELECT pwr.group_sequence_start AS event_time
                FROM primitive_weights pwr
                WHERE pwr.group_sequence_start <= $effective_from

            ) all_times_before
            ORDER BY event_time DESC
            LIMIT 1
        ) as anchor_event
    ),

    -- Modified initial_primitive_states with frozen mechanism
    initial_primitive_states AS (
        SELECT
            pe.stream_ref,
            pe.event_time,
            pe.value
        FROM (
            SELECT
                pe_inner.stream_ref,
                pe_inner.event_time,
                pe_inner.value,
                ROW_NUMBER() OVER (
                    PARTITION BY pe_inner.stream_ref
                    ORDER BY 
                        pe_inner.event_time DESC,
                        CASE WHEN pe_inner.truf_ts > $effective_frozen_at THEN 0 ELSE 1 END,
                        CASE WHEN pe_inner.truf_ts > $effective_frozen_at THEN pe_inner.truf_ts ELSE NULL END ASC,
                        CASE WHEN pe_inner.truf_ts <= $effective_frozen_at THEN pe_inner.truf_ts ELSE NULL END DESC,
                        pe_inner.created_at DESC
                ) as rn
            FROM (
                SELECT
                    pe_calc.stream_ref,
                    pe_calc.event_time,
                    pe_calc.value,
                    pe_calc.created_at,
                    parse_unix_timestamp(pe_calc.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 AS truf_ts
                FROM primitive_events pe_calc
            ) pe_inner
            WHERE pe_inner.event_time <= $effective_from
              AND EXISTS (
                  SELECT 1 FROM primitive_weights pwr_exists
                  WHERE pwr_exists.primitive_stream_ref = pe_inner.stream_ref
              )
        ) pe
        WHERE pe.rn = 1
    ),

    -- Modified primitive_events_in_interval with frozen mechanism
    primitive_events_in_interval AS (
        SELECT
            pe.stream_ref,
            pe.event_time,
            pe.value
        FROM (
             SELECT
                pe_inner.stream_ref,
                pe_inner.event_time,
                pe_inner.created_at,
                pe_inner.value,
                ROW_NUMBER() OVER (
                    PARTITION BY pe_inner.stream_ref, pe_inner.event_time
                    ORDER BY 
                        CASE WHEN pe_inner.truf_ts > $effective_frozen_at THEN 0 ELSE 1 END,
                        CASE WHEN pe_inner.truf_ts > $effective_frozen_at THEN pe_inner.truf_ts ELSE NULL END ASC,
                        CASE WHEN pe_inner.truf_ts <= $effective_frozen_at THEN pe_inner.truf_ts ELSE NULL END DESC,
                        pe_inner.created_at DESC
                ) as rn
            FROM (
                SELECT
                    pe_calc.stream_ref,
                    pe_calc.event_time,
                    pe_calc.created_at,
                    pe_calc.value,
                    parse_unix_timestamp(pe_calc.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 AS truf_ts
                FROM primitive_events pe_calc
            ) pe_inner
            JOIN primitive_weights pwr
               ON pe_inner.stream_ref = pwr.primitive_stream_ref
              AND pe_inner.event_time >= pwr.group_sequence_start
              AND pe_inner.event_time <= pwr.group_sequence_end
            WHERE pe_inner.event_time > $effective_from
              AND pe_inner.event_time <= $effective_to
        ) pe
        WHERE pe.rn = 1
    ),

    -- The rest of the CTEs remain the same...
    all_primitive_points AS (
        SELECT stream_ref, event_time, value FROM initial_primitive_states
        UNION ALL
        SELECT stream_ref, event_time, value FROM primitive_events_in_interval
    ),

    primitive_event_changes AS (
        SELECT * FROM (
            SELECT stream_ref, event_time, value,
                   COALESCE(value - LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time), value)::numeric(36,18) AS delta_value
            FROM all_primitive_points
        ) calc WHERE delta_value != 0::numeric(36,18)
    ),

    first_value_times AS (
        SELECT
            stream_ref,
            MIN(event_time) as first_value_time
        FROM all_primitive_points
        GROUP BY stream_ref
    ),

    effective_weight_changes AS (
        SELECT
            pwr.primitive_stream_ref AS stream_ref,
            GREATEST(pwr.group_sequence_start, fvt.first_value_time) AS event_time,
            pwr.raw_weight AS weight_delta
        FROM primitive_weights pwr
        JOIN first_value_times fvt
            ON pwr.primitive_stream_ref = fvt.stream_ref
        WHERE GREATEST(pwr.group_sequence_start, fvt.first_value_time) <= pwr.group_sequence_end
          AND pwr.raw_weight != 0::numeric(36,18)

        UNION ALL

        SELECT
            pwr.primitive_stream_ref AS stream_ref,
            pwr.group_sequence_end + 1 AS event_time,
            -pwr.raw_weight AS weight_delta
        FROM primitive_weights pwr
        JOIN first_value_times fvt
            ON pwr.primitive_stream_ref = fvt.stream_ref
        WHERE GREATEST(pwr.group_sequence_start, fvt.first_value_time) <= pwr.group_sequence_end
          AND pwr.raw_weight != 0::numeric(36,18)
          AND pwr.group_sequence_end < ($max_int8 - 1)
    ),

    unified_events AS (
        SELECT
            pec.stream_ref,
            pec.event_time,
            pec.delta_value,
            0::numeric(36,18) AS weight_delta,
            1 AS event_type_priority
        FROM primitive_event_changes pec

        UNION ALL

        SELECT
            ewc.stream_ref,
            ewc.event_time,
            0::numeric(36,18) AS delta_value,
            ewc.weight_delta,
            2 AS event_type_priority
        FROM effective_weight_changes ewc
    ),

    primitive_state_timeline AS (
        SELECT
            stream_ref,
            event_time,
            delta_value,
            weight_delta,
            COALESCE(LAG(value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as value_before_event,
            COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as weight_before_event
        FROM (
            SELECT
                stream_ref,
                event_time,
                delta_value,
                weight_delta,
                event_type_priority,
                (SUM(delta_value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as value_after_event,
                (SUM(weight_delta) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as weight_after_event
            FROM unified_events
        ) state_calc
    ),

    final_deltas AS (
        SELECT
            event_time,
            SUM(
                (delta_value * weight_before_event) +
                (value_before_event * weight_delta) +
                (delta_value * weight_delta)
            )::numeric(72, 18) AS delta_ws,
            SUM(weight_delta)::numeric(36, 18) AS delta_sw
        FROM primitive_state_timeline
        GROUP BY event_time
        HAVING SUM(
                (delta_value * weight_before_event) +
                (value_before_event * weight_delta) +
                (delta_value * weight_delta)
            )::numeric(72, 18) != 0::numeric(72, 18)
            OR SUM(weight_delta)::numeric(36, 18) != 0::numeric(36, 18)
    ),

    all_combined_times AS (
        SELECT time_point FROM (
            SELECT event_time as time_point FROM final_deltas
            UNION
            SELECT event_time as time_point FROM cleaned_event_times
        ) distinct_times
    ),

    cumulative_values AS (
        SELECT
            act.time_point as event_time,
            (COALESCE((SUM(fd.delta_ws) OVER (ORDER BY act.time_point ASC))::numeric(72,18), 0::numeric(72,18))) as cum_ws,
            (COALESCE((SUM(fd.delta_sw) OVER (ORDER BY act.time_point ASC))::numeric(36,18), 0::numeric(36,18))) as cum_sw
        FROM all_combined_times act
        LEFT JOIN final_deltas fd ON fd.event_time = act.time_point
    ),

    aggregated AS (
        SELECT cv.event_time,
               CASE WHEN cv.cum_sw = 0::numeric(36,18) THEN 0::numeric(72,18)
                    ELSE cv.cum_ws / cv.cum_sw::numeric(72,18)
                   END AS value
        FROM cumulative_values cv
    ),

    real_change_times AS (
        -- 1. Times where the *aggregated* value definitively changes (non-zero deltas)
        SELECT DISTINCT event_time AS time_point
        FROM final_deltas

        UNION

        -- 2. Times where a primitive emits an event inside the requested interval, even if
        --    the emitted value is identical to its previous value (delta == 0). These are
        --    required emission points for the composed stream.
        SELECT DISTINCT event_time
        FROM primitive_events_in_interval
    ),

    anchor_time_calc AS (
        SELECT MAX(time_point) as anchor_time
        FROM real_change_times
        WHERE time_point < $effective_from
    ),

    final_mapping AS (
        SELECT agg.event_time, agg.value,
               (SELECT MAX(rct.time_point) FROM real_change_times rct WHERE rct.time_point <= agg.event_time) AS effective_time,
               EXISTS (SELECT 1 FROM real_change_times rct WHERE rct.time_point = agg.event_time) AS query_time_had_real_change
        FROM aggregated agg
    ),

    filtered_mapping AS (
        SELECT fm.*
        FROM final_mapping fm
                 JOIN anchor_time_calc atc ON 1=1
        WHERE
            (fm.event_time >= $effective_from AND fm.event_time <= $effective_to)
            OR
            (atc.anchor_time IS NOT NULL AND fm.event_time = atc.anchor_time)
    ),

    range_check AS (
        SELECT EXISTS (
            SELECT 1 FROM final_mapping fm_check
            WHERE fm_check.event_time >= $effective_from
              AND fm_check.event_time <= $effective_to
        ) AS range_has_direct_hits
    ),

    locf_applied AS (
        SELECT
            fm.*,
            rc.range_has_direct_hits,
            atc.anchor_time,
            CASE 
                WHEN fm.query_time_had_real_change
                    THEN fm.event_time 
                    ELSE fm.effective_time 
            END as final_event_time
        FROM filtered_mapping fm
        JOIN range_check rc ON 1=1
        JOIN anchor_time_calc atc ON 1=1
    ),

    direct_hits AS (
        SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
        FROM locf_applied la
        WHERE la.event_time >= $effective_from
          AND la.event_time <= $effective_to
          AND la.final_event_time IS NOT NULL
    ),
    anchor_hit AS (
      SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
      FROM locf_applied la
      WHERE la.anchor_time IS NOT NULL
        AND la.event_time = la.anchor_time
        AND $effective_from > la.anchor_time
        AND la.final_event_time IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM locf_applied dh
            WHERE dh.event_time = $effective_from
        )
    ),
    result AS (
        SELECT event_time, value FROM direct_hits
        UNION ALL
        SELECT event_time, value FROM anchor_hit
    )
    SELECT DISTINCT event_time, value FROM result
    ORDER BY 1;
};

CREATE OR REPLACE ACTION truflation_last_rc_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8,
    $use_cache BOOL DEFAULT false
) PRIVATE VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    $lower_caller  := LOWER(@caller);
    /*
     * Step 1: Basic setup
     */
    $stream_ref := get_stream_id($data_provider, $stream_id);
    IF $stream_ref IS NULL {
        ERROR(
          'Stream does not exist: data_provider='
          || $data_provider
          || ' stream_id='
          || $stream_id
        );
    }

    IF !is_allowed_to_read_all_core($stream_ref, $lower_caller, NULL, $before) {
        ERROR('Not allowed to read stream');
    }

    -- Check compose permissions
    if !is_allowed_to_compose_all_core($stream_ref, NULL, $before) {
        ERROR('Not allowed to compose stream');
    }

    $max_int8 INT8 := 9223372036854775000;    -- "Infinity" sentinel
    $effective_before INT8 := COALESCE($before, $max_int8);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    -- Set default value for use_cache
    $effective_enable_cache := COALESCE($use_cache, false);
    $effective_enable_cache := $effective_enable_cache AND $frozen_at IS NULL; -- frozen queries bypass cache

    if $effective_enable_cache {
        -- we use before as to, because if we have data for that, it automatically means
        -- that we can answer this query
        $effective_enable_cache := helper_check_cache($data_provider, $stream_id, $before, NULL, NULL);
    }

    -- If using cache, get the most recent cached record
    if $effective_enable_cache {
        -- Get cached data up to the before time and return the most recent
        for $row in tn_cache.get_cached_last_before($data_provider, $stream_id, $before, NULL) {
            RETURN NEXT $row.event_time, $row.value;
        }
        RETURN;
    }

    $latest_event_time INT8;

    /*
     * Step 2: Recursively gather all children (ignoring overshadow),
     *         then identify primitive leaves.
     */
    for $row in WITH RECURSIVE descendants AS (
      SELECT t.child_stream_ref
      FROM taxonomies t
      WHERE t.stream_ref = $stream_ref AND t.disabled_at IS NULL
      UNION
      SELECT t.child_stream_ref
      FROM taxonomies t
      JOIN descendants d ON t.stream_ref = d.child_stream_ref
      WHERE t.disabled_at IS NULL
    ),
    primitive_leaves AS (
      SELECT DISTINCT s.id AS stream_ref
      FROM streams s
      WHERE s.stream_type = 'primitive'
        AND s.id IN (SELECT child_stream_ref FROM descendants)
    ),
    /*
     * Step 3: In each primitive, pick the single latest event_time <= effective_before.
     *         Apply frozen mechanism for truflation_created_at ordering.
     */
    latest_events AS (
      SELECT
        pl.stream_ref,
        pe_wrap.event_time,
        pe_wrap.value,
        pe_wrap.created_at,
        ROW_NUMBER() OVER (
          PARTITION BY pl.stream_ref
          ORDER BY
            pe_wrap.event_time DESC,
            CASE WHEN pe_wrap.truf_ts > $effective_frozen_at THEN 0 ELSE 1 END,
            CASE WHEN pe_wrap.truf_ts > $effective_frozen_at THEN pe_wrap.truf_ts ELSE NULL END ASC,
            CASE WHEN pe_wrap.truf_ts <= $effective_frozen_at THEN pe_wrap.truf_ts ELSE NULL END DESC,
            pe_wrap.created_at DESC
        ) AS rn
      FROM primitive_leaves pl
      JOIN (
        SELECT
          pe.stream_ref,
          pe.event_time,
          pe.value,
          pe.created_at,
          parse_unix_timestamp(pe.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 AS truf_ts
        FROM primitive_events pe
      ) pe_wrap
        ON pe_wrap.stream_ref = pl.stream_ref
      WHERE pe_wrap.event_time <= $effective_before
    ),
    latest_values AS (
      /* Step 4: Filter to rn=1 => the single latest event per stream_ref */
      SELECT
        stream_ref,
        event_time,
        value
      FROM latest_events
      WHERE rn = 1
    ),
    global_max AS (
      /* Step 5: Find the maximum event_time among all leaves */
      SELECT MAX(event_time) AS latest_time
      FROM latest_values
    )
    /* Step 6: Return the row(s) matching that global latest_time (pick first) */
    SELECT
      lv.event_time,
      lv.value::NUMERIC(36,18)
    FROM latest_values lv
    JOIN global_max gm
      ON lv.event_time = gm.latest_time
    {
        $latest_event_time := $row.event_time;
        break;  -- break out after storing
    }

    /*
     * Step 7: If we found latest_event_time, call truflation_get_record_composed() at
     *          [latest_event_time, latest_event_time] for overshadow logic.
     */
    IF $latest_event_time IS DISTINCT FROM NULL {
        for $row in truflation_get_record_composed($data_provider, $stream_id, $latest_event_time, $latest_event_time, $frozen_at, $use_cache) {
            return next $row.event_time, $row.value;
            break;
        }
    }

    /* If no events were found, no rows are returned */
};

-- NOTE: This function finds the single earliest event ignoring overshadow,
-- then uses truflation_get_record_composed() at that time. Simplified, but effective
-- for most common use cases; may have edge cases and is not highly optimized.
CREATE OR REPLACE ACTION truflation_first_rc_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,
    $frozen_at INT8,
    $use_cache BOOL DEFAULT false
) PRIVATE VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    $lower_caller  := LOWER(@caller);
    /*
     * Step 1: Basic setup
     */
    $stream_ref := get_stream_id($data_provider, $stream_id);
    IF $stream_ref IS NULL {
        ERROR(
          'Stream does not exist: data_provider='
          || $data_provider
          || ' stream_id='
          || $stream_id
        );
    }

    IF !is_allowed_to_read_all_core($stream_ref, $lower_caller, $after, NULL) {
        ERROR('Not allowed to read stream');
    }

    $max_int8 INT8 := 9223372036854775000;   -- "Infinity" sentinel
    $effective_after INT8 := COALESCE($after, 0);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    -- Set default value for use_cache
    $effective_enable_cache := COALESCE($use_cache, false);
    $effective_enable_cache := $effective_enable_cache AND $frozen_at IS NULL; -- frozen queries bypass cache

    if $effective_enable_cache {
        -- we use after as from, because if we have data for that, it automatically means
        -- that we can answer this query
        $effective_enable_cache := helper_check_cache($data_provider, $stream_id, $after, NULL, NULL);
    }

    -- If using cache, get the earliest cached record
    if $effective_enable_cache {
        -- Get cached data from the after time and return the earliest
        for $row in tn_cache.get_cached_first_after($data_provider, $stream_id, $after, NULL) {
            RETURN NEXT $row.event_time, $row.value;
        }
        RETURN;
    }

    $earliest_event_time INT8;

    /*
     * Step 2: Recursively gather all children (ignoring overshadow),
     *         then identify primitive leaves.
     */
    for $row in WITH RECURSIVE descendants AS (
      SELECT t.child_stream_ref
      FROM taxonomies t
      WHERE t.stream_ref = $stream_ref AND t.disabled_at IS NULL
      UNION
      SELECT t.child_stream_ref
      FROM taxonomies t
      JOIN descendants d ON t.stream_ref = d.child_stream_ref
      WHERE t.disabled_at IS NULL
    ),
    primitive_leaves AS (
      SELECT DISTINCT s.id AS stream_ref
      FROM streams s
      WHERE s.stream_type = 'primitive'
        AND s.id IN (SELECT child_stream_ref FROM descendants)
    ),
    /*
     * Step 3: In each primitive, pick the single earliest event_time >= effective_after.
     *         Apply frozen mechanism for truflation_created_at ordering.
     */
    earliest_events AS (
      SELECT
        pl.stream_ref,
        pe_wrap.event_time,
        pe_wrap.value,
        pe_wrap.created_at,
        ROW_NUMBER() OVER (
          PARTITION BY pl.stream_ref
          ORDER BY
            pe_wrap.event_time ASC,
            CASE WHEN pe_wrap.truf_ts > $effective_frozen_at THEN 0 ELSE 1 END,
            CASE WHEN pe_wrap.truf_ts > $effective_frozen_at THEN pe_wrap.truf_ts ELSE NULL END ASC,
            CASE WHEN pe_wrap.truf_ts <= $effective_frozen_at THEN pe_wrap.truf_ts ELSE NULL END DESC,
            pe_wrap.created_at DESC
        ) AS rn
      FROM primitive_leaves pl
      JOIN (
        SELECT
          pe.stream_ref,
          pe.event_time,
          pe.value,
          pe.created_at,
          parse_unix_timestamp(pe.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 AS truf_ts
        FROM primitive_events pe
      ) pe_wrap
        ON pe_wrap.stream_ref = pl.stream_ref
      WHERE pe_wrap.event_time >= $effective_after
    ),
    earliest_values AS (
      /* Step 4: Filter to rn=1 => the single earliest event per stream_ref */
      SELECT
        stream_ref,
        event_time,
        value
      FROM earliest_events
      WHERE rn = 1
    ),
    global_min AS (
      /* Step 5: Find the minimum event_time among all leaves */
      SELECT MIN(event_time) AS earliest_time
      FROM earliest_values
    )
    /* Step 6: Return the row(s) matching that global earliest_time (pick first) */
    SELECT
      ev.event_time,
      ev.value::NUMERIC(36,18)
    FROM earliest_values ev
    JOIN global_min gm
      ON ev.event_time = gm.earliest_time
    {
        $earliest_event_time := $row.event_time;
        break;  -- break out after storing
    }

    /*
     * Step 7: If we have earliest_event_time, call truflation_get_record_composed() at
     *          [earliest_event_time, earliest_event_time].
     */
    IF $earliest_event_time IS DISTINCT FROM NULL {
        for $row in truflation_get_record_composed($data_provider, $stream_id, $earliest_event_time, $earliest_event_time, $frozen_at, $use_cache) {
            return next $row.event_time, $row.value;
            break;
        }
    }
};

CREATE OR REPLACE ACTION truflation_get_index_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $base_time INT8,
    $use_cache BOOL DEFAULT false
) PRIVATE VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    $lower_caller  := LOWER(@caller);
    $max_int8 := 9223372036854775000;
    $effective_from := COALESCE($from, 0);
    $effective_to := COALESCE($to, $max_int8);
    $effective_frozen_at := COALESCE($frozen_at, $max_int8);

    -- Resolve referenced stream once for anchor filters
    $stream_ref := get_stream_id($data_provider, $stream_id);

    -- Fail-fast guard: ensure stream exists before calling _core function
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Base time determination: Use parameter, metadata, or first event time.
    $effective_base_time INT8;
    if $base_time is not null {
        $effective_base_time := $base_time;
    } else {
        $effective_base_time := get_latest_metadata_int_core($stream_ref, 'default_base_time');
    }
    $effective_base_time := COALESCE($effective_base_time, 0);

    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('Invalid time range: from (%s) > to (%s)', $from, $to));
    }

    -- Permissions check
    IF !is_allowed_to_read_all_core($stream_ref, $lower_caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }
    IF !is_allowed_to_compose_all_core($stream_ref, $from, $to) {
        ERROR('Not allowed to compose stream');
    }

    -- Set default value for enable_cache
    $effective_enable_cache := COALESCE($use_cache, false);
    $effective_enable_cache := $effective_enable_cache AND $frozen_at IS NULL; -- frozen queries bypass cache

    if $effective_enable_cache {
        -- Check if we have pre-calculated index values in cache
        $effective_enable_cache := helper_check_cache($data_provider, $stream_id, $from, $to, $effective_base_time);
    }

    -- If using pre-calculated index cache, return directly
    if $effective_enable_cache {
        for $row in tn_cache.get_cached_index_data($data_provider, $stream_id, $from, $to, $effective_base_time) {
            RETURN NEXT $row.event_time, $row.value;
        }
        RETURN;
    }

    -- If both $from and $to are NULL, we find the latest event time
    IF $from IS NULL AND $to IS NULL {
        $actual_latest_event_time INT8;
        $found_latest_event BOOLEAN := FALSE;

        FOR $last_record_row IN truflation_last_rc_composed($data_provider, $stream_id, NULL, $effective_frozen_at, $use_cache) {
            $actual_latest_event_time := $last_record_row.event_time;
            $found_latest_event := TRUE;
            BREAK;
        }

        IF $found_latest_event {
            $effective_from := $actual_latest_event_time;
            $effective_to   := $actual_latest_event_time;
        } ELSE {
            RETURN;
        }
    }

    -- Get the base value for index calculation (non-cache path)
    $base_value := truflation_get_base_value($data_provider, $stream_id, $effective_base_time, $effective_frozen_at, false);

    RETURN WITH RECURSIVE
    /*----------------------------------------------------------------------
    * ANCHOR CTE: Compute the anchor time once for reuse throughout the query
    *
    * Purpose: Find the latest taxonomy start_time at or before $effective_from
    * for the target stream to establish a baseline for filtering.
    *---------------------------------------------------------------------*/
    anchor AS (
        SELECT COALESCE(
            (SELECT t_anchor_base.start_time FROM taxonomies t_anchor_base
             WHERE t_anchor_base.stream_ref = $stream_ref
               AND t_anchor_base.disabled_at IS NULL AND t_anchor_base.start_time <= $effective_from
             ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC LIMIT 1),
            0 -- Default anchor to 0
        ) AS anchor_time
    ),

    /*----------------------------------------------------------------------
    * RELEVANT_PARENTS CTE: Find all streams in the subtree of the target stream
    *---------------------------------------------------------------------*/
    relevant_parents AS (
        -- Base case: the target stream itself
        SELECT $stream_ref AS stream_ref
        UNION ALL
        -- Recursive case: traverse descendants (children of already discovered streams)
        SELECT DISTINCT t.child_stream_ref
        FROM taxonomies t
        JOIN relevant_parents rp ON t.stream_ref = rp.stream_ref
        WHERE t.disabled_at IS NULL
    ),

    parent_distinct_start_times AS (
        SELECT DISTINCT
            t.stream_ref,
            t.start_time
        FROM taxonomies t
        JOIN relevant_parents rp ON t.stream_ref = rp.stream_ref
        WHERE t.disabled_at IS NULL
    ),

    parent_next_starts AS (
        SELECT
            stream_ref,
            start_time,
            LEAD(start_time) OVER (PARTITION BY stream_ref ORDER BY start_time) as next_start_time
        FROM parent_distinct_start_times
    ),

    taxonomy_true_segments AS (
        SELECT
            tx.stream_ref,
            tx.child_stream_ref,
            tx.weight AS weight_for_segment,
            tx.start_time AS segment_start,
            COALESCE(pns.next_start_time, $max_int8) - 1::INT8 AS segment_end
        FROM (
            SELECT
                t.stream_ref,
                t.child_stream_ref,
                t.weight,
                t.start_time,
                t.group_sequence
            FROM taxonomies t
            WHERE t.disabled_at IS NULL
        ) tx
        JOIN (
            SELECT
                t.stream_ref,
                t.start_time,
                MAX(t.group_sequence) as max_gs
            FROM taxonomies t
            WHERE t.disabled_at IS NULL
            GROUP BY t.stream_ref, t.start_time
        ) max_gs_filter
            ON tx.stream_ref = max_gs_filter.stream_ref
           AND tx.start_time = max_gs_filter.start_time
           AND tx.group_sequence = max_gs_filter.max_gs
        JOIN parent_next_starts pns
          ON tx.stream_ref = pns.stream_ref
         AND tx.start_time = pns.start_time
    ),

    hierarchy AS (
      SELECT
          tts.stream_ref AS root_stream_ref,
          tts.child_stream_ref AS descendant_stream_ref,
          tts.weight_for_segment AS raw_weight,
          tts.weight_for_segment AS effective_weight,
          tts.segment_start AS path_start,
          tts.segment_end AS path_end,
          1 AS level
      FROM taxonomy_true_segments tts
      WHERE tts.stream_ref = $stream_ref
        AND tts.segment_end >= (SELECT anchor_time FROM anchor)
        AND tts.segment_start <= $effective_to

      UNION ALL

      SELECT
          h.root_stream_ref,
          tts.child_stream_ref AS descendant_stream_ref,
          (h.raw_weight * tts.weight_for_segment)::NUMERIC(36,18) AS raw_weight,
          (h.effective_weight * (
              tts.weight_for_segment /
              NULLIF((
                  SELECT SUM(sibling_tts.weight_for_segment)
                  FROM taxonomy_true_segments sibling_tts
                  WHERE sibling_tts.stream_ref = h.descendant_stream_ref
                    AND sibling_tts.segment_start = tts.segment_start
                    AND sibling_tts.segment_end = tts.segment_end
              )::NUMERIC(36,18), 0::NUMERIC(36,18))
          ))::NUMERIC(36,18) AS effective_weight,
          GREATEST(h.path_start, tts.segment_start) AS path_start,
          LEAST(h.path_end, tts.segment_end) AS path_end,
          h.level + 1
      FROM
          hierarchy h
      JOIN taxonomy_true_segments tts
          ON h.descendant_stream_ref = tts.stream_ref
      WHERE
          GREATEST(h.path_start, tts.segment_start) <= LEAST(h.path_end, tts.segment_end)
          AND LEAST(h.path_end, tts.segment_end) >= (SELECT anchor_time FROM anchor)
          AND GREATEST(h.path_start, tts.segment_start) <= $effective_to
          AND h.level < 100 -- Recursion depth limit to prevent taxonomy recursion attacks
    ),

    hierarchy_primitive_paths AS (
      SELECT
          h.descendant_stream_ref AS primitive_stream_ref,
          h.effective_weight AS raw_weight,
          h.path_start AS group_sequence_start,
          h.path_end AS group_sequence_end
      FROM hierarchy h
      WHERE EXISTS (
          SELECT 1 FROM streams s
          WHERE s.id = h.descendant_stream_ref
            AND s.stream_type = 'primitive'
      )
    ),

    primitive_weights AS (
      SELECT
          hpp.primitive_stream_ref,
          hpp.raw_weight,
          hpp.group_sequence_start,
          hpp.group_sequence_end
      FROM hierarchy_primitive_paths hpp
    ),

    cleaned_event_times AS (
        SELECT DISTINCT event_time
        FROM (
            SELECT pe.event_time
            FROM primitive_events pe
            JOIN primitive_weights pwr
              ON pe.stream_ref = pwr.primitive_stream_ref
             AND pe.event_time >= pwr.group_sequence_start
             AND pe.event_time <= pwr.group_sequence_end
            WHERE pe.event_time > $effective_from
              AND pe.event_time <= $effective_to

            UNION

            SELECT pwr.group_sequence_start AS event_time
            FROM primitive_weights pwr
            WHERE pwr.group_sequence_start > $effective_from
              AND pwr.group_sequence_start <= $effective_to
        ) all_times_in_range

        UNION

        SELECT event_time FROM (
            SELECT event_time
            FROM (
                SELECT pe.event_time
                FROM primitive_events pe
                JOIN primitive_weights pwr
                  ON pe.stream_ref = pwr.primitive_stream_ref
                 AND pe.event_time >= pwr.group_sequence_start
                 AND pe.event_time <= pwr.group_sequence_end
                WHERE pe.event_time <= $effective_from

                UNION

                SELECT pwr.group_sequence_start AS event_time
                FROM primitive_weights pwr
                WHERE pwr.group_sequence_start <= $effective_from

            ) all_times_before
            ORDER BY event_time DESC
            LIMIT 1
        ) as anchor_event
    ),

    -- Modified initial_primitive_states with frozen mechanism
    initial_primitive_states AS (
        SELECT
            pe.stream_ref,
            pe.event_time,
            pe.value
        FROM (
            SELECT
                pe_inner.stream_ref,
                pe_inner.event_time,
                pe_inner.value,
                ROW_NUMBER() OVER (
                    PARTITION BY pe_inner.stream_ref
                    ORDER BY 
                        pe_inner.event_time DESC,
                        CASE WHEN pe_inner.truf_ts > $effective_frozen_at THEN 0 ELSE 1 END,
                        CASE WHEN pe_inner.truf_ts > $effective_frozen_at THEN pe_inner.truf_ts ELSE NULL END ASC,
                        CASE WHEN pe_inner.truf_ts <= $effective_frozen_at THEN pe_inner.truf_ts ELSE NULL END DESC,
                        pe_inner.created_at DESC
                ) as rn
            FROM (
                SELECT
                    pe_calc.stream_ref,
                    pe_calc.event_time,
                    pe_calc.value,
                    pe_calc.created_at,
                    parse_unix_timestamp(pe_calc.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 AS truf_ts
                FROM primitive_events pe_calc
            ) pe_inner
            WHERE pe_inner.event_time <= $effective_from
              AND EXISTS (
                  SELECT 1 FROM primitive_weights pwr_exists
                  WHERE pwr_exists.primitive_stream_ref = pe_inner.stream_ref
              )
        ) pe
        WHERE pe.rn = 1
    ),

    -- Modified primitive_events_in_interval with frozen mechanism
    primitive_events_in_interval AS (
        SELECT
            pe.stream_ref,
            pe.event_time,
            pe.value
        FROM (
             SELECT
                pe_inner.stream_ref,
                pe_inner.event_time,
                pe_inner.created_at,
                pe_inner.value,
                ROW_NUMBER() OVER (
                    PARTITION BY pe_inner.stream_ref, pe_inner.event_time
                    ORDER BY 
                        CASE WHEN pe_inner.truf_ts > $effective_frozen_at THEN 0 ELSE 1 END,
                        CASE WHEN pe_inner.truf_ts > $effective_frozen_at THEN pe_inner.truf_ts ELSE NULL END ASC,
                        CASE WHEN pe_inner.truf_ts <= $effective_frozen_at THEN pe_inner.truf_ts ELSE NULL END DESC,
                        pe_inner.created_at DESC
                ) as rn
            FROM (
                SELECT
                    pe_calc.stream_ref,
                    pe_calc.event_time,
                    pe_calc.created_at,
                    pe_calc.value,
                    parse_unix_timestamp(pe_calc.truflation_created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 AS truf_ts
                FROM primitive_events pe_calc
            ) pe_inner
            JOIN primitive_weights pwr
               ON pe_inner.stream_ref = pwr.primitive_stream_ref
              AND pe_inner.event_time >= pwr.group_sequence_start
              AND pe_inner.event_time <= pwr.group_sequence_end
            WHERE pe_inner.event_time > $effective_from
              AND pe_inner.event_time <= $effective_to
        ) pe
        WHERE pe.rn = 1
    ),

    all_primitive_points AS (
        SELECT stream_ref, event_time, value FROM initial_primitive_states
        UNION ALL
        SELECT stream_ref, event_time, value FROM primitive_events_in_interval
    ),

    distinct_primitives_for_base AS (
        SELECT DISTINCT stream_ref
        FROM all_primitive_points
    ),

    -- Modified primitive_base_values with frozen mechanism
    primitive_base_values AS (
        SELECT
            dp.stream_ref,
            COALESCE(
                /* exact at base_time */
                (SELECT pe.value FROM (
                    SELECT value,
                           ROW_NUMBER() OVER (
                             ORDER BY
                               CASE WHEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN 0 ELSE 1 END,
                               CASE WHEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 ELSE NULL END ASC,
                               CASE WHEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 <= $effective_frozen_at THEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 ELSE NULL END DESC,
                               created_at DESC
                           ) AS rn
                    FROM primitive_events
                    WHERE stream_ref = dp.stream_ref
                      AND event_time = $effective_base_time
                ) pe WHERE pe.rn = 1),
                /* latest before base_time */
                (SELECT pe.value FROM (
                    SELECT value,
                           ROW_NUMBER() OVER (
                             ORDER BY
                               event_time DESC,
                               CASE WHEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN 0 ELSE 1 END,
                               CASE WHEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 ELSE NULL END ASC,
                               CASE WHEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 <= $effective_frozen_at THEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 ELSE NULL END DESC,
                               created_at DESC
                           ) AS rn
                    FROM primitive_events
                    WHERE stream_ref = dp.stream_ref
                      AND event_time < $effective_base_time
                ) pe WHERE pe.rn = 1),
                /* earliest after base_time */
                (SELECT pe.value FROM (
                    SELECT value,
                           ROW_NUMBER() OVER (
                             ORDER BY
                               event_time ASC,
                               CASE WHEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN 0 ELSE 1 END,
                               CASE WHEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 > $effective_frozen_at THEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 ELSE NULL END ASC,
                               CASE WHEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 <= $effective_frozen_at THEN parse_unix_timestamp(truflation_created_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')::INT8 ELSE NULL END DESC,
                               created_at DESC
                           ) AS rn
                    FROM primitive_events
                    WHERE stream_ref = dp.stream_ref
                      AND event_time > $effective_base_time
                ) pe WHERE pe.rn = 1),
                1::numeric(36,18)
            )::numeric(36,18) AS base_value
        FROM distinct_primitives_for_base dp
    ),

    -- The rest remains the same since it's just calculations on the selected values
    primitive_event_changes AS (
        SELECT
            calc.stream_ref,
            calc.event_time,
            calc.value,
            calc.delta_value,
            CASE
                WHEN COALESCE(pbv.base_value, 0::numeric(36,18)) = 0::numeric(36,18) THEN 0::numeric(36,18)
                ELSE (calc.delta_value * 100::numeric(36,18) / pbv.base_value)::numeric(36,18)
            END AS delta_indexed_value
        FROM (
            SELECT stream_ref, event_time, value,
                   COALESCE(value - LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time), value)::numeric(36,18) AS delta_value
            FROM all_primitive_points
        ) calc
        JOIN primitive_base_values pbv
          ON calc.stream_ref = pbv.stream_ref
        WHERE calc.delta_value != 0::numeric(36,18)
    ),

    first_value_times AS (
        SELECT
            stream_ref,
            MIN(event_time) as first_value_time
        FROM all_primitive_points
        GROUP BY stream_ref
    ),

    effective_weight_changes AS (
        SELECT
            pwr.primitive_stream_ref AS stream_ref,
            GREATEST(pwr.group_sequence_start, fvt.first_value_time) AS event_time,
            pwr.raw_weight AS weight_delta
        FROM primitive_weights pwr
        JOIN first_value_times fvt
            ON pwr.primitive_stream_ref = fvt.stream_ref
        WHERE GREATEST(pwr.group_sequence_start, fvt.first_value_time) <= pwr.group_sequence_end
          AND pwr.raw_weight != 0::numeric(36,18)

        UNION ALL

        SELECT
            pwr.primitive_stream_ref AS stream_ref,
            (pwr.group_sequence_end + 1::INT8) AS event_time,
            -pwr.raw_weight AS weight_delta
        FROM primitive_weights pwr
        JOIN first_value_times fvt
            ON pwr.primitive_stream_ref = fvt.stream_ref
        WHERE
            GREATEST(pwr.group_sequence_start, fvt.first_value_time) <= pwr.group_sequence_end
            AND pwr.raw_weight != 0::numeric(36,18)
            AND pwr.group_sequence_end < ($max_int8 - 1::INT8)
    ),

    unified_events AS (
        SELECT
            pec.stream_ref,
            pec.event_time,
            pec.delta_indexed_value,
            0::numeric(36,18) AS weight_delta,
            1 AS event_type_priority
        FROM primitive_event_changes pec

        UNION ALL

        SELECT
            ewc.stream_ref,
            ewc.event_time,
            0::numeric(36,18) AS delta_indexed_value,
            ewc.weight_delta,
            2 AS event_type_priority
        FROM effective_weight_changes ewc
    ),

    primitive_state_timeline AS (
        SELECT
            stream_ref,
            event_time,
            delta_indexed_value,
            weight_delta,
            COALESCE(LAG(indexed_value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as indexed_value_before_event,
            COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as weight_before_event
        FROM (
            SELECT
                stream_ref,
                event_time,
                delta_indexed_value,
                weight_delta,
                event_type_priority,
                (SUM(delta_indexed_value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as indexed_value_after_event,
                (SUM(weight_delta) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as weight_after_event
            FROM unified_events
        ) state_calc
    ),

    final_deltas AS (
        SELECT
            event_time,
            SUM(
                (delta_indexed_value * weight_before_event) +
                (indexed_value_before_event * weight_delta) +
                (delta_indexed_value * weight_delta)
            )::numeric(72, 18) AS delta_ws_indexed,
            SUM(weight_delta)::numeric(36, 18) AS delta_sw
        FROM primitive_state_timeline
        GROUP BY event_time
        HAVING SUM(
                (delta_indexed_value * weight_before_event) +
                (indexed_value_before_event * weight_delta) +
                (delta_indexed_value * weight_delta)
            )::numeric(72, 18) != 0::numeric(72, 18)
            OR SUM(weight_delta)::numeric(36, 18) != 0::numeric(36, 18)
    ),

    all_combined_times AS (
        SELECT time_point FROM (
            SELECT event_time as time_point FROM final_deltas
            UNION
            SELECT event_time as time_point FROM cleaned_event_times
        ) distinct_times
    ),

    cumulative_values AS (
        SELECT
            act.time_point as event_time,
            (COALESCE((SUM(fd.delta_ws_indexed) OVER (ORDER BY act.time_point ASC))::numeric(72,18), 0::numeric(72,18))) as cum_ws_indexed,
            (COALESCE((SUM(fd.delta_sw) OVER (ORDER BY act.time_point ASC))::numeric(36,18), 0::numeric(36,18))) as cum_sw
        FROM all_combined_times act
        LEFT JOIN final_deltas fd ON fd.event_time = act.time_point
    ),

    aggregated AS (
        SELECT cv.event_time,
               CASE WHEN cv.cum_sw = 0::numeric(36,18) THEN 0::numeric(72,18)
                    ELSE cv.cum_ws_indexed / cv.cum_sw::numeric(72,18)
                   END AS value
        FROM cumulative_values cv
    ),

    real_change_times AS (
        -- 1. Times where the *aggregated* value definitively changes (non-zero deltas)
        SELECT DISTINCT event_time AS time_point
        FROM final_deltas

        UNION

        -- 2. Times where a primitive emits an event inside the requested interval, even if
        --    the emitted value is identical to its previous value (delta == 0). These are
        --    required emission points for the composed stream.
        SELECT DISTINCT event_time
        FROM primitive_events_in_interval
    ),
    
    anchor_time_calc AS (
        SELECT MAX(time_point) as anchor_time
        FROM real_change_times
        WHERE time_point < $effective_from
    ),
    
    final_mapping AS (
        SELECT agg.event_time, agg.value,
               (SELECT MAX(rct.time_point) FROM real_change_times rct WHERE rct.time_point <= agg.event_time) AS effective_time,
               EXISTS (SELECT 1 FROM real_change_times rct WHERE rct.time_point = agg.event_time) AS query_time_had_real_change
        FROM aggregated agg
    ),
    
    filtered_mapping AS (
        SELECT fm.*
        FROM final_mapping fm
                 JOIN anchor_time_calc atc ON 1=1
        WHERE
            (fm.event_time >= $effective_from AND fm.event_time <= $effective_to)
            OR
            (atc.anchor_time IS NOT NULL AND fm.event_time = atc.anchor_time)
    ),

    range_check AS (
        SELECT EXISTS (
            SELECT 1 FROM final_mapping fm_check
            WHERE fm_check.event_time >= $effective_from
              AND fm_check.event_time <= $effective_to
        ) AS range_has_direct_hits
    ),

    locf_applied AS (
        SELECT
            fm.*,
            rc.range_has_direct_hits,
            atc.anchor_time,
            CASE
                WHEN fm.query_time_had_real_change THEN fm.event_time
                ELSE fm.effective_time
            END as final_event_time
        FROM filtered_mapping fm
        JOIN range_check rc ON 1=1
        JOIN anchor_time_calc atc ON 1=1
    ),

    direct_hits AS (
        SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
        FROM locf_applied la
        WHERE la.event_time >= $effective_from
          AND la.event_time <= $effective_to
          AND la.final_event_time IS NOT NULL
    ),
    
    anchor_hit AS (
      SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
      FROM locf_applied la
      WHERE la.anchor_time IS NOT NULL
        AND la.event_time = la.anchor_time
        AND $effective_from > la.anchor_time
        AND la.final_event_time IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM locf_applied dh
            WHERE dh.event_time = $effective_from
        )
    ),
    
    result AS (
        SELECT event_time, value FROM direct_hits
        UNION ALL
        SELECT event_time, value FROM anchor_hit
    )
    SELECT DISTINCT event_time, value FROM result
    ORDER BY 1;
};
