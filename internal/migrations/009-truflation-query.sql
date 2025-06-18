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
    -- Check read access first
    if !is_allowed_to_read($data_provider, $stream_id, $lower_caller, 0, 0) {
        ERROR('wallet not allowed to read');
    }

    -- Ensure that the stream is a primitive stream
    if !is_primitive_stream($data_provider, $stream_id) {
        ERROR('stream is not a primitive stream');
    }

    RETURN SELECT truflation_created_at
           FROM primitive_events
           WHERE data_provider = $data_provider
             AND stream_id = $stream_id
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

    -- Check stream existence in batch
    for $row in stream_exists_batch($data_provider, $stream_id) {
        if !$row.stream_exists {
            ERROR('stream does not exist: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Check if streams are primitive in batch
    for $row in is_primitive_stream_batch($data_provider, $stream_id) {
        if !$row.is_primitive {
            ERROR('stream is not a primitive stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Validate that the wallet is allowed to write to each stream
    for $row in is_wallet_allowed_to_write_batch($data_provider, $stream_id, $lower_caller) {
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
            $stream_id AS stream_ids,
            $data_provider AS data_providers,
            $event_time AS event_times,
            $value AS values_array,
            $truflation_created_at AS truflation_created_at_array
    ),
    arguments AS (
        SELECT
            record_arrays.stream_ids[idx] AS stream_id,
            record_arrays.data_providers[idx] AS data_provider,
            record_arrays.event_times[idx] AS event_time,
            record_arrays.values_array[idx] AS value,
            record_arrays.truflation_created_at_array[idx] AS truflation_created_at
        FROM indexes
        JOIN record_arrays ON 1=1
    )
    INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at, truflation_created_at)
    SELECT
        stream_id,
        data_provider,
        event_time,
        value,
        $current_block,
        truflation_created_at
    FROM arguments;
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
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    
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
        WHERE pe.data_provider = $data_provider
            AND pe.stream_id = $stream_id
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
                data_provider = $data_provider
                AND stream_id = $stream_id
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
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);

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
            WHERE data_provider = $data_provider
                AND stream_id = $stream_id
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
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    
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
            WHERE data_provider = $data_provider
                AND stream_id = $stream_id
                AND event_time >= $effective_after
        ) pe
        WHERE pe.rn = 1
        LIMIT 1;
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

    -- Get the base value (will use frozen mechanism through truflation_get_base_value)
    $base_value NUMERIC(36,18) := truflation_get_base_value($data_provider, $stream_id, $effective_base_time, $frozen_at);

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
    $frozen_at INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        for $row in truflation_get_record_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        for $row in truflation_get_record_composed($data_provider, $stream_id, $from, $to, $frozen_at) {
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
    $frozen_at INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        -- unfortunately, using the query directly creates error, then we use return next
        for $row in truflation_last_rc_primitive($data_provider, $stream_id, $before, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        -- unfortunately, using the query directly creates error, then we use return next
        for $row in truflation_last_rc_composed($data_provider, $stream_id, $before, $frozen_at) {
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
    $frozen_at INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    $data_provider  := LOWER($data_provider);
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);

    -- Route to the appropriate internal action
    if $is_primitive {
        for $row in truflation_first_rc_primitive($data_provider, $stream_id, $after, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        for $row in truflation_first_rc_composed($data_provider, $stream_id, $after, $frozen_at) {
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
    $frozen_at INT8
) PUBLIC view returns (value NUMERIC(36,18)) {
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    -- Check read permissions
    if !is_allowed_to_read_all($data_provider, $stream_id, $lower_caller, NULL, $base_time) {
        ERROR('Not allowed to read stream');
    }
    
    -- If base_time is null, try to get it from metadata
    $effective_base_time INT8 := $base_time;
    if $effective_base_time IS NULL {
        -- First try to get base_time from metadata
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
        
        -- If still null after checking metadata, get the first ever record
        if !$found_metadata OR $effective_base_time IS NULL {
            $found_value NUMERIC(36,18);
            $found := FALSE;
            
            -- Execute the function and store results in variables
            $first_time INT8;
            $first_value NUMERIC(36,18);
            for $record in truflation_get_first_record($data_provider, $stream_id, NULL, $frozen_at) {
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
    for $row in truflation_get_record($data_provider, $stream_id, $effective_base_time, $effective_base_time, $frozen_at) {
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
    for $row in truflation_get_last_record($data_provider, $stream_id, $effective_base_time, $frozen_at) {
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
    for $row in truflation_get_first_record($data_provider, $stream_id, $effective_base_time, $frozen_at) {
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