-- get_record returns the value of the primitive stream for a given date range
-- It fills gaps in the primitive stream by using the last value before the given date
CREATE OR REPLACE ACTION get_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    ts INT8,
    value NUMERIC(36,18)
) {
    -- Set default values if parameters are null
    if $frozen_at IS NULL {
        $frozen_at := 0;
    }

    -- Get original records first
    $original_records table(ts INT8, value NUMERIC(36,18)) := 
        get_original_record_primitive($data_provider, $stream_id, $from, $to, $frozen_at);
    
    -- Check if we have any results
    $has_results BOOL := false;
    $first_result_ts INT8 := NULL;
    
    for $row in $original_records {
        if $has_results == false {
            $has_results := true;
            $first_result_ts := $row.ts;
            
            -- If the first result timestamp is not the same as the start timestamp,
            -- then we need to fetch the last record before it
            if $first_result_ts != $from_time AND $from_time IS NOT NULL {
                for $last_row in get_last_record_before_ts($data_provider, $stream_id, $from_time, $frozen_at) {
                    RETURN NEXT $last_row.ts, $last_row.value;
                }
            }
        }
        
        RETURN NEXT $row.ts, $row.value;
    }
    
    -- If we didn't get any results, try to find the last record before the start timestamp
    if $has_results == false AND $from_time IS NOT NULL {
        for $last_row in get_last_record_before_ts($data_provider, $stream_id, $from_time, $frozen_at) {
            RETURN NEXT $last_row.ts, $last_row.value;
        }
    }
};

-- get_original_record returns the original values of the primitive stream for a given date range
-- It does not fill gaps in the primitive stream
CREATE OR REPLACE ACTION get_original_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $from_time INT8,
    $to_time INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read access, since we're querying directly from the primitive_events table
    if is_wallet_allowed_to_read(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to read');
    }
    
    -- Set default values if parameters are null
    if $frozen_at IS NULL {
        $frozen_at := 0;
    }
    
    -- Build the query based on provided parameters
    if $from_time IS NOT NULL {
        if $to_time IS NOT NULL {
            -- Both from_time and to_time are provided
            RETURN SELECT pe.ts, pe.value 
                FROM primitive_events pe
                WHERE pe.data_provider = $data_provider 
                AND pe.stream_id = $stream_id
                AND pe.ts >= $from_time 
                AND pe.ts <= $to_time
                AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
                ORDER BY pe.ts DESC, pe.created_at DESC;
        } else {
            -- Only from_time is provided
            RETURN SELECT pe.ts, pe.value 
                FROM primitive_events pe
                WHERE pe.data_provider = $data_provider 
                AND pe.stream_id = $stream_id
                AND pe.ts >= $from_time
                AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
                ORDER BY pe.ts DESC, pe.created_at DESC;
        }
    } else {
        if $to_time IS NULL {
            -- No from_time and to_time provided, fetch only the latest record
            RETURN SELECT pe.ts, pe.value 
                FROM primitive_events pe
                WHERE pe.data_provider = $data_provider 
                AND pe.stream_id = $stream_id
                AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
                ORDER BY pe.ts DESC, pe.created_at DESC
                LIMIT 1;
        } else {
            -- to_time is provided but from_time is not
            ERROR('from_time is required if to_time is provided');
        }
    }
};

-- get_last_record returns the last record before the given timestamp
CREATE OR REPLACE ACTION get_last_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $before_time INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read access, since we're querying directly from the primitive_events table
    if is_wallet_allowed_to_read(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to read');
    }
    
    -- Set default values if parameters are null
    if $frozen_at IS NULL {
        $frozen_at := 0;
    }
    
    RETURN SELECT pe.event_time, pe.value 
        FROM primitive_events pe
        WHERE pe.data_provider = $data_provider 
        AND pe.stream_id = $stream_id
        AND pe.event_time < $before_time
        AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
        ORDER BY pe.event_time DESC, pe.created_at DESC
        LIMIT 1;
};

-- get_first_record returns the first record of the primitive stream (optionally after a given timestamp - inclusive)
CREATE OR REPLACE ACTION get_first_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $after_time INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read access, since we're querying directly from the primitive_events table
    if is_wallet_allowed_to_read(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to read');
    }
    
    -- Set default values if parameters are null
    if $after_time IS NULL {
        $after_time := 0;
    }
    
    if $frozen_at IS NULL {
        $frozen_at := 0;
    }
    
    RETURN SELECT pe.event_time, pe.value 
        FROM primitive_events pe
        WHERE pe.data_provider = $data_provider 
        AND pe.stream_id = $stream_id
        AND pe.event_time >= $after_time
        AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
        ORDER BY pe.event_time ASC, pe.created_at DESC
        LIMIT 1;
};

-- get_base_value returns the first nearest value of the primitive stream before the given timestamp
CREATE OR REPLACE ACTION get_base_value_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $base_time INT8,
    $frozen_at INT8
) PRIVATE view returns (value NUMERIC(36,18)) {
    -- Check read access, since we're querying directly from the primitive_events table
    if is_wallet_allowed_to_read(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to read');
    }
    
    -- Set default values if parameters are null
    if $frozen_at IS NULL {
        $frozen_at := 0;
    }
    
    -- If $base_time is null or zero, return the first-ever value from the primitive stream
    if $base_time IS NULL OR $base_time = 0 {
        for $row in SELECT pe.value 
            FROM primitive_events pe
            WHERE pe.data_provider = $data_provider 
            AND pe.stream_id = $stream_id
            AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
            ORDER BY pe.event_time ASC, pe.created_at DESC 
            LIMIT 1 {
            return $row.value;
        }
    }
    
    -- Try to find a value at or before the base timestamp
    for $row in SELECT pe.value 
        FROM primitive_events pe
        WHERE pe.data_provider = $data_provider 
        AND pe.stream_id = $stream_id
        AND pe.event_time <= $base_time
        AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
        ORDER BY pe.event_time DESC, pe.created_at DESC 
        LIMIT 1 {
        return $row.value;
    }
    
    -- If no value is found before the base timestamp, find the first value after it
    for $row in SELECT pe.value 
        FROM primitive_events pe
        WHERE pe.data_provider = $data_provider 
        AND pe.stream_id = $stream_id
        AND pe.event_time > $base_time
        AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
        ORDER BY pe.event_time ASC, pe.created_at DESC 
        LIMIT 1 {
        return $row.value;
    }
    
    -- If no value is found at all, return an error
    ERROR('no base value found');
};

-- get_index calculation is ((current_primitive/base_primitive)*100)
CREATE OR REPLACE ACTION get_index_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $from_time INT8,
    $to_time INT8,
    $frozen_at INT8,
    $base_time INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read access
    if is_wallet_allowed_to_read(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to read');
    }
    
    -- Set default values if parameters are null
    if $frozen_at IS NULL {
        $frozen_at := 0;
    }
    
    -- If base_time is not provided, try to get it from metadata
    $effective_base_time INT8 := $base_time;
    if $effective_base_time IS NULL OR $effective_base_time = 0 {
        for $row in SELECT value_i 
            FROM metadata 
            WHERE data_provider = $data_provider 
            AND stream_id = $stream_id 
            AND metadata_key = 'default_base_time' 
            AND disabled_at IS NULL
            ORDER BY created_at DESC 
            LIMIT 1 {
            $effective_base_time := $row.value_i;
        }
    }
    
    -- Get the base value
    $base_value NUMERIC(36,18) := get_base_value($data_provider, $stream_id, $effective_base_time, $frozen_at);
    
    -- Check if base value is zero to avoid division by zero
    if $base_value = 0 {
        ERROR('base value is 0');
    }
    
    -- Calculate the index for each record
    RETURN SELECT r.event_time, (r.value * 100) / $base_value AS value 
        FROM get_record($data_provider, $stream_id, $from_time, $to_time, $frozen_at) r;
};

