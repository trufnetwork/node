-- get_record returns the value of the primitive stream for a given date range
-- It fills gaps in the primitive stream by using the last value before the given date
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
    -- Set default values if parameters are null
    if $frozen_at IS NULL {
        $frozen_at := 0;
    }

    notice('Starting get_record_primitive');
    
    $has_results BOOL := false;
    $first_result_ts INT8 := NULL;


    -- Directly iterate over query results
    FOR $row IN get_original_record_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
        -- Original processing logic here
        notice('Processing record from original query');
        notice('Row: ' || $row.event_time::TEXT || ', ' || $row.value::TEXT);
 
        IF $has_results == false {
            $has_results := true;
            $first_result_ts := $row.event_time;
            notice('Found first result');
            
            -- If the first result timestamp is not the same as the start timestamp,
            -- then we need to fetch the last record before it
            IF $first_result_ts != $from AND $from IS NOT NULL {
                notice('First result timestamp does not match from parameter');
                notice('Fetching last record before from timestamp');
                FOR $last_row IN get_last_record_primitive($data_provider, $stream_id, $from, $frozen_at) {
                    notice('Found last record before from timestamp');
                    RETURN NEXT $last_row.event_time, $last_row.value;
                }
            }
        }
        
        notice('Returning record from original query');
        RETURN NEXT $row.event_time, $row.value;
    }
    
    -- If we didn't get any results, try to find the last record before the start timestamp
    IF $has_results == false AND $from IS NOT NULL {
        notice('No results found in original query');
        notice('Attempting to find last record before from timestamp');
        FOR $last_row IN get_last_record($data_provider, $stream_id, $from, $frozen_at) {
            notice('Found last record before from timestamp as fallback');
            RETURN NEXT $last_row.event_time, $last_row.value;
        }
    }
    
    notice('Completed get_record_primitive');
};

-- get_original_record returns the original values of the primitive stream for a given date range
-- It does not fill gaps in the primitive stream
CREATE OR REPLACE ACTION get_original_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
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
    if $from IS NOT NULL {
        if $to IS NOT NULL {
            -- Both from and to are provided
            RETURN SELECT pe.event_time, pe.value 
                FROM primitive_events pe
                WHERE pe.data_provider = $data_provider 
                AND pe.stream_id = $stream_id
                AND pe.event_time >= $from 
                AND pe.event_time <= $to
                AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
                ORDER BY pe.event_time DESC, pe.created_at DESC;
        } ELSE {
            -- Only from is provided
            RETURN SELECT pe.event_time, pe.value 
                FROM primitive_events pe
                WHERE pe.data_provider = $data_provider 
                AND pe.stream_id = $stream_id
                AND pe.event_time >= $from
                AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
                ORDER BY pe.event_time DESC, pe.created_at DESC;
        }
    } else {
        if $to IS NULL {
            -- No from and to provided, fetch only the latest record
            RETURN SELECT pe.event_time, pe.value 
                FROM primitive_events pe
                WHERE pe.data_provider = $data_provider 
                AND pe.stream_id = $stream_id
                AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
                ORDER BY pe.event_time DESC, pe.created_at DESC
                LIMIT 1;
        } else {
            -- to is provided but from is not
            ERROR('from is required if to is provided');
        }
    }
};

-- get_last_record returns the last record before the given timestamp
CREATE OR REPLACE ACTION get_last_record_primitive(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    notice('get_last_record_primitive called with data_provider: ' || $data_provider || ', stream_id: ' || $stream_id);
    -- Check read access, since we're querying directly from the primitive_events table
    if is_wallet_allowed_to_read(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to read');
    }
    
    -- Set default values if parameters are null
    if $frozen_at IS NULL {
        $frozen_at := 0;
    }

    notice('Querying primitive_events table');
    -- for $row in just_produce_for_testing() {
    --     RETURN NEXT $row.event_time, $row.value;
    -- }
    return select event_time, value from primitive_events limit 1 offset 1;
    -- for $row in SELECT pe.event_time, pe.value 
    --     FROM primitive_events pe
    --     WHERE pe.data_provider = $data_provider 
    --     AND pe.stream_id = $stream_id
    --     AND pe.event_time < $before
    --     AND ($frozen_at = 0 OR pe.created_at <= $frozen_at)
    --     ORDER BY pe.event_time DESC, pe.created_at DESC
    --     LIMIT 1 {
    --     notice('Found record with event_time: ' || $row.event_time::TEXT || ', value: ' || $row.value::TEXT);
    --     RETURN NEXT $row.event_time, $row.value;
    -- }
};

CREATE OR REPLACE ACTION just_produce_for_testing() PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    RETURN NEXT 5::INT8, 3.000000000000000000::NUMERIC(36,18);
};

-- get_first_record returns the first record of the primitive stream (optionally after a given timestamp - inclusive)
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
    if is_wallet_allowed_to_read(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to read');
    }
    
    -- Set default values if parameters are null
    if $after IS NULL {
        $after := 0;
    }
    
    if $frozen_at IS NULL {
        $frozen_at := 0;
    }
    
    RETURN SELECT pe.event_time, pe.value 
        FROM primitive_events pe
        WHERE pe.data_provider = $data_provider 
        AND pe.stream_id = $stream_id
        AND pe.event_time >= $after
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
    $from INT8,
    $to INT8,
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
    for $row in get_record_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
        RETURN NEXT $row.event_time, ($row.value * 100) / $base_value;
    }
    return;
};

