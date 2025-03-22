/**
 * get_record: Public facade for retrieving time series data.
 * Routes to primitive or composed implementation based on stream type.
 */
CREATE OR REPLACE ACTION get_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        for $row in get_record_primitive($data_provider, $stream_id, $from, $to, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        for $row in get_record_composed($data_provider, $stream_id, $from, $to, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    }
};

/**
 * get_last_record: Retrieves the most recent record before a timestamp.
 * Routes to primitive or composed implementation based on stream type.
 */
CREATE OR REPLACE ACTION get_last_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        RETURN get_last_record_primitive($data_provider, $stream_id, $before, $frozen_at);
    } else {
        RETURN get_last_record_composed($data_provider, $stream_id, $before, $frozen_at);
    }
};

/**
 * get_first_record: Retrieves the earliest record after a timestamp.
 * Routes to primitive or composed implementation based on stream type.
 */
CREATE OR REPLACE ACTION get_first_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,
    $frozen_at INT8
) PUBLIC view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);

    -- Route to the appropriate internal action
    if $is_primitive {
        for $row in get_first_record_primitive($data_provider, $stream_id, $after, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        for $row in get_first_record_composed($data_provider, $stream_id, $after, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    }
};

/**
 * get_base_value: Retrieves reference value for index calculations.
 * Routes to primitive or composed implementation based on stream type.
 */
CREATE OR REPLACE ACTION get_base_value(
    $data_provider TEXT,
    $stream_id TEXT,
    $base_time INT8,
    $frozen_at INT8
) PUBLIC view returns (value NUMERIC(36,18)) {
    -- Check read permissions
    if !is_allowed_to_read_all($data_provider, $stream_id, @caller, NULL, $base_time) {
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
            for $record in get_first_record($data_provider, $stream_id, NULL, $frozen_at) {
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
    for $row in get_record($data_provider, $stream_id, $effective_base_time, $effective_base_time, $frozen_at) {
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
    for $row in get_last_record($data_provider, $stream_id, $effective_base_time, $frozen_at) {
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
    for $row in get_first_record($data_provider, $stream_id, $effective_base_time, $frozen_at) {
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

/**
 * get_index: Calculates indexed values relative to a base value.
 * Routes to primitive or composed implementation based on stream type.
 */
CREATE OR REPLACE ACTION get_index(
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
        -- Check read permissions
    if !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }
    
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

    -- Get the base value
    $base_value NUMERIC(36,18) := get_base_value($data_provider, $stream_id, $effective_base_time, $frozen_at);

    -- Check if base value is zero to avoid division by zero
    if $base_value = 0::NUMERIC(36,18) {
        ERROR('base value is 0');
    }

    -- Calculate the index for each record through loop and RETURN NEXT
    -- This avoids nested SQL queries and uses proper action calling patterns
    for $record in get_record($data_provider, $stream_id, $from, $to, $frozen_at) {
        $indexed_value NUMERIC(36,18) := ($record.value * 100::NUMERIC(36,18)) / $base_value;
        RETURN NEXT $record.event_time, $indexed_value;
    }
};

CREATE OR REPLACE ACTION get_index_change(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $base_time INT8,
    $time_interval INT
) PUBLIC VIEW
RETURNS TABLE (
    event_time INT8,
    value NUMERIC(36,18)
)
{
    /*
     * 1. Parameter checks
     */
    IF $time_interval IS NULL {
        ERROR('time_interval is required');
    }

    /*
     * 2. Preallocate arrays to hold up to N rows (avoid array_append in loops).
     *    We'll do a "max_array_support" as an upper bound. If that’s too small,
     *    we may raise it.
     */
    $max_array_support INT := 5000;

    $numeric_array NUMERIC(36,18)[];
    $int_array INT8[];


    -- it's more performant to fill like this than trying to fill with normal loops,
    -- as the interpreter adds additional roundtrips
    for $row_array in 
    WITH RECURSIVE blanks AS (
        SELECT 1 as n, NULL::INT8 as int_value, NULL::NUMERIC(36,18) as num_value
        UNION ALL
        SELECT n + 1, NULL::INT8 as int_value, NULL::NUMERIC(36,18) as num_value
        FROM blanks
        WHERE n < $max_array_support
    )
    SELECT array_agg(blanks.int_value) AS int_array, array_agg(blanks.num_value) AS num_array
    FROM blanks
    {
        -- Only returns one row; store the array
        $int_array := $row_array.int_array;
        $numeric_array := $row_array.num_array;
        break;
    }


    -- Current arrays
    $current_dates  INT8[] := $int_array;
    $current_values NUMERIC(36,18)[] := $numeric_array;
    $current_count  INT := 0;

    -- Prev arrays
    $prev_dates  INT8[] := $int_array;
    $prev_values NUMERIC(36,18)[] := $numeric_array;
    $prev_count  INT := 0;

    $empty_array INT8[];


    /*
     * 3. Gather CURRENT data from get_index(...) into $current_*
     */
    FOR $row IN get_index($data_provider, $stream_id, $from, $to, $frozen_at, $base_time) {
        IF $current_count >= $max_array_support {
            ERROR('Too many current data points; raise max_array_support if needed');
        }
        -- Bump the counter (use 1-based indexing for arrays)
        $current_count := $current_count + 1;
        $current_dates[$current_count]  := $row.event_time;
        $current_values[$current_count] := $row.value;
    }

    IF $current_count = 0 {
        -- No current data => no output
        RETURN;
        ERROR('Code execution error; Should not reach here');
    }

    /*
     * 4. We know we’ll need “previous” data from earliest to latest possible.
     *    The earliest needed is: min(current_date) - time_interval
     *    The latest  needed is: max(current_date) - time_interval
     *
     *    Because get_index returns ascending times, we can gather them in one pass.
     */

    $earliest_needed := $current_dates[1]  - ($time_interval)::INT8;
    $latest_needed   := $current_dates[$current_count] - ($time_interval)::INT8;

    -- If the user passed $from and $to, it’s possible earliest_needed < from. 
    -- We can use that or just rely on earliest_needed < to. 
    -- We'll just do the direct range here:
    FOR $row IN get_index($data_provider, $stream_id, $earliest_needed, $latest_needed, $frozen_at, $base_time) {
        IF $prev_count >= $max_array_support {
            ERROR('Too many previous data points; raise max_array_support if needed');
        }
        $prev_count := $prev_count + 1;
        $prev_dates[$prev_count]  := $row.event_time;
        $prev_values[$prev_count] := $row.value;
    }

    IF $prev_count = 0 {
        -- If no previous data at all, then there's nothing to compare => no output
        RETURN;
        ERROR('Code execution error; Should not reach here');
    }

    /*
     * 5. “Two-pointer” pass:
     *    - i => index in current arrays  (1..$current_count)
     *    - j => index in prev arrays     (1..$prev_count)
     *
     *    Move i from 1..$current_count.
     *    For each i, move j forward while possible so that 
     *      prev_dates[j] <= current_dates[i] - interval
     *    stops once j+1 would exceed that threshold.
     *
     *    Then prev_dates[j] is the “best match” if it’s ≤ that target.
     */

    $j := 1;  -- pointer for the prev arrays

    FOR $i IN 1..$current_count {
        $target := $current_dates[$i] - ($time_interval)::INT8;

        -- Move j forward while the next item is still ≤ $target
        FOR $k IN $j..$prev_count {
            IF $k < $prev_count AND $prev_dates[$k + 1] <= $target {
                $j := $k + 1;  -- we can safely advance
            } ELSE {
                BREAK;  -- we've gone as far as we can
            }
        }

        -- Now if $prev_dates[$j] <= $target, we have a match
        IF $prev_dates[$j] <= $target {
            IF $prev_values[$j] != 0::NUMERIC(36,18) {
                $change := (
                    ($current_values[$i] - $prev_values[$j])
                    * 100::NUMERIC(36,18)
                ) / $prev_values[$j];

                RETURN NEXT $current_dates[$i], $change;
            }
            -- if it’s zero, skip or output NULL
        }
        -- else no valid previous => skip
    }
}

