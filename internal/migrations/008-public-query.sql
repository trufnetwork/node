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
    $data_provider  := LOWER($data_provider);
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
    $data_provider  := LOWER($data_provider);
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        -- unfortunately, using the query directly creates error, then we use return next
        for $row in get_last_record_primitive($data_provider, $stream_id, $before, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        -- unfortunately, using the query directly creates error, then we use return next
        for $row in get_last_record_composed($data_provider, $stream_id, $before, $frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
        }
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
    $data_provider  := LOWER($data_provider);
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
    $data_provider  := LOWER($data_provider);
    -- Check if the stream is primitive or composed
    $is_primitive BOOL := is_primitive_stream($data_provider, $stream_id);
    
    -- Route to the appropriate internal action
    if $is_primitive {
        for $row in get_index_primitive($data_provider, $stream_id, $from, $to, $frozen_at, $base_time) {
            RETURN NEXT $row.event_time, $row.value;
        }
    } else {
        for $row in get_index_composed($data_provider, $stream_id, $from, $to, $frozen_at, $base_time) {
            RETURN NEXT $row.event_time, $row.value;
        }
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
    $data_provider  := LOWER($data_provider);
    /*
     * 1. Parameter checks
     */
    
    IF $time_interval IS NULL {
        ERROR('time_interval is required');
    }

    -- Current arrays
    $current_dates  := []::INT8[];
    $current_values := []::NUMERIC(36,18)[];
    $current_count  := 0;

    -- Prev arrays
    $prev_dates  := []::INT8[];
    $prev_values := []::NUMERIC(36,18)[];
    $prev_count  := 0;

    /*
     * 3. Gather CURRENT data from get_index(...) into $current_*
     */
    
    FOR $row IN get_index($data_provider, $stream_id, $from, $to, $frozen_at, $base_time) {
        -- Bump the counter (use 1-based indexing for arrays)
        $current_dates := array_append($current_dates, $row.event_time);
        $current_values := array_append($current_values, $row.value);
    }

    $current_count := array_length($current_dates);

    IF $current_count = 0 {
        -- No current data => no output
        RETURN;
    }

    /*
     * 4. We know we'll need "previous" data from earliest to latest possible.
     *    The earliest needed is: min(current_date) - time_interval
     *    The latest  needed is: max(current_date) - time_interval
     *
     *    Because get_index returns ascending times, we can gather them in one pass.
     */

    $earliest_needed := $current_dates[1]  - ($time_interval)::INT8;
    $latest_needed   := $current_dates[$current_count] - ($time_interval)::INT8;

    -- If the user passed $from and $to, it's possible earliest_needed < from. 
    -- We can use that or just rely on earliest_needed < to. 
    -- We'll just do the direct range here:
    FOR $row IN get_index($data_provider, $stream_id, $earliest_needed, $latest_needed, $frozen_at, $base_time) {
        $prev_dates := array_append($prev_dates, $row.event_time);
        $prev_values := array_append($prev_values, $row.value);
    }

    $prev_count := array_length($prev_dates);

    IF $prev_count = 0 {
        -- If no previous data at all, then there's nothing to compare => no output
        RETURN;
    }

    /*
     * 5. "Two-pointer" pass:
     *    - i => index in current arrays  (1..$current_count)
     *    - j => index in prev arrays     (1..$prev_count)
     *
     *    Move i from 1..$current_count.
     *    For each i, move j forward while the next item is still ≤ $target
     *    stops once j+1 would exceed that threshold.
     *
     *    Then prev_dates[j] is the "best match" if it's ≤ that target.
     */

    $j := 1;  -- pointer for the prev arrays
    
    $matches_found := 0;
    $matches_skipped_prev_gt_target := 0;
    $matches_skipped_zero_value := 0;

    FOR $i IN 1..$current_count {
        $target := $current_dates[$i] - ($time_interval)::INT8;
        
        -- Move j forward while the next item is still ≤ $target
        FOR $k IN $j..$prev_count {
            $next_index := $k + 1;
            -- interpreter can't support short circuiting here
            -- then we split the ifs to avoid out of bounds errors
            IF $k < $prev_count {
                IF $prev_dates[$next_index] <= $target {
                    $j := $next_index;
                } ELSE {
                    BREAK;  -- we've gone as far as we can
                }
            } ELSE {
                BREAK;  -- we've gone as far as we can
            }
        }
        
        -- Check if the found prev_date is <= target
        IF $j <= $prev_count AND $prev_dates[$j] <= $target {
            IF $prev_values[$j] != 0::NUMERIC(36,18) {
                $change := (
                    ($current_values[$i] - $prev_values[$j])
                    * 100::NUMERIC(36,18)
                ) / $prev_values[$j];
                
                RETURN NEXT $current_dates[$i], $change;
                $matches_found := $matches_found + 1;
            } ELSE {
                $matches_skipped_zero_value := $matches_skipped_zero_value + 1;
            }
        } ELSE {
            $matches_skipped_prev_gt_target := $matches_skipped_prev_gt_target + 1;
        }
    }
    
}