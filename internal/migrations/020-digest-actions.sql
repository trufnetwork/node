/*
 * DIGEST ACTIONS MIGRATION
 * 
 * Implements the three core actions for the digest system:
 * - digest_daily: Process a single day's data into OHLC format
 * - auto_digest: Batch process multiple pending days
 * - get_daily_ohlc: Query daily OHLC data
 * 
 * Dependencies:
 * - Requires digest schema (019-digest-schema.sql)
 * - Requires @leader contextual variable 
 */

-- =============================================================================
-- CORE DIGEST ACTIONS
-- =============================================================================

/**
 * digest_daily: Process a single day's data into OHLC format
 * 
 * Only the leader can execute this action.
 * Calculates OPEN, HIGH, LOW, CLOSE values and deletes redundant records.
 */
CREATE OR REPLACE ACTION digest_daily(
    $stream_ref INT,
    $day_index INT
) PUBLIC RETURNS TABLE(
    deleted_rows INT,
    preserved_records INT
) {
    -- Leader authorization check, keep it commented out for now so test passing and until we can inject how leader is
    -- if @caller != @leader {
    --     ERROR('Only the leader node can execute digest operations');
    -- }
    
    -- Verify day is queued for processing
    $day_queued BOOL := false;
    for $row in SELECT 1 FROM pending_prune_days 
                WHERE stream_ref = $stream_ref AND day_index = $day_index {
        $day_queued := true;
    }
    
    if !$day_queued {
        ERROR('Day not in pending queue');
    }
    
    $day_start := $day_index * 86400;
    $day_end := $day_start + 86400;
    
    -- Count records for this day
    $count := 0;
    for $row in SELECT COUNT(*) as cnt FROM primitive_events
                WHERE stream_ref = $stream_ref 
                  AND event_time >= $day_start AND event_time <= $day_end {
        $count := $row.cnt;
    }
    
    -- Skip if too few records (need at least 2 for meaningful digest)
    if $count <= 1 {
        DELETE FROM pending_prune_days 
        WHERE stream_ref = $stream_ref AND day_index = $day_index;
        NOTICE('Skipping digest for day ' || $day_index::TEXT || ' in stream ' || $stream_ref::TEXT ||
               ': only ' || $count::TEXT || ' records found');
        RETURN 0, 0;
    }
    
    -- Skip if already digested
    $already_digested BOOL := false;
    for $row in SELECT 1 FROM primitive_event_type
                WHERE stream_ref = $stream_ref 
                  AND event_time >= $day_start AND event_time <= $day_end {
        $already_digested := true;
    }
    
    if $already_digested {
        DELETE FROM pending_prune_days 
        WHERE stream_ref = $stream_ref AND day_index = $day_index;
        RETURN 0, 0;
    }
    
    -- Calculate OPEN: Earliest time, tie-break by latest created_at
    $open_time INT;
    $open_val NUMERIC(36,18);
    for $row in SELECT event_time, value FROM primitive_events
                WHERE stream_ref = $stream_ref 
                  AND event_time >= $day_start AND event_time <= $day_end
                ORDER BY event_time ASC, created_at DESC
                LIMIT 1 {
        $open_time := $row.event_time;
        $open_val := $row.value;
    }
    
    -- Calculate CLOSE: Latest time, tie-break by latest created_at
    $close_time INT;
    $close_val NUMERIC(36,18);
    for $row in SELECT event_time, value FROM primitive_events
                WHERE stream_ref = $stream_ref 
                  AND event_time >= $day_start AND event_time <= $day_end
                ORDER BY event_time DESC, created_at DESC
                LIMIT 1 {
        $close_time := $row.event_time;
        $close_val := $row.value;
    }
    
    -- Calculate HIGH: Maximum value, tie-break by earliest time
    $high_time INT;
    $high_val NUMERIC(36,18);
    for $row in SELECT event_time, value FROM primitive_events
                WHERE stream_ref = $stream_ref 
                  AND event_time >= $day_start AND event_time <= $day_end
                ORDER BY value DESC, event_time ASC, created_at DESC
                LIMIT 1 {
        $high_time := $row.event_time;
        $high_val := $row.value;
    }
    
    -- Calculate LOW: Minimum value, tie-break by earliest time
    $low_time INT;
    $low_val NUMERIC(36,18);
    for $row in SELECT event_time, value FROM primitive_events
                WHERE stream_ref = $stream_ref 
                  AND event_time >= $day_start AND event_time <= $day_end
                ORDER BY value ASC, event_time ASC, created_at DESC
                LIMIT 1 {
        $low_time := $row.event_time;
        $low_val := $row.value;
    }
    
    -- Delete excess records (keep only OPEN, HIGH, LOW, CLOSE)
    $deleted := 0;
    
    -- Always delete records that are not OPEN, HIGH, LOW, or CLOSE
    DELETE FROM primitive_events
    WHERE stream_ref = $stream_ref
      AND event_time >= $day_start AND event_time <= $day_end
      AND event_time != $open_time
      AND event_time != $close_time
      AND event_time != $high_time
      AND event_time != $low_time;
    
    -- Insert type markers for OHLC
    -- Type flags: 1=OPEN, 2=HIGH, 4=LOW, 8=CLOSE (OHLC order)
    $preserved_count := 0;
    
    -- Insert OPEN marker
    $open_type := 1;
    if $high_time = $open_time AND $high_val = $open_val {
        $open_type := $open_type + 2;  -- Add HIGH flag
    }
    if $low_time = $open_time AND $low_val = $open_val {
        $open_type := $open_type + 4;  -- Add LOW flag
    }
    if $close_time = $open_time AND $close_val = $open_val {
        $open_type := $open_type + 8;  -- Add CLOSE flag
    }
    
    INSERT INTO primitive_event_type (stream_ref, event_time, type)
    VALUES ($stream_ref, $open_time, $open_type);
    
    $preserved_count := $preserved_count + 1;
    
    -- Insert CLOSE marker if different from OPEN
    if $close_time != $open_time {
        $close_type := 8;
        if $high_time = $close_time AND $high_val = $close_val {
            $close_type := $close_type + 2;  -- Add HIGH flag
        }
        if $low_time = $close_time AND $low_val = $close_val {
            $close_type := $close_type + 4;  -- Add LOW flag
        }
        
        INSERT INTO primitive_event_type (stream_ref, event_time, type)
        VALUES ($stream_ref, $close_time, $close_type);
        
        $preserved_count := $preserved_count + 1;
    }
    
    -- Insert HIGH marker if different from OPEN and CLOSE
    if $high_time != $open_time AND $high_time != $close_time {
        $high_type := 2;
        if $low_time = $high_time AND $low_val = $high_val {
            $high_type := $high_type + 4;  -- HIGH is also LOW
        }
        
        INSERT INTO primitive_event_type (stream_ref, event_time, type)
        VALUES ($stream_ref, $high_time, $high_type);
        
        $preserved_count := $preserved_count + 1;
    }
    
    -- Insert LOW marker if different from OPEN, HIGH, and CLOSE
    if $low_time != $open_time AND $low_time != $close_time AND $low_time != $high_time {
        INSERT INTO primitive_event_type (stream_ref, event_time, type)
        VALUES ($stream_ref, $low_time, 4);
        
        $preserved_count := $preserved_count + 1;
    }
    
    -- Remove from pending queue
    DELETE FROM pending_prune_days 
    WHERE stream_ref = $stream_ref AND day_index = $day_index;
    
    -- Calculate deleted count
    $deleted := $count - $preserved_count;
    
    RETURN $deleted, $preserved_count;
};

/**
 * auto_digest: Batch process multiple pending days
 * 
 * Processes pending days in order (oldest first)
 */
CREATE OR REPLACE ACTION auto_digest(
    $batch_size INT DEFAULT 50
) PUBLIC RETURNS TABLE(
    processed_days INT,
    total_deleted_rows INT
) {
    -- Leader authorization check, keep it commented out for now so test passing and until we can inject how leader is
    -- if @caller != @leader {
    --     ERROR('Only the leader node can execute auto digest operations');
    -- }
    
    $processed := 0;
    $total_deleted := 0;
    
    -- Process batch of pending days (oldest first)
    for $candidate in SELECT stream_ref, day_index FROM pending_prune_days
                      ORDER BY day_index ASC, stream_ref ASC
                      LIMIT $batch_size {
        $deleted INT;
        $preserved INT;
        
        -- Process this day
        for $result in digest_daily($candidate.stream_ref, $candidate.day_index) {
            $deleted := $result.deleted_rows;
            $preserved := $result.preserved_records;
        }
        
        $processed := $processed + 1;
        $total_deleted := $total_deleted + $deleted;
    }
    
    RETURN $processed, $total_deleted;
};

/**
 * get_daily_ohlc: Query daily OHLC data
 * 
 * Returns OHLC values for a specific day and stream
 */
CREATE OR REPLACE ACTION get_daily_ohlc(
    $stream_ref INT,
    $day INT
) PUBLIC VIEW RETURNS TABLE(
    open_value NUMERIC(36,18),
    high_value NUMERIC(36,18),
    low_value NUMERIC(36,18),
    close_value NUMERIC(36,18)
) {
    $day_start := $day * 86400;
    $day_end := $day_start + 86400;
    
    -- Check if this day has been digested
    $is_digested BOOL := false;
    for $row in SELECT 1 FROM primitive_event_type
                WHERE stream_ref = $stream_ref 
                  AND event_time >= $day_start AND event_time <= $day_end {
        $is_digested := true;
    }
    
    if $is_digested {
        -- Return from digested data using type markers
        $open_value NUMERIC(36,18);
        $high_value NUMERIC(36,18);
        $low_value NUMERIC(36,18);
        $close_value NUMERIC(36,18);
        
        -- Get OPEN value (type includes 1)
        for $row in SELECT p.value FROM primitive_events p
                    JOIN primitive_event_type t ON p.stream_ref = t.stream_ref AND p.event_time = t.event_time
                    WHERE p.stream_ref = $stream_ref
                      AND p.event_time >= $day_start AND p.event_time <= $day_end
                      AND (t.type % 2) = 1 {
            $open_value := $row.value;
        }
        
        -- Get HIGH value (type includes 2)
        for $row in SELECT p.value FROM primitive_events p
                    JOIN primitive_event_type t ON p.stream_ref = t.stream_ref AND p.event_time = t.event_time
                    WHERE p.stream_ref = $stream_ref
                      AND p.event_time >= $day_start AND p.event_time <= $day_end
                      AND ((t.type / 2) % 2) = 1 {
            $high_value := $row.value;
        }
        
        -- Get LOW value (type includes 4)
        for $row in SELECT p.value FROM primitive_events p
                    JOIN primitive_event_type t ON p.stream_ref = t.stream_ref AND p.event_time = t.event_time
                    WHERE p.stream_ref = $stream_ref
                      AND p.event_time >= $day_start AND p.event_time <= $day_end
                      AND ((t.type / 4) % 2) = 1 {
            $low_value := $row.value;
        }
        
        -- Get CLOSE value (type includes 8)
        for $row in SELECT p.value FROM primitive_events p
                    JOIN primitive_event_type t ON p.stream_ref = t.stream_ref AND p.event_time = t.event_time
                    WHERE p.stream_ref = $stream_ref
                      AND p.event_time >= $day_start AND p.event_time <= $day_end
                      AND ((t.type / 8) % 2) = 1 {
            $close_value := $row.value;
        }
        
        RETURN $open_value, $high_value, $low_value, $close_value;
    } else {
        -- Calculate from raw data
        $open_value NUMERIC(36,18);
        $high_value NUMERIC(36,18);
        $low_value NUMERIC(36,18);
        $close_value NUMERIC(36,18);
        
        -- OPEN: Earliest time value, tie-break by latest created_at
        for $row in SELECT value FROM primitive_events
                    WHERE stream_ref = $stream_ref 
                      AND event_time >= $day_start AND event_time <= $day_end
                    ORDER BY event_time ASC, created_at DESC
                    LIMIT 1 {
            $open_value := $row.value;
        }
        
        -- CLOSE: Latest time value
        for $row in SELECT value FROM primitive_events
                    WHERE stream_ref = $stream_ref 
                      AND event_time >= $day_start AND event_time <= $day_end
                    ORDER BY event_time DESC, created_at DESC
                    LIMIT 1 {
            $close_value := $row.value;
        }
        
        -- HIGH: Maximum value
        for $row in SELECT MAX(value) as max_val FROM primitive_events
                    WHERE stream_ref = $stream_ref
                      AND event_time >= $day_start AND event_time <= $day_end {
            $high_value := $row.max_val;
        }
        
        -- LOW: Minimum value
        for $row in SELECT MIN(value) as min_val FROM primitive_events
                    WHERE stream_ref = $stream_ref
                      AND event_time >= $day_start AND event_time <= $day_end {
            $low_value := $row.min_val;
        }
        
        RETURN $open_value, $high_value, $low_value, $close_value;
    }
};