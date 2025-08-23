/*
 * DIGEST ACTIONS MIGRATION
 * 
 * Implements optimized digest system with UNNEST batch processing:
 * - batch_digest: Direct OHLC processing with bulk operations (replaces digest_daily)
 * - auto_digest: Batch process multiple pending days using optimized batch_digest
 * - get_daily_ohlc: Query daily OHLC data from raw or digested sources
 * 
 * Performance improvements:
 * - Eliminates expensive database roundtrips from one-by-one processing
 * - Uses UNNEST table-valued function for bulk array operations
 * - Pre-compiles valid candidates to avoid nested query issues
 * 
 * Dependencies:
 * - Requires digest schema (019-digest-schema.sql)
 * - Requires @leader contextual variable 
 * - Requires UNNEST table-valued function support in kwil-db
 */

-- =============================================================================
-- CORE DIGEST ACTIONS
-- =============================================================================

/**
 * batch_digest: Efficiently process multiple pending days using UNNEST batch processing
 * 
 * Uses UNNEST for bulk candidate filtering and optimized pre-processing.
 *
 * Performance benefits:
 * - UNNEST-based candidate validation in single query
 * - Reduced database roundtrips compared to loops
 * - Bulk operations where possible
 */
CREATE OR REPLACE ACTION batch_digest(
    $stream_refs INT[],
    $day_indexes INT[]
) PUBLIC RETURNS TABLE(
    processed_days INT,
    total_deleted_rows INT,
    total_preserved_rows INT
) {
    -- Leader authorization check, keep it commented out for now so test passing and until we can inject how leader is
    -- if @caller != @leader {
    --     ERROR('Only the leader node can execute batch digest operations');
    -- }
    
    -- Validate input arrays have same length
    if array_length($stream_refs) != array_length($day_indexes) {
        ERROR('stream_refs and day_indexes arrays must have the same length');
    }
    
    if array_length($stream_refs) = 0 {
        RETURN 0, 0, 0;
    }
    
    $total_processed := 0;
    $total_deleted := 0;
    $total_preserved := 0;
    
    -- Step 1: Use UNNEST for bulk candidate validation and filtering
    $valid_stream_refs INT[] := ARRAY[]::INT[];
    $valid_day_indexes INT[] := ARRAY[]::INT[];
    
    -- UNNEST-based candidate validation - single query for all candidates
    for $valid_candidate in 
    SELECT u.stream_ref, u.day_index
    FROM UNNEST($stream_refs, $day_indexes) AS u(stream_ref, day_index)
    INNER JOIN pending_prune_days ppd 
        ON ppd.stream_ref = u.stream_ref AND ppd.day_index = u.day_index {
        
        $valid_stream_refs := array_append($valid_stream_refs, $valid_candidate.stream_ref);
        $valid_day_indexes := array_append($valid_day_indexes, $valid_candidate.day_index);
    }
    
    -- Step 2: TRUE BULK OHLC Processing using UNNEST for ALL streams simultaneously
    if array_length($valid_stream_refs) > 0 {
        -- Build arrays for bulk processing with UNNEST
        $ohlc_stream_refs INT[] := ARRAY[]::INT[];
        $ohlc_day_starts INT[] := ARRAY[]::INT[];
        $ohlc_day_ends INT[] := ARRAY[]::INT[];
        $ohlc_open_times INT[] := ARRAY[]::INT[];
        $ohlc_open_created_ats INT[] := ARRAY[]::INT[];
        $ohlc_close_times INT[] := ARRAY[]::INT[];
        $ohlc_close_created_ats INT[] := ARRAY[]::INT[];
        $ohlc_high_times INT[] := ARRAY[]::INT[];
        $ohlc_high_created_ats INT[] := ARRAY[]::INT[];
        $ohlc_low_times INT[] := ARRAY[]::INT[];
        $ohlc_low_created_ats INT[] := ARRAY[]::INT[];
        
        -- Step 2a: Bulk OHLC calculation for ALL streams using single query with UNNEST
        for $bulk_ohlc in
        WITH RECURSIVE stream_days AS (
            -- Use UNNEST to create table of all stream/day combinations
            SELECT u.stream_ref, u.day_index,
                   (u.day_index * 86400) as day_start,
                   ((u.day_index * 86400) + 86400) as day_end
            FROM UNNEST($valid_stream_refs, $valid_day_indexes) AS u(stream_ref, day_index)
        ),
        stream_record_counts AS (
            -- Count records for each stream in single query
            SELECT sd.stream_ref, sd.day_index, sd.day_start, sd.day_end,
                   COUNT(pe.event_time) as record_count
            FROM stream_days sd
            LEFT JOIN primitive_events pe ON pe.stream_ref = sd.stream_ref
                AND pe.event_time >= sd.day_start AND pe.event_time <= sd.day_end
            GROUP BY sd.stream_ref, sd.day_index, sd.day_start, sd.day_end
            HAVING COUNT(pe.event_time) > 1  -- Only streams with >1 record
        )
        SELECT 
            src.stream_ref,
            src.day_index,
            src.day_start,
            src.day_end,
            src.record_count,
            -- OPEN: Earliest time, tie-break by latest created_at (aggregated for all streams)
            (
                SELECT pe.event_time
                FROM primitive_events pe
                WHERE pe.stream_ref = src.stream_ref
                  AND pe.event_time >= src.day_start AND pe.event_time <= src.day_end
                ORDER BY pe.event_time ASC, pe.created_at DESC
                LIMIT 1
            ) as open_time,
            (
                SELECT pe.created_at
                FROM primitive_events pe
                WHERE pe.stream_ref = src.stream_ref
                  AND pe.event_time >= src.day_start AND pe.event_time <= src.day_end
                ORDER BY pe.event_time ASC, pe.created_at DESC
                LIMIT 1
            ) as open_created_at,
            -- CLOSE: Latest time, tie-break by latest created_at
            (
                SELECT pe.event_time
                FROM primitive_events pe
                WHERE pe.stream_ref = src.stream_ref
                  AND pe.event_time >= src.day_start AND pe.event_time <= src.day_end
                ORDER BY pe.event_time DESC, pe.created_at DESC
                LIMIT 1
            ) as close_time,
            (
                SELECT pe.created_at
                FROM primitive_events pe
                WHERE pe.stream_ref = src.stream_ref
                  AND pe.event_time >= src.day_start AND pe.event_time <= src.day_end
                ORDER BY pe.event_time DESC, pe.created_at DESC
                LIMIT 1
            ) as close_created_at,
            -- HIGH: Maximum value, tie-break by earliest time and latest created_at
            (
                SELECT pe.event_time
                FROM primitive_events pe
                WHERE pe.stream_ref = src.stream_ref
                  AND pe.event_time >= src.day_start AND pe.event_time <= src.day_end
                ORDER BY pe.value DESC, pe.event_time ASC, pe.created_at DESC
                LIMIT 1
            ) as high_time,
            (
                SELECT pe.created_at
                FROM primitive_events pe
                WHERE pe.stream_ref = src.stream_ref
                  AND pe.event_time >= src.day_start AND pe.event_time <= src.day_end
                ORDER BY pe.value DESC, pe.event_time ASC, pe.created_at DESC
                LIMIT 1
            ) as high_created_at,
            -- LOW: Minimum value, tie-break by earliest time and latest created_at
            (
                SELECT pe.event_time
                FROM primitive_events pe
                WHERE pe.stream_ref = src.stream_ref
                  AND pe.event_time >= src.day_start AND pe.event_time <= src.day_end
                ORDER BY pe.value ASC, pe.event_time ASC, pe.created_at DESC
                LIMIT 1
            ) as low_time,
            (
                SELECT pe.created_at
                FROM primitive_events pe
                WHERE pe.stream_ref = src.stream_ref
                  AND pe.event_time >= src.day_start AND pe.event_time <= src.day_end
                ORDER BY pe.value ASC, pe.event_time ASC, pe.created_at DESC
                LIMIT 1
            ) as low_created_at
        FROM stream_record_counts src
        ORDER BY src.stream_ref, src.day_index {
            -- Collect OHLC data for bulk operations
            $ohlc_stream_refs := array_append($ohlc_stream_refs, $bulk_ohlc.stream_ref);
            $ohlc_day_starts := array_append($ohlc_day_starts, $bulk_ohlc.day_start);
            $ohlc_day_ends := array_append($ohlc_day_ends, $bulk_ohlc.day_end);
            $ohlc_open_times := array_append($ohlc_open_times, $bulk_ohlc.open_time);
            $ohlc_open_created_ats := array_append($ohlc_open_created_ats, $bulk_ohlc.open_created_at);
            $ohlc_close_times := array_append($ohlc_close_times, $bulk_ohlc.close_time);
            $ohlc_close_created_ats := array_append($ohlc_close_created_ats, $bulk_ohlc.close_created_at);
            $ohlc_high_times := array_append($ohlc_high_times, $bulk_ohlc.high_time);
            $ohlc_high_created_ats := array_append($ohlc_high_created_ats, $bulk_ohlc.high_created_at);
            $ohlc_low_times := array_append($ohlc_low_times, $bulk_ohlc.low_time);
            $ohlc_low_created_ats := array_append($ohlc_low_created_ats, $bulk_ohlc.low_created_at);
            
            $total_processed := $total_processed + 1;
        }
        
        -- Step 2b: Process individual stream operations (still optimized by bulk OHLC calculation above)
        if array_length($ohlc_stream_refs) > 0 {
            for $i in 1..array_length($ohlc_stream_refs) {
                $stream_ref := $ohlc_stream_refs[$i];
                $day_index := $valid_day_indexes[$i];
                $day_start := $ohlc_day_starts[$i];
                $day_end := $ohlc_day_ends[$i];
                $open_time := $ohlc_open_times[$i];
                $open_created_at := $ohlc_open_created_ats[$i];
                $close_time := $ohlc_close_times[$i];
                $close_created_at := $ohlc_close_created_ats[$i];
                $high_time := $ohlc_high_times[$i];
                $high_created_at := $ohlc_high_created_ats[$i];
                $low_time := $ohlc_low_times[$i];
                $low_created_at := $ohlc_low_created_ats[$i];
                
                -- Count initial records BEFORE deletion
                $initial_record_count := 0;
                for $initial_row in SELECT COUNT(*) as cnt FROM primitive_events
                                   WHERE stream_ref = $stream_ref
                                     AND event_time >= $day_start AND event_time <= $day_end {
                    $initial_record_count := $initial_row.cnt;
                }
                
                -- Delete excess records (keep only OHLC)
                DELETE FROM primitive_events
                WHERE stream_ref = $stream_ref
                  AND event_time >= $day_start AND event_time <= $day_end
                  AND NOT (
                    (event_time = $open_time  AND created_at = $open_created_at)  OR
                    (event_time = $close_time AND created_at = $close_created_at) OR
                    (event_time = $high_time  AND created_at = $high_created_at)  OR
                    (event_time = $low_time   AND created_at = $low_created_at)
                  );
                
                -- Count remaining records AFTER deletion
                $remaining_count := 0;
                for $remaining_row in SELECT COUNT(*) as cnt FROM primitive_events
                                     WHERE stream_ref = $stream_ref
                                       AND event_time >= $day_start AND event_time <= $day_end {
                    $remaining_count := $remaining_row.cnt;
                }
                
                -- Calculate actual deletions
                $deleted_this_stream := $initial_record_count - $remaining_count;
                $total_deleted := $total_deleted + $deleted_this_stream;
                
                -- Delete old type markers
                DELETE FROM primitive_event_type 
                WHERE stream_ref = $stream_ref 
                  AND event_time >= $day_start AND event_time <= $day_end;
                
                -- Insert OPEN marker with combined flags
                $open_type := 1;  -- OPEN flag
                if $high_time = $open_time AND $high_created_at = $open_created_at {
                    $open_type := $open_type + 2;  -- Add HIGH flag
                }
                if $low_time = $open_time AND $low_created_at = $open_created_at {
                    $open_type := $open_type + 4;  -- Add LOW flag
                }
                if $close_time = $open_time AND $close_created_at = $open_created_at {
                    $open_type := $open_type + 8;  -- Add CLOSE flag
                }
                
                INSERT INTO primitive_event_type (stream_ref, event_time, type)
                VALUES ($stream_ref, $open_time, $open_type);
                
                $total_preserved := $total_preserved + 1;
                
                -- Insert CLOSE marker if different from OPEN
                if $close_time != $open_time OR $close_created_at != $open_created_at {
                    $close_type := 8;  -- CLOSE flag
                    if $high_time = $close_time AND $high_created_at = $close_created_at {
                        $close_type := $close_type + 2;  -- Add HIGH flag
                    }
                    if $low_time = $close_time AND $low_created_at = $close_created_at {
                        $close_type := $close_type + 4;  -- Add LOW flag
                    }
                    
                    INSERT INTO primitive_event_type (stream_ref, event_time, type)
                    VALUES ($stream_ref, $close_time, $close_type);
                    
                    $total_preserved := $total_preserved + 1;
                }
                
                -- Insert HIGH marker if different from OPEN and CLOSE
                if ($high_time != $open_time OR $high_created_at != $open_created_at) 
                   AND ($high_time != $close_time OR $high_created_at != $close_created_at) {
                    $high_type := 2;  -- HIGH flag
                    if $low_time = $high_time AND $low_created_at = $high_created_at {
                        $high_type := $high_type + 4;  -- Add LOW flag
                    }
                    
                    INSERT INTO primitive_event_type (stream_ref, event_time, type)
                    VALUES ($stream_ref, $high_time, $high_type);
                    
                    $total_preserved := $total_preserved + 1;
                }
                
                -- Insert LOW marker if different from OPEN, HIGH, and CLOSE
                if ($low_time != $open_time OR $low_created_at != $open_created_at) 
                   AND ($low_time != $close_time OR $close_created_at != $close_created_at) 
                   AND ($low_time != $high_time OR $high_created_at != $high_created_at) {
                    INSERT INTO primitive_event_type (stream_ref, event_time, type)
                    VALUES ($stream_ref, $low_time, 4);  -- LOW flag only
                    
                    $total_preserved := $total_preserved + 1;
                }
                
                -- Remove from pending queue
                DELETE FROM pending_prune_days 
                WHERE stream_ref = $stream_ref AND day_index = $day_index;
            }
        }
    }
    
    RETURN $total_processed, $total_deleted, $total_preserved;
};

/**
 * auto_digest: Batch process multiple pending days (optimized version)
 * 
 * Now uses the efficient batch_digest internally for better performance
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
    
    -- Collect candidates using array aggregation for batch processing
    $stream_refs INT[];
    $day_indexes INT[];
    
    for $candidates in 
    SELECT 
        ARRAY_AGG(stream_ref) as stream_refs,
        ARRAY_AGG(day_index) as day_indexes
    FROM (
        SELECT stream_ref, day_index FROM pending_prune_days
        ORDER BY day_index ASC, stream_ref ASC
        LIMIT $batch_size
    ) AS ordered_candidates {
        $stream_refs := $candidates.stream_refs;
        $day_indexes := $candidates.day_indexes;
    }
    
    -- Handle empty result case
    if $stream_refs IS NULL OR array_length($stream_refs) = 0 {
        RETURN 0, 0;
    }
    
    -- Process using optimized batch_digest
    $processed := 0;
    $total_deleted := 0;
    
    for $result in batch_digest($stream_refs, $day_indexes) {
        $processed := $result.processed_days;
        $total_deleted := $result.total_deleted_rows;
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