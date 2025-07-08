/*
 * Calculates the time series for a composed stream within a specified time range.
 *
 * This function handles complex scenarios involving:
 * - Recursively resolving taxonomies (stream dependencies).
 * - Time-varying weights assigned to child streams.
 * - Aggregating values based on current weights.
 * - Handling overshadowing taxonomy definitions (using the latest version).
 * - Filling gaps in data using Last Observation Carried Forward (LOCF).
 * - Time-travel queries using the $frozen_at parameter.
 *
 * It employs a delta-based calculation method for efficiency, computing changes
 * in weighted sums and weight sums rather than recalculating the full state at
 * every point in time. Boundary adjustments are applied at times when the
 * root taxonomy definition changes.
 *
 * Parameters:
 *   $data_provider: The deployer address of the composed stream.
 *   $stream_id: The ID of the composed stream.
 *   $from: Start timestamp (inclusive) of the query range. Defaults to 0.
 *   $to: End timestamp (inclusive) of the query range. Defaults to max INT8.
 *   $frozen_at: Timestamp for time-travel queries; considers only events
 *                created at or before this time. Defaults to max INT8.
 *
 * Returns:
 *   A table with (event_time, value) representing the calculated time series
 *   for the composed stream within the requested range, including LOCF points.
 *
 * Accuracy Note:
 *   Maintaining calculation accuracy and precision across all scenarios is critical.
 *   The method must consistently handle taxonomy changes occurring anywhere in the
 *   dependency tree, not just at the root. Simplifications that only perform
 *   full state adjustments at root boundaries are insufficient and can lead to
 *   inaccuracies. The delta calculation must correctly account for the change
 *   in weighted sum (`Value * dWeight`) whenever an effective weight changes,
 *   regardless of the change's origin in the taxonomy.
 */
CREATE OR REPLACE ACTION get_record_composed(
    $data_provider TEXT,  -- Stream Deployer
    $stream_id TEXT,      -- Target composed stream
    $from INT8,           -- Start of requested time range (inclusive)
    $to INT8,             -- End of requested time range (inclusive)
    $frozen_at INT8       -- Created-at cutoff: only consider events created before this
) PRIVATE VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
)  {
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    -- Define boundary defaults and effective values
    $max_int8 := 9223372036854775000;          -- "Infinity" sentinel for INT8
    $effective_from := COALESCE($from, 0);      -- Lower bound, default 0
    $effective_to := COALESCE($to, $max_int8);  -- Upper bound, default "infinity"
    $effective_frozen_at := COALESCE($frozen_at, $max_int8);

    -- Validate time range
    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('Invalid time range: from (%s) > to (%s)', $from, $to));
}

    -- Check permissions; raises error if unauthorized
    IF !is_allowed_to_read_all($data_provider, $stream_id, $lower_caller, $from, $to) {
        ERROR('Not allowed to read stream');
}
    IF !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
        ERROR('Not allowed to compose stream');
}

    -- Check if cache is enabled and frozen_at is null (frozen queries bypass cache)
    $cache_enabled BOOL := false;
    if $frozen_at IS NULL {
        for $row in tn_cache.is_enabled() {
            $cache_enabled := $row.enabled;
            break;
}
    }

    if $cache_enabled {
        -- Check if we have cached data for this request
        $has_cache BOOL := false;
for $row in tn_cache.has_cached_data($data_provider, $stream_id, $from, $to) {
            $has_cache := $row.has_data;
            break;
}

        if $has_cache {
            -- Cache hit - return cached data
            NOTICE('Cache hit for stream ' || $data_provider || ':' || $stream_id || ' from ' || $from::TEXT || ' to ' || $to::TEXT);
for $row in tn_cache.get_cached_data($data_provider, $stream_id, $from, $to) {
                RETURN NEXT $row.event_time, $row.value;
}
            return;
} else {
            -- Cache miss - log and fallback to original logic
            NOTICE('Cache miss for stream ' || $data_provider || ':' || $stream_id || ' from ' || $from::TEXT || ' to ' || $to::TEXT);
}
    }

        -- for historical consistency, if both from and to are omitted, return the latest record
    if $from IS NULL AND $to IS NULL {
        FOR $row IN get_last_record_composed($data_provider, $stream_id, NULL, $effective_frozen_at) {
            RETURN NEXT $row.event_time, $row.value;
}
        RETURN;
}

    RETURN WITH RECURSIVE
    /*----------------------------------------------------------------------
     * PARENT_DISTINCT_START_TIMES CTE:
     *
     * Purpose: Identifies all unique start times at which a parent stream
     * has at least one taxonomy definition.
     *---------------------------------------------------------------------*/
    parent_distinct_start_times AS (
        SELECT DISTINCT
            data_provider AS parent_dp,
            stream_id AS parent_sid,
            start_time
        FROM taxonomies
        WHERE disabled_at IS NULL
    ),

    /*----------------------------------------------------------------------
     * PARENT_NEXT_STARTS CTE:
     *
     * Purpose: For each unique start time of a parent's definition,
     * determines the immediately following unique start time for that same parent.
     * This is used to define the upper bound of a taxonomy segment's validity.
     *---------------------------------------------------------------------*/
    parent_next_starts AS (
        SELECT
            parent_dp,
            parent_sid,
            start_time,
            LEAD(start_time) OVER (PARTITION BY parent_dp, parent_sid ORDER BY start_time) as next_start_time
        FROM parent_distinct_start_times
    ),

    /*----------------------------------------------------------------------
     * TAXONOMY_TRUE_SEGMENTS CTE:
     *
     * Purpose: For every direct parent-child link recorded in the taxonomies table,
     * determine its true, non-overlapping, active time segments and the specific
     * weight associated with each segment. This is independent of $effective_from.
     * Overshadowing for a parent's child configuration (i.e. multiple group_sequences
     * for the same parent and start_time) is handled by first selecting only those
     * taxonomy entries belonging to the MAX(group_sequence) for that parent/start_time.
     * Segment end is then determined by the parent's next distinct definition start_time
     * from parent_next_starts.
     *---------------------------------------------------------------------*/
    taxonomy_true_segments AS (
        SELECT
            t.parent_dp,
            t.parent_sid,
            t.child_dp,
            t.child_sid,
            t.weight_for_segment,
            t.segment_start,
            COALESCE(pns.next_start_time, $max_int8) - 1 AS segment_end
        FROM (
            -- Select all child links from the latest taxonomy version (highest group_sequence)
            -- for each parent at each specific start_time.
            SELECT
                tx.data_provider AS parent_dp,
                tx.stream_id AS parent_sid,
                tx.child_data_provider AS child_dp,
                tx.child_stream_id AS child_sid,
                tx.weight AS weight_for_segment,
                tx.start_time AS segment_start
            FROM taxonomies tx
            JOIN (
                -- Subquery to find the max group_sequence for each parent's start_time.
                -- This determines the "winning" version of the taxonomy for that parent at that start_time.
                SELECT
                    data_provider, stream_id, start_time,
                    MAX(group_sequence) as max_gs
                FROM taxonomies
                WHERE disabled_at IS NULL
                GROUP BY data_provider, stream_id, start_time
            ) max_gs_filter
            ON tx.data_provider = max_gs_filter.data_provider
           AND tx.stream_id = max_gs_filter.stream_id
           AND tx.start_time = max_gs_filter.start_time
           AND tx.group_sequence = max_gs_filter.max_gs -- Only pick rows matching the max group_sequence
            WHERE tx.disabled_at IS NULL -- Ensure the chosen taxonomy entry itself is not disabled
        ) t -- Each row 't' is now part of the "winning" taxonomy version for its parent and start_time
        JOIN parent_next_starts pns -- This correctly defines how long this "winning" version is active
          ON t.parent_dp = pns.parent_dp
         AND t.parent_sid = pns.parent_sid
         AND t.segment_start = pns.start_time
    ),

    /*----------------------------------------------------------------------
     * HIERARCHY CTE: Recursively resolves the dependency tree.
     *
     * Purpose: Determines the effective weighted contribution of every stream
     * (down to the primitives) to the root composed stream over time.
     * Calculates cumulative `raw_weight` and intersected `path_start`, `path_end`
     * for each path. Pruning is applied based on an anchor and $effective_to.
     *---------------------------------------------------------------------*/
    hierarchy AS (
      -- Base Case: Direct children of the root composed stream.
      SELECT
          tts.parent_dp AS root_dp,       -- This is $data_provider
          tts.parent_sid AS root_sid,      -- This is $stream_id
          tts.child_dp AS descendant_dp,
          tts.child_sid AS descendant_sid,
          tts.weight_for_segment AS raw_weight,
          tts.segment_start AS path_start,
          tts.segment_end AS path_end,
          1 AS level
      FROM taxonomy_true_segments tts
      WHERE tts.parent_dp = $data_provider AND tts.parent_sid = $stream_id
        AND tts.segment_end >= ( -- Path segment must end at or after the root's anchor time
          COALESCE(
              (SELECT t_anchor_base.start_time FROM taxonomies t_anchor_base
               WHERE t_anchor_base.data_provider = $data_provider AND t_anchor_base.stream_id = $stream_id
                 AND t_anchor_base.disabled_at IS NULL AND t_anchor_base.start_time <= $effective_from
               ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC LIMIT 1),
              0 -- Default anchor to 0
          )
        )
        AND tts.segment_start <= $effective_to -- Path segment must start at or before query end

      UNION ALL

      -- Recursive Step: Children of the children found in the previous level.
      SELECT
          h.root_dp,
          h.root_sid,
          tts.child_dp AS descendant_dp,
          tts.child_sid AS descendant_sid,
          -- Multiply parent weight by child weight for cumulative effect
          (h.raw_weight * tts.weight_for_segment)::NUMERIC(36,18) AS raw_weight,
          -- Effective interval start is the later of the parent's path_start or child's segment_start
          GREATEST(h.path_start, tts.segment_start) AS path_start,
          -- Effective interval end is the earlier of the parent's path_end or child's segment_end
          LEAST(h.path_end, tts.segment_end) AS path_end,
          h.level + 1
      FROM
          hierarchy h
      JOIN taxonomy_true_segments tts
          ON h.descendant_dp = tts.parent_dp AND h.descendant_sid = tts.parent_sid
      WHERE
          -- Effective intersected path segment must be valid (start <= end)
          GREATEST(h.path_start, tts.segment_start) <= LEAST(h.path_end, tts.segment_end)
          -- Intersected segment must end at or after the root's anchor time
          AND LEAST(h.path_end, tts.segment_end) >= (
               COALESCE(
                  (SELECT t_anchor_base.start_time FROM taxonomies t_anchor_base
                   WHERE t_anchor_base.data_provider = $data_provider AND t_anchor_base.stream_id = $stream_id
                     AND t_anchor_base.disabled_at IS NULL AND t_anchor_base.start_time <= $effective_from
                   ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC LIMIT 1),
                  0 -- Default anchor to 0
              )
          )
          -- Intersected segment must start at or before query end
          AND GREATEST(h.path_start, tts.segment_start) <= $effective_to
          AND h.level < 10 -- Recursion depth limit
    ),

    /*----------------------------------------------------------------------
     * HIERARCHY_PRIMITIVE_PATHS CTE: Filters the hierarchy to find paths ending in leaf nodes (primitives).
     *
     * Purpose: Identifies all paths from the root composed stream to any contributing
     * primitive stream, along with the raw weight and validity interval for that specific path.
     *--------------------------------------------------------------------*/
    hierarchy_primitive_paths AS (
      SELECT
          h.descendant_dp AS primitive_dp,
          h.descendant_sid AS primitive_sid,
          h.raw_weight,
          h.path_start,
          h.path_end
      FROM hierarchy h
      WHERE EXISTS (
          SELECT 1 FROM streams s
          WHERE s.data_provider = h.descendant_dp
            AND s.stream_id     = h.descendant_sid
            AND s.stream_type   = 'primitive' -- Ensure it's a primitive
      )
    ),

    /*----------------------------------------------------------------------
     * PRIMITIVE_WEIGHTS CTE: Passes through each distinct segment of a primitive's activity.
     *
     * Purpose: Ensures each primitive stream's contribution is processed based on
     * its specific activity intervals and weights as determined by the hierarchy.
     * Each row represents a distinct period where a primitive is a child in the
     * resolved taxonomy, with its corresponding raw_weight and validity interval
     * (group_sequence_start, group_sequence_end) for that specific path/segment.
     * This allows downstream CTEs to correctly model changes in a primitive's
     * effective weight over time, respecting taxonomy overshadowing.
     *--------------------------------------------------------------------*/
    primitive_weights AS (
      SELECT
          hpp.primitive_dp AS data_provider,
          hpp.primitive_sid AS stream_id,
          hpp.raw_weight, -- The raw_weight for this specific path segment
          hpp.path_start AS group_sequence_start, -- The start of this specific path segment
          hpp.path_end AS group_sequence_end -- The end of this specific path segment
      FROM hierarchy_primitive_paths hpp
      -- No GROUP BY. Each row from hierarchy_primitive_paths represents a distinct segment
      -- of activity for a primitive, which needs to be processed individually.
    ),

    /*----------------------------------------------------------------------
     * CLEANED_EVENT_TIMES CTE: Gathers all essential timestamps for calculation.
     *
     * Purpose: Creates a distinct set of all time points where the composed
     * value might change. This includes primitive events, taxonomy changes.
     * Ensures the final calculation considers all necessary points.
     *---------------------------------------------------------------------*/
    cleaned_event_times AS (
        SELECT DISTINCT event_time
        FROM (
            -- 1. Primitive event times strictly within the requested range
            SELECT pe.event_time
            FROM primitive_events pe
            JOIN primitive_weights pw -- Only events from relevant primitives during their active weight interval
              ON pe.data_provider = pw.data_provider
             AND pe.stream_id = pw.stream_id
             AND pe.event_time >= pw.group_sequence_start
             AND pe.event_time <= pw.group_sequence_end
            WHERE pe.event_time > $effective_from
              AND pe.event_time <= $effective_to
              AND pe.created_at <= $effective_frozen_at -- Apply frozen_at

            UNION

            -- 2. Taxonomy start times (weight changes) strictly within the range
            SELECT pw.group_sequence_start AS event_time
            FROM primitive_weights pw
            WHERE pw.group_sequence_start > $effective_from
              AND pw.group_sequence_start <= $effective_to
        ) all_times_in_range

        UNION

        -- 4. Anchor Point: The latest relevant time AT or BEFORE $effective_from.
        -- This establishes the initial state for the delta calculation.
        SELECT event_time FROM (
            SELECT event_time
            FROM (
                -- Latest primitive event at or before start
                SELECT pe.event_time
                FROM primitive_events pe
                JOIN primitive_weights pw -- Check relevance against weight intervals
                  ON pe.data_provider = pw.data_provider
                 AND pe.stream_id = pw.stream_id
                 AND pe.event_time >= pw.group_sequence_start
                 AND pe.event_time <= pw.group_sequence_end
                WHERE pe.event_time <= $effective_from
                  AND pe.created_at <= $effective_frozen_at

                UNION

                -- Latest taxonomy start at or before start
                SELECT pw.group_sequence_start AS event_time
                FROM primitive_weights pw
                WHERE pw.group_sequence_start <= $effective_from

            ) all_times_before
            ORDER BY event_time DESC -- Get the latest one
            LIMIT 1
        ) as anchor_event
    ),

    /*----------------------------------------------------------------------
     * NEW DELTA CALCULATION METHOD
     *---------------------------------------------------------------------*/

    -- Step 1: Find initial states (value at or before $effective_from)
    initial_primitive_states AS (
        SELECT
            pe.data_provider,
            pe.stream_id,
            pe.event_time, -- Keep the actual time of the initial event
            pe.value
        FROM (
            -- Use ROW_NUMBER to find the latest event per primitive before/at $from
            SELECT
                pe_inner.data_provider,
                pe_inner.stream_id,
                pe_inner.event_time,
                pe_inner.value,
                ROW_NUMBER() OVER (
                    PARTITION BY pe_inner.data_provider, pe_inner.stream_id
                    ORDER BY pe_inner.event_time DESC, pe_inner.created_at DESC -- Tie-break by creation time
                ) as rn
            FROM primitive_events pe_inner
            WHERE pe_inner.event_time <= $effective_from -- At or before the start
              AND EXISTS ( -- Ensure the primitive exists in the resolved hierarchy
                  SELECT 1 FROM primitive_weights pw_exists
                  WHERE pw_exists.data_provider = pe_inner.data_provider AND pw_exists.stream_id = pe_inner.stream_id
              )
              AND pe_inner.created_at <= $effective_frozen_at
        ) pe
        WHERE pe.rn = 1 -- Select the latest state
    ),

    -- Step 2: Find distinct primitive events strictly WITHIN the interval ($from < time <= $to).
    primitive_events_in_interval AS (
        SELECT
            pe.data_provider,
            pe.stream_id,
            pe.event_time,
            pe.value
        FROM (
             -- Use ROW_NUMBER to pick the latest created_at for duplicate event_times
             SELECT
                pe_inner.data_provider,
                pe_inner.stream_id,
                pe_inner.event_time,
                pe_inner.created_at,
                pe_inner.value,
                ROW_NUMBER() OVER (
                    PARTITION BY pe_inner.data_provider, pe_inner.stream_id, pe_inner.event_time
                    ORDER BY pe_inner.created_at DESC
                ) as rn
            FROM primitive_events pe_inner
            JOIN primitive_weights pw_check -- Ensure validity against *a* taxonomy interval
                ON pe_inner.data_provider = pw_check.data_provider
               AND pe_inner.stream_id = pw_check.stream_id
               AND pe_inner.event_time >= pw_check.group_sequence_start
               AND pe_inner.event_time <= pw_check.group_sequence_end
            WHERE pe_inner.event_time > $effective_from -- Strictly after start
                AND pe_inner.event_time <= $effective_to    -- At or before end
                AND pe_inner.created_at <= $effective_frozen_at
        ) pe
        WHERE pe.rn = 1 -- Select the latest created_at for each (dp, sid, et)
    ),

    -- Step 3: Combine initial states and interval events.
    all_primitive_points AS (
        SELECT data_provider, stream_id, event_time, value FROM initial_primitive_states
        UNION ALL
        SELECT data_provider, stream_id, event_time, value FROM primitive_events_in_interval
    ),

    -- Step 4: Calculate value change (delta_value) for each primitive.
    primitive_event_changes AS (
        SELECT * FROM (
                          SELECT data_provider, stream_id, event_time, value,
                                 COALESCE(value - LAG(value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time), value)::numeric(36,18) AS delta_value
                          FROM all_primitive_points
                      ) calc WHERE delta_value != 0::numeric(36,18)
    ),

    -- Step 5: Find the first time each primitive provides a value. (Added for correctness)
    first_value_times AS (
        SELECT
            data_provider,
            stream_id,
            MIN(event_time) as first_value_time
        FROM all_primitive_points -- Based on combined initial state and interval events
        GROUP BY data_provider, stream_id
    ),

    -- Step 6: Generate effective weight change events based on first value time. (Added for correctness)
    effective_weight_changes AS (
        -- Positive delta: Occurs at the LATER of weight definition start OR first value time
        SELECT
            pw.data_provider,
            pw.stream_id,
            GREATEST(pw.group_sequence_start, fvt.first_value_time) AS event_time, -- Use effective start time
            pw.raw_weight AS weight_delta
        FROM primitive_weights pw
        INNER JOIN first_value_times fvt -- Only consider primitives that HAVE values
            ON pw.data_provider = fvt.data_provider AND pw.stream_id = fvt.stream_id
        -- Ensure the calculated effective start time is still within the weight's defined interval
        WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
          AND pw.raw_weight != 0::numeric(36,18)

        UNION ALL

        -- Negative delta: Occurs when the original weight interval ends
        SELECT
            pw.data_provider,
            pw.stream_id,
            pw.group_sequence_end + 1 AS event_time,
            -pw.raw_weight AS weight_delta
        FROM primitive_weights pw
        INNER JOIN first_value_times fvt -- Ensure we only add a negative delta if a positive one was possible
            ON pw.data_provider = fvt.data_provider AND pw.stream_id = fvt.stream_id
        -- Check the same validity condition as the positive delta
        WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
          AND pw.raw_weight != 0::numeric(36,18)
          -- don't emit closing delta for open interval
          AND pw.group_sequence_end < ($max_int8 - 1)
    ),

    -- Step 7: Combine value and *effective* weight changes into a unified timeline.
    unified_events AS (
        SELECT
            pec.data_provider,
            pec.stream_id,
            pec.event_time,
            pec.delta_value,
            0::numeric(36,18) AS weight_delta,
            1 AS event_type_priority -- Value changes first
        FROM primitive_event_changes pec

        UNION ALL

        -- *Effective* Weight changes (deltas)
        SELECT
            ewc.data_provider,
            ewc.stream_id,
            ewc.event_time,
            0::numeric(36,18) AS delta_value,
            ewc.weight_delta,
            2 AS event_type_priority -- Weight changes second
        FROM effective_weight_changes ewc -- Use effective changes
    ),

    -- Step 8: Calculate state timeline and delta contributions using window functions.
    primitive_state_timeline AS (
        SELECT
            data_provider,
            stream_id,
            event_time,
            delta_value,
            weight_delta,
            -- Calculate value and weight *before* this event using LAG on cumulative sums
            COALESCE(LAG(value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as value_before_event,
            COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as weight_before_event
        FROM (
            SELECT
                data_provider,
                stream_id,
                event_time,
                delta_value,
                weight_delta,
                event_type_priority, -- Ensure it's selected for ordering
                -- Cumulative value up to and including this event
                (SUM(delta_value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as value_after_event,
                -- Cumulative weight up to and including this event
                (SUM(weight_delta) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as weight_after_event
            FROM unified_events
        ) state_calc
    ),

    -- Step 9: Calculate final aggregated deltas per time point.
    final_deltas AS ( -- Renamed from new_final_deltas to match original naming convention
        SELECT
            event_time,
            SUM(
                (delta_value * weight_before_event) +
                (value_before_event * weight_delta) +
                (delta_value * weight_delta) -- Added missing term
            )::numeric(72, 18) AS delta_ws,
            SUM(weight_delta)::numeric(36, 18) AS delta_sw
        FROM primitive_state_timeline
        GROUP BY event_time
        HAVING SUM(
                (delta_value * weight_before_event) +
                (value_before_event * weight_delta) +
                (delta_value * weight_delta)
            )::numeric(72, 18) != 0::numeric(72, 18)
            OR SUM(weight_delta)::numeric(36, 18) != 0::numeric(36, 18) -- Keep if either delta is non-zero
    ),

    -- Step 10: Combine all time points where any delta might occur or are requested.
    all_combined_times AS (
        SELECT time_point FROM (
            SELECT event_time as time_point FROM final_deltas -- Use times from the new delta calculation
            UNION
            SELECT event_time as time_point FROM cleaned_event_times -- Ensures query bounds and anchor are present
        ) distinct_times
    ),

    -- Step 11: Calculate cumulative values.
    cumulative_values AS (
        SELECT
            act.time_point as event_time, -- Use time_point from all_combined_times
            (COALESCE((SUM(fd.delta_ws) OVER (ORDER BY act.time_point ASC))::numeric(72,18), 0::numeric(72,18))) as cum_ws, -- Sum based on combined time order
            (COALESCE((SUM(fd.delta_sw) OVER (ORDER BY act.time_point ASC))::numeric(36,18), 0::numeric(36,18))) as cum_sw  -- Sum based on combined time order
        FROM all_combined_times act
        LEFT JOIN final_deltas fd ON fd.event_time = act.time_point -- Left join to keep all times
    ),

    -- Step 12: Compute the aggregated value (Weighted Average)
    aggregated AS (
        SELECT cv.event_time,
               CASE WHEN cv.cum_sw = 0::numeric(36,18) THEN 0::numeric(72,18)
                    ELSE cv.cum_ws / cv.cum_sw::numeric(72,18)
                   END AS value
        FROM cumulative_values cv
    ),

    /*----------------------------------------------------------------------
     * LOCF (Last Observation Carried Forward) Logic
     *
     * Purpose: Fills gaps in the results. If a query requests a time point where
     * no underlying primitive event or taxonomy change occurred, this logic finds
     * the value from the most recent preceding time point where such a change did happen.
     *---------------------------------------------------------------------*/
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
        WHERE time_point < $effective_from -- Strictly before the requested start
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
            -- Include rows within the requested query range [$from, $to]
            (fm.event_time >= $effective_from AND fm.event_time <= $effective_to)
            OR
            -- Include the anchor point row if it exists and matches an aggregated time
            (atc.anchor_time IS NOT NULL AND fm.event_time = atc.anchor_time)
    ),

    -- Check if there are any rows from aggregated whose event_time falls directly within the requested range.
    -- This helps decide whether to include the anchor point row for LOCF purposes.
    range_check AS (
        SELECT EXISTS (
            SELECT 1 FROM final_mapping fm_check -- Check the source data before filtering
            WHERE fm_check.event_time >= $effective_from
              AND fm_check.event_time <= $effective_to
        ) AS range_has_direct_hits
    ),

    -- Pre-calculate the final event time after applying LOCF
    locf_applied AS (
        SELECT
            fm.*, -- Include all columns from filtered_mapping
            rc.range_has_direct_hits, -- Include the flag
            atc.anchor_time, -- Include anchor time
            CASE
                WHEN fm.query_time_had_real_change
                    THEN fm.event_time
                    ELSE fm.effective_time
            END as final_event_time
        FROM filtered_mapping fm
        JOIN range_check rc ON 1=1
        JOIN anchor_time_calc atc ON 1=1
    ),

    /*----------------------------------------------------------------------
     * FINAL OUTPUT SELECTION
     *
     * Selects direct hits within the range plus the anchor point for LOCF if needed.
     *---------------------------------------------------------------------*/
    -- Use CTEs for clarity, though could be done inline in UNION
    direct_hits AS (
        SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
        FROM locf_applied la
        WHERE la.event_time >= $effective_from -- Use original event time for range check
          AND la.event_time <= $effective_to
          AND la.final_event_time IS NOT NULL
    ),
    anchor_hit AS (
      SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
      FROM locf_applied la
      WHERE la.anchor_time IS NOT NULL           -- Anchor must exist
        AND la.event_time = la.anchor_time       -- This IS the anchor row
        AND $effective_from > la.anchor_time     -- Query starts after anchor
        AND la.final_event_time IS NOT NULL
        AND NOT EXISTS ( -- Crucially, ensure no direct hit exists AT the start time $from
            SELECT 1 FROM locf_applied dh
            WHERE dh.event_time = $effective_from
        )
    ),
    result AS (
        SELECT event_time, value FROM direct_hits
        UNION ALL -- Use UNION ALL as times should be distinct
        SELECT event_time, value FROM anchor_hit
    )
SELECT DISTINCT event_time, value FROM result
ORDER BY 1;
};

/**
 * get_index_composed: Cache-enabled version of get_index for composed streams.
 * 
 * This function adds cache support to the index calculation for composed streams.
 * The cache is bypassed when:
 * - frozen_at is provided (cache only stores latest data)
 * - base_time is provided (cache stores raw data, not index calculations)
 * 
 * When cache is used, raw data is retrieved from cache and index calculations
 * are performed on the cached data.
 */
CREATE OR REPLACE ACTION get_index_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $base_time INT8
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

    -- Base time determination: Use parameter, metadata, or first event time.
    $effective_base_time INT8;
    if $base_time is not null {
        $effective_base_time := $base_time;
    } else {
        $effective_base_time := get_latest_metadata_int($data_provider, $stream_id, 'default_base_time');
    }
    $effective_base_time := COALESCE($effective_base_time, 0);

    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('Invalid time range: from (%s) > to (%s)', $from, $to));
    }

    -- Permissions check (must be done before cache logic)
    IF !is_allowed_to_read_all($data_provider, $stream_id, $lower_caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }
    IF !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
        ERROR('Not allowed to compose stream');
    }

    -- Cache logic: Only use cache if frozen_at is NULL and base_time is not provided
    -- (cache bypass conditions)
    $cache_enabled BOOL := false;
    $use_cache BOOL := false;
    
    -- Check if cache conditions are met
    if $frozen_at IS NULL AND $base_time IS NULL {
        for $row in tn_cache.is_enabled() {
            $cache_enabled := $row.enabled;
            break;
        }
        
        if $cache_enabled {
            -- Check if we have cached data for this request
            $has_cache BOOL := false;
            for $row in tn_cache.has_cached_data($data_provider, $stream_id, $from, $to) {
                $has_cache := $row.has_data;
                break;
            }
            
            if $has_cache {
                -- Cache hit - get cached data and calculate index
                NOTICE('Cache hit for get_index stream ' || $data_provider || ':' || $stream_id || ' from ' || $from::TEXT || ' to ' || $to::TEXT);
                $use_cache := true;
            } else {
                -- Cache miss - log and fallback to original logic
                NOTICE('Cache miss for get_index stream ' || $data_provider || ':' || $stream_id || ' from ' || $from::TEXT || ' to ' || $to::TEXT);
            }
        }
    }

    -- If using cache, get raw data from cache and calculate index
    if $use_cache {
        -- Get base value from cache (need to get data around base_time)
        $base_value NUMERIC(36,18);
        $found_base_value BOOL := false;
        
        -- Try to get base value from cached data
        for $row in tn_cache.get_cached_data($data_provider, $stream_id, $effective_base_time, $effective_base_time) {
            $base_value := $row.value;
            $found_base_value := true;
            break;
        }
        
        -- If no exact base value found, get data around base_time
        if !$found_base_value {
            -- Get cached data before base_time
            for $row in tn_cache.get_cached_data($data_provider, $stream_id, NULL, $effective_base_time) {
                $base_value := $row.value;
                $found_base_value := true;
                -- Take the last (closest to base_time) value
            }
        }
        
        -- If still no base value, get data after base_time
        if !$found_base_value {
            for $row in tn_cache.get_cached_data($data_provider, $stream_id, $effective_base_time, NULL) {
                $base_value := $row.value;
                $found_base_value := true;
                break; -- Take the first (closest to base_time) value
            }
        }
        
        -- Default base value if nothing found
        if !$found_base_value {
            $base_value := 1::NUMERIC(36,18);
        }
        
        -- Calculate index values from cached data
        if $base_value != 0::NUMERIC(36,18) {
            for $row in tn_cache.get_cached_data($data_provider, $stream_id, $from, $to) {
                $index_value := ($row.value * 100::NUMERIC(36,18)) / $base_value;
                RETURN NEXT $row.event_time, $index_value;
            }
        } else {
            -- Handle zero base value case
            for $row in tn_cache.get_cached_data($data_provider, $stream_id, $from, $to) {
                RETURN NEXT $row.event_time, 0::NUMERIC(36,18);
            }
        }
        
        RETURN;
    }

    -- Original logic fallback (cache miss or cache disabled)
    -- If both $from and $to are NULL, we find the latest event time
    -- and set $effective_from and $effective_to to this single point.
    IF $from IS NULL AND $to IS NULL {
        $actual_latest_event_time INT8;
        $found_latest_event BOOLEAN := FALSE;

        FOR $last_record_row IN get_last_record_composed($data_provider, $stream_id, NULL, $effective_frozen_at) {
            $actual_latest_event_time := $last_record_row.event_time;
            $found_latest_event := TRUE;
            BREAK;
        }

        IF $found_latest_event {
            $effective_from := $actual_latest_event_time; -- Override
            $effective_to   := $actual_latest_event_time; -- Override
        } ELSE {
            -- No records found in the composed stream, so return empty.
            RETURN;
        }
    }
    
    -- If $from and/or $to were provided, $effective_from and $effective_to retain their initial COALESCEd values.
    -- All paths now proceed to the main CTE logic using the (potentially overridden) $effective_from and $effective_to.

    -- For detailed explanations of the CTEs below (hierarchy, primitive_weights,
    -- cleaned_event_times, initial_primitive_states, primitive_events_in_interval,
    -- all_primitive_points, first_value_times, effective_weight_changes, unified_events),
    -- please refer to the comments in the `get_record_composed` action
    -- in 006-composed-query.sql. The logic is largely identical.

    RETURN WITH RECURSIVE
    /*----------------------------------------------------------------------
     * PARENT_DISTINCT_START_TIMES CTE:
     *---------------------------------------------------------------------*/
    parent_distinct_start_times AS (
        SELECT DISTINCT
            data_provider AS parent_dp,
            stream_id AS parent_sid,
            start_time
        FROM taxonomies
        WHERE disabled_at IS NULL
    ),

    /*----------------------------------------------------------------------
     * PARENT_NEXT_STARTS CTE:
     *---------------------------------------------------------------------*/
    parent_next_starts AS (
        SELECT
            parent_dp,
            parent_sid,
            start_time,
            LEAD(start_time) OVER (PARTITION BY parent_dp, parent_sid ORDER BY start_time) as next_start_time
        FROM parent_distinct_start_times
    ),

    /*----------------------------------------------------------------------
     * TAXONOMY_TRUE_SEGMENTS CTE:
     *---------------------------------------------------------------------*/
    taxonomy_true_segments AS (
        SELECT
            t.parent_dp,
            t.parent_sid,
            t.child_dp,
            t.child_sid,
            t.weight_for_segment,
            t.segment_start,
            COALESCE(pns.next_start_time, $max_int8) - 1 AS segment_end
        FROM (
            SELECT
                tx.data_provider AS parent_dp,
                tx.stream_id AS parent_sid,
                tx.child_data_provider AS child_dp,
                tx.child_stream_id AS child_sid,
                tx.weight AS weight_for_segment,
                tx.start_time AS segment_start
            FROM taxonomies tx
            JOIN (
                SELECT
                    data_provider, stream_id, start_time,
                    MAX(group_sequence) as max_gs
                FROM taxonomies
                WHERE disabled_at IS NULL
                GROUP BY data_provider, stream_id, start_time
            ) max_gs_filter
            ON tx.data_provider = max_gs_filter.data_provider
           AND tx.stream_id = max_gs_filter.stream_id
           AND tx.start_time = max_gs_filter.start_time
           AND tx.group_sequence = max_gs_filter.max_gs
            WHERE tx.disabled_at IS NULL
        ) t
        JOIN parent_next_starts pns
          ON t.parent_dp = pns.parent_dp
         AND t.parent_sid = pns.parent_sid
         AND t.segment_start = pns.start_time
    ),

    /*----------------------------------------------------------------------
     * HIERARCHY CTE: 
     * Calculates effective weights that preserve 
     * hierarchical normalization instead of just multiplying raw weights.
     *---------------------------------------------------------------------*/
    hierarchy AS (
      -- Base Case: Direct children of the root composed stream.
      SELECT
          tts.parent_dp AS root_dp,
          tts.parent_sid AS root_sid,
          tts.child_dp AS descendant_dp,
          tts.child_sid AS descendant_sid,
          tts.weight_for_segment AS raw_weight,
          tts.weight_for_segment AS effective_weight, -- For level 1, effective = raw
          tts.segment_start AS path_start,
          tts.segment_end AS path_end,
          1 AS level
      FROM taxonomy_true_segments tts
      WHERE tts.parent_dp = $data_provider AND tts.parent_sid = $stream_id
        AND tts.segment_end >= (
          COALESCE(
              (SELECT t_anchor_base.start_time FROM taxonomies t_anchor_base
               WHERE t_anchor_base.data_provider = $data_provider AND t_anchor_base.stream_id = $stream_id
                 AND t_anchor_base.disabled_at IS NULL AND t_anchor_base.start_time <= $effective_from
               ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC LIMIT 1),
              0
          )
        )
        AND tts.segment_start <= $effective_to

      UNION ALL

      -- Recursive Step: Calculate effective weights properly
      SELECT
          h.root_dp,
          h.root_sid,
          tts.child_dp AS descendant_dp,
          tts.child_sid AS descendant_sid,
          (h.raw_weight * tts.weight_for_segment)::NUMERIC(36,18) AS raw_weight,
          -- Calculate effective weight = parent_effective_weight × (child_weight / sibling_sum)
          (h.effective_weight * (
              tts.weight_for_segment / 
              -- Calculate sum of sibling weights for normalization
              (SELECT SUM(sibling_tts.weight_for_segment) 
               FROM taxonomy_true_segments sibling_tts 
               WHERE sibling_tts.parent_dp = h.descendant_dp 
                 AND sibling_tts.parent_sid = h.descendant_sid
                 AND sibling_tts.segment_start = tts.segment_start
                 AND sibling_tts.segment_end = tts.segment_end)::NUMERIC(36,18)
          ))::NUMERIC(36,18) AS effective_weight,
          GREATEST(h.path_start, tts.segment_start) AS path_start,
          LEAST(h.path_end, tts.segment_end) AS path_end,
          h.level + 1
      FROM
          hierarchy h
      JOIN taxonomy_true_segments tts
          ON h.descendant_dp = tts.parent_dp AND h.descendant_sid = tts.parent_sid
      WHERE
          GREATEST(h.path_start, tts.segment_start) <= LEAST(h.path_end, tts.segment_end)
          AND LEAST(h.path_end, tts.segment_end) >= (
               COALESCE(
                  (SELECT t_anchor_base.start_time FROM taxonomies t_anchor_base
                   WHERE t_anchor_base.data_provider = $data_provider AND t_anchor_base.stream_id = $stream_id
                     AND t_anchor_base.disabled_at IS NULL AND t_anchor_base.start_time <= $effective_from
                   ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC LIMIT 1),
                  0
              )
          )
          AND GREATEST(h.path_start, tts.segment_start) <= $effective_to
          AND h.level < 10 -- Recursion depth limit
    ),

    /*----------------------------------------------------------------------
     * HIERARCHY_PRIMITIVE_PATHS CTE: Updated to use effective_weight
     *--------------------------------------------------------------------*/
    hierarchy_primitive_paths AS (
      SELECT
          h.descendant_dp AS child_data_provider,
          h.descendant_sid AS child_stream_id,
          h.effective_weight AS raw_weight, -- Use effective_weight instead of raw_weight
          h.path_start AS group_sequence_start,
          h.path_end AS group_sequence_end
      FROM hierarchy h
      WHERE EXISTS (
          SELECT 1 FROM streams s
          WHERE s.data_provider = h.descendant_dp  -- Use descendant_dp for matching
            AND s.stream_id     = h.descendant_sid -- Use descendant_sid for matching
            AND s.stream_type   = 'primitive'
      )
    ),

    /*----------------------------------------------------------------------
     * PRIMITIVE_WEIGHTS CTE: Passes through each distinct segment of a primitive's activity.
     * (Modified to be a pass-through like in 006-composed-query.sql)
     *--------------------------------------------------------------------*/
    primitive_weights AS (
      SELECT
          hpp.child_data_provider AS data_provider, -- Keep original expected names
          hpp.child_stream_id     AS stream_id,
          hpp.raw_weight,
          hpp.group_sequence_start,
          hpp.group_sequence_end
      FROM hierarchy_primitive_paths hpp
      -- No GROUP BY, direct pass-through
    ),

    cleaned_event_times AS (
        SELECT DISTINCT event_time
        FROM (
            SELECT pe.event_time
            FROM primitive_events pe
            JOIN primitive_weights pw
              ON pe.data_provider = pw.data_provider
             AND pe.stream_id = pw.stream_id
             AND pe.event_time >= pw.group_sequence_start
             AND pe.event_time <= pw.group_sequence_end
            WHERE pe.event_time > $effective_from
              AND pe.event_time <= $effective_to
              AND pe.created_at <= $effective_frozen_at

            UNION

            SELECT pw.group_sequence_start AS event_time
            FROM primitive_weights pw
            WHERE pw.group_sequence_start > $effective_from
              AND pw.group_sequence_start <= $effective_to
        ) all_times_in_range

        UNION

        SELECT event_time FROM (
            SELECT event_time
            FROM (
                SELECT pe.event_time
                FROM primitive_events pe
                JOIN primitive_weights pw
                  ON pe.data_provider = pw.data_provider
                 AND pe.stream_id = pw.stream_id
                 AND pe.event_time >= pw.group_sequence_start
                 AND pe.event_time <= pw.group_sequence_end
                WHERE pe.event_time <= $effective_from
                  AND pe.created_at <= $effective_frozen_at

                UNION

                SELECT pw.group_sequence_start AS event_time
                FROM primitive_weights pw
                WHERE pw.group_sequence_start <= $effective_from

            ) all_times_before
            ORDER BY event_time DESC
            LIMIT 1
        ) as anchor_event
    ),

    initial_primitive_states AS (
        SELECT
            pe.data_provider,
            pe.stream_id,
            pe.event_time,
            pe.value
        FROM (
            SELECT
                pe_inner.data_provider,
                pe_inner.stream_id,
                pe_inner.event_time,
                pe_inner.value,
                ROW_NUMBER() OVER (
                    PARTITION BY pe_inner.data_provider, pe_inner.stream_id
                    ORDER BY pe_inner.event_time DESC, pe_inner.created_at DESC
                ) as rn
            FROM primitive_events pe_inner
            WHERE pe_inner.event_time <= $effective_from
              AND EXISTS (
                  SELECT 1 FROM primitive_weights pw_exists
                  WHERE pw_exists.data_provider = pe_inner.data_provider AND pw_exists.stream_id = pe_inner.stream_id
              )
              AND pe_inner.created_at <= $effective_frozen_at
        ) pe
        WHERE pe.rn = 1
    ),

    primitive_events_in_interval AS (
        SELECT
            pe.data_provider,
            pe.stream_id,
            pe.event_time,
            pe.value
        FROM (
             SELECT
                pe_inner.data_provider,
                pe_inner.stream_id,
                pe_inner.event_time,
                pe_inner.created_at,
                pe_inner.value,
                ROW_NUMBER() OVER (
                    PARTITION BY pe_inner.data_provider, pe_inner.stream_id, pe_inner.event_time
                    ORDER BY pe_inner.created_at DESC
                ) as rn
            FROM primitive_events pe_inner
            JOIN primitive_weights pw_check
                ON pe_inner.data_provider = pw_check.data_provider
               AND pe_inner.stream_id = pw_check.stream_id
               AND pe_inner.event_time >= pw_check.group_sequence_start
               AND pe_inner.event_time <= pw_check.group_sequence_end
            WHERE pe_inner.event_time > $effective_from
                AND pe_inner.event_time <= $effective_to
                AND pe_inner.created_at <= $effective_frozen_at
        ) pe
        WHERE pe.rn = 1
    ),

    all_primitive_points AS (
        SELECT data_provider, stream_id, event_time, value FROM initial_primitive_states
        UNION ALL
        SELECT data_provider, stream_id, event_time, value FROM primitive_events_in_interval
    ),

    -- Defines the set of primitives that have data points within the requested
    -- time range and therefore require a base value calculation.
    distinct_primitives_for_base AS (
       SELECT DISTINCT data_provider, stream_id
        FROM all_primitive_points
    ),

    -- Base Value Calculation: For each distinct primitive, find its base value
    -- using correlated subqueries for performance. This avoids scanning the
    -- entire history of each primitive.
    -- The value is determined with the following priority:
    --   1. The event at the exact base_time.
    --   2. The latest event before the base_time.
    --   3. The earliest event after the base_time.
    --   4. A default of 1 if no event is found.
    primitive_base_values AS (
        SELECT
            dp.data_provider,
            dp.stream_id,
             COALESCE(
                -- Priority 1: Exact match
                 (SELECT pe.value FROM primitive_events pe
                  WHERE pe.data_provider = dp.data_provider
                    AND pe.stream_id = dp.stream_id
                    AND pe.event_time = $effective_base_time
                    AND pe.created_at <= $effective_frozen_at
                  ORDER BY pe.created_at DESC LIMIT 1),

                -- Priority 2: Latest Before
                 (SELECT pe.value FROM primitive_events pe
                  WHERE pe.data_provider = dp.data_provider
                    AND pe.stream_id = dp.stream_id
                    AND pe.event_time < $effective_base_time
                    AND pe.created_at <= $effective_frozen_at
                   ORDER BY pe.event_time DESC, pe.created_at DESC LIMIT 1),

                -- Priority 3: Earliest After
                  (SELECT pe.value FROM primitive_events pe
                   WHERE pe.data_provider = dp.data_provider
                    AND pe.stream_id = dp.stream_id
                    AND pe.event_time > $effective_base_time
                    AND pe.created_at <= $effective_frozen_at
                   ORDER BY pe.event_time ASC, pe.created_at DESC LIMIT 1),

                  1::numeric(36,18) -- Default value
             )::numeric(36,18) AS base_value
        FROM distinct_primitives_for_base dp
    ),

    -- Calculate Index Value Change (delta_indexed_value) for each primitive event.
    primitive_event_changes AS (
        SELECT
            calc.data_provider,
            calc.stream_id,
            calc.event_time,
            calc.value,
            calc.delta_value,
            -- Calculate the change in indexed value. Handle potential division by zero if base_value is 0.
            CASE
                WHEN COALESCE(pbv.base_value, 0::numeric(36,18)) = 0::numeric(36,18) THEN 0::numeric(36,18) -- Or handle as error/null depending on requirements
                ELSE (calc.delta_value * 100::numeric(36,18) / pbv.base_value)::numeric(36,18)
            END AS delta_indexed_value
        FROM (
            SELECT data_provider, stream_id, event_time, value,
                    COALESCE(value - LAG(value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time), value)::numeric(36,18) AS delta_value
            FROM all_primitive_points
        ) calc
        JOIN primitive_base_values pbv -- Join to get the base value for normalization
            ON calc.data_provider = pbv.data_provider AND calc.stream_id = pbv.stream_id
        WHERE calc.delta_value != 0::numeric(36,18)
    ),

    first_value_times AS (
        SELECT
            data_provider,
            stream_id,
            MIN(event_time) as first_value_time
        FROM all_primitive_points
        GROUP BY data_provider, stream_id
    ),

    effective_weight_changes AS (
        SELECT
            pw.data_provider,
            pw.stream_id,
            GREATEST(pw.group_sequence_start, fvt.first_value_time) AS event_time,
            pw.raw_weight AS weight_delta
        FROM primitive_weights pw
        INNER JOIN first_value_times fvt
            ON pw.data_provider = fvt.data_provider AND pw.stream_id = fvt.stream_id
        WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
          AND pw.raw_weight != 0::numeric(36,18)

        UNION ALL

        SELECT
            pw.data_provider,
            pw.stream_id,
            pw.group_sequence_end + 1 AS event_time,
            -pw.raw_weight AS weight_delta
        FROM primitive_weights pw
        INNER JOIN first_value_times fvt
            ON pw.data_provider = fvt.data_provider AND pw.stream_id = fvt.stream_id
        WHERE 
            GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
            AND pw.raw_weight != 0::numeric(36,18)
            -- don't emit closing delta for open interval
            AND pw.group_sequence_end < ($max_int8 - 1)
    ),

    -- Combine indexed value changes and weight changes.
    unified_events AS (
        SELECT
            pec.data_provider,
            pec.stream_id,
            pec.event_time,
            pec.delta_indexed_value,  -- Use delta_indexed_value here
            0::numeric(36,18) AS weight_delta,
            1 AS event_type_priority -- Value changes first
        FROM primitive_event_changes pec

        UNION ALL

        SELECT
            ewc.data_provider,
            ewc.stream_id,
            ewc.event_time,
            0::numeric(36,18) AS delta_indexed_value, -- Zero indexed value change for weight events
            ewc.weight_delta,
            2 AS event_type_priority -- Weight changes second
        FROM effective_weight_changes ewc
    ),

    -- Calculate state timeline using indexed values.
    primitive_state_timeline AS (
        SELECT
            data_provider,
            stream_id,
            event_time,
            delta_indexed_value,
            weight_delta,
            -- Calculate indexed value and weight *before* this event
            COALESCE(LAG(indexed_value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as indexed_value_before_event,
            COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as weight_before_event
        FROM (
            SELECT
                data_provider,
                stream_id,
                event_time,
                delta_indexed_value,
                weight_delta,
                event_type_priority, -- Select for ordering
                -- Cumulative indexed value up to and including this event
                (SUM(delta_indexed_value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as indexed_value_after_event,
                -- Cumulative weight up to and including this event
                (SUM(weight_delta) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as weight_after_event
            FROM unified_events
        ) state_calc
    ),

    -- Calculate final aggregated deltas using indexed values.
    final_deltas AS (
        SELECT
            event_time,
            -- Calculate delta for the weighted sum numerator using indexed values
            SUM(
                (delta_indexed_value * weight_before_event) +
                (indexed_value_before_event * weight_delta) +
                (delta_indexed_value * weight_delta) -- Added missing term for indexed calculation
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

    -- Calculate cumulative sums for indexed weighted sum and sum of weights.
    cumulative_values AS (
        SELECT
            act.time_point as event_time,
            (COALESCE((SUM(fd.delta_ws_indexed) OVER (ORDER BY act.time_point ASC))::numeric(72,18), 0::numeric(72,18))) as cum_ws_indexed,
            (COALESCE((SUM(fd.delta_sw) OVER (ORDER BY act.time_point ASC))::numeric(36,18), 0::numeric(36,18))) as cum_sw
        FROM all_combined_times act
        LEFT JOIN final_deltas fd ON fd.event_time = act.time_point
    ),

    -- Compute the final aggregated index value (Weighted Average of Indexed Values)
    aggregated AS (
        SELECT cv.event_time,
               CASE WHEN cv.cum_sw = 0::numeric(36,18) THEN 0::numeric(72,18)
                    -- Divide cumulative indexed weighted sum by cumulative sum of weights
                    ELSE cv.cum_ws_indexed / cv.cum_sw::numeric(72,18)
                   END AS value
        FROM cumulative_values cv
    ),

    -- LOCF Logic (Identical to get_record_composed)
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

    -- Check if there are any rows from aggregated whose event_time falls directly within the requested range.
    -- This helps decide whether to include the anchor point row for LOCF purposes.
    range_check AS (
        SELECT EXISTS (
            SELECT 1 FROM final_mapping fm_check  -- Check the source data before filtering
            WHERE fm_check.event_time >= $effective_from
              AND fm_check.event_time <= $effective_to
        ) AS range_has_direct_hits
    ),

    -- Pre-calculate the final event time after applying LOCF
    locf_applied AS (
        SELECT
            fm.*,  -- Include all columns from filtered_mapping
            rc.range_has_direct_hits, -- Include the flag
            atc.anchor_time, -- Include anchor time
            CASE
                WHEN fm.query_time_had_real_change THEN fm.event_time
                ELSE fm.effective_time
            END as final_event_time
        FROM filtered_mapping fm
        JOIN range_check rc ON 1=1
        JOIN anchor_time_calc atc ON 1=1
    ),

    /*----------------------------------------------------------------------
     * FINAL OUTPUT SELECTION
     *
     * Selects direct hits within the range plus the anchor point for LOCF if needed.
     *---------------------------------------------------------------------*/
    -- Use CTEs for clarity, though could be done inline in UNION
    direct_hits AS (
        SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
        FROM locf_applied la
        WHERE la.event_time >= $effective_from -- Use original event time for range check
          AND la.event_time <= $effective_to
          AND la.final_event_time IS NOT NULL
    ),
    anchor_hit AS (
      SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
      FROM locf_applied la
      WHERE la.anchor_time IS NOT NULL        -- Anchor must exist
        AND la.event_time = la.anchor_time    -- This IS the anchor row
        AND $effective_from > la.anchor_time  -- Query starts after anchor
        AND la.final_event_time IS NOT NULL
        AND NOT EXISTS ( -- Crucially, ensure no direct hit exists AT the start time $from
            SELECT 1 FROM locf_applied dh
            WHERE dh.event_time = $effective_from
        )
    ),
    result AS (
        SELECT event_time, value FROM direct_hits
        UNION ALL  -- Use UNION ALL as times should be distinct
        SELECT event_time, value FROM anchor_hit
    )
    SELECT DISTINCT event_time, value FROM result
    ORDER BY 1;
};