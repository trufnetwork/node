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
    IF !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }
    IF !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
        ERROR('Not allowed to compose stream');
    }

    RETURN WITH RECURSIVE
    /*----------------------------------------------------------------------
     * HIERARCHY CTE: Recursively resolves the dependency tree defined by taxonomies.
     *
     * Purpose: Determines the effective weighted contribution of every stream
     * (down to the primitives) to the root composed stream over time.
     *
     * - Calculates the cumulative `raw_weight` for each path from the root to a child.
     * - Determines the validity interval (`group_sequence_start`, `group_sequence_end`)
     *   for each weighted relationship, handling overshadowing definitions.
     * - Filters based on the query time range (`$effective_from`, `$effective_to`)
     *   and an anchor point before `$effective_from`.
     *
     * Overshadowing Logic: Uses a LEFT JOIN anti-join pattern (`t2 IS NULL`) to select
     * only the taxonomy definition with the highest `group_sequence` for any given
     * `start_time`, ensuring that later definitions supersede earlier ones.
     *
     * Interval Calculation:
     *   - `group_sequence_end` is derived using `LEAD` to find the next `start_time`
     *     for the same parent stream.
     *   - In the recursive step, the effective interval is the intersection
     *     (GREATEST start, LEAST end) of the parent's and child's intervals.
     *---------------------------------------------------------------------*/
    hierarchy AS (
      -- Base Case: Direct children of the root composed stream.
      SELECT
          t1.data_provider AS parent_data_provider,
          t1.stream_id AS parent_stream_id,
          t1.child_data_provider,
          t1.child_stream_id,
          t1.weight AS raw_weight,
          t1.start_time AS group_sequence_start,
          -- Calculate end time based on the start of the next definition
          COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
      FROM
          taxonomies t1
      -- Anti-join: Ensures we select the row with the highest group_sequence
      -- for a given (dp, sid, start_time) by checking if a row t2 with a
      -- higher sequence exists. We keep t1 only if no such t2 is found.
      LEFT JOIN taxonomies t2
        ON t1.data_provider = t2.data_provider
       AND t1.stream_id = t2.stream_id
       AND t1.start_time = t2.start_time
       AND t1.group_sequence < t2.group_sequence -- t2 must have a strictly higher sequence
       AND t2.disabled_at IS NULL -- Ignore disabled rows for overshadowing comparison
      -- Join to find the start_time of the next taxonomy definition for this parent
      JOIN (
          SELECT
              dt.data_provider, dt.stream_id, dt.start_time,
              LEAD(dt.start_time) OVER (PARTITION BY dt.data_provider, dt.stream_id ORDER BY dt.start_time) AS next_start
          FROM ( -- Select distinct start times within the relevant range plus anchor
                 SELECT DISTINCT t_ot.data_provider, t_ot.stream_id, t_ot.start_time FROM taxonomies t_ot
                 WHERE t_ot.data_provider = $data_provider AND t_ot.stream_id = $stream_id AND t_ot.disabled_at IS NULL AND t_ot.start_time <= $effective_to
                 -- Anchor logic: Find the latest taxonomy start at or before $effective_from
                 AND t_ot.start_time >= COALESCE((SELECT t2_anchor.start_time FROM taxonomies t2_anchor WHERE t2_anchor.data_provider=t_ot.data_provider AND t2_anchor.stream_id=t_ot.stream_id AND t2_anchor.disabled_at IS NULL AND t2_anchor.start_time<=$effective_from ORDER BY t2_anchor.start_time DESC, t2_anchor.group_sequence DESC LIMIT 1),0)
               ) dt
      ) ot
        ON t1.data_provider = ot.data_provider
       AND t1.stream_id     = ot.stream_id
       AND t1.start_time    = ot.start_time
      WHERE
          t1.data_provider = $data_provider -- Filter for the specific root stream
      AND t1.stream_id     = $stream_id
      AND t1.disabled_at   IS NULL
      AND t2.group_sequence IS NULL -- Keep t1 only if no row t2 with a higher sequence was found
      -- Apply time range filter to the taxonomy start time
      AND t1.start_time <= $effective_to
      -- Anchor logic: Ensure we include the relevant taxonomy active at $effective_from
      AND t1.start_time >= COALESCE(
            (SELECT t_anchor_base.start_time
             FROM taxonomies t_anchor_base
             WHERE t_anchor_base.data_provider = t1.data_provider -- Correlated subquery
               AND t_anchor_base.stream_id     = t1.stream_id     -- Correlated subquery
               AND t_anchor_base.disabled_at   IS NULL
               AND t_anchor_base.start_time   <= $effective_from
             ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC
             LIMIT 1
            ), 0 -- Default to 0 if no anchor found
          )

      UNION ALL

      -- Recursive Step: Children of the children found in the previous level.
      SELECT
          parent.parent_data_provider,
          parent.parent_stream_id,
          t1_child.child_data_provider,
          t1_child.child_stream_id,
          -- Multiply parent weight by child weight for cumulative effect
          (parent.raw_weight * t1_child.weight)::NUMERIC(36,18) AS raw_weight,
          -- Effective interval start is the later of the parent's or child's start
          GREATEST(parent.group_sequence_start, t1_child.start_time) AS group_sequence_start,
          -- Effective interval end is the earlier of the parent's or child's end
          LEAST(parent.group_sequence_end, (COALESCE(ot_child.next_start, $max_int8) - 1)) AS group_sequence_end
      FROM
          hierarchy parent -- Result from the previous recursion level
      -- Join parent with potential child taxonomies
      JOIN taxonomies t1_child
        ON t1_child.data_provider = parent.child_data_provider
       AND t1_child.stream_id     = parent.child_stream_id
      -- Anti-join for child overshadowing (same pattern as base case)
      LEFT JOIN taxonomies t2_child
        ON t1_child.data_provider = t2_child.data_provider
       AND t1_child.stream_id = t2_child.stream_id
       AND t1_child.start_time = t2_child.start_time
       AND t1_child.group_sequence < t2_child.group_sequence -- t2 must be higher
       AND t2_child.disabled_at IS NULL -- Ignore disabled rows
      -- Join to get the next start time for the child interval end calculation
      JOIN (
          SELECT
              dt.data_provider, dt.stream_id, dt.start_time,
              LEAD(dt.start_time) OVER ( PARTITION BY dt.data_provider, dt.stream_id ORDER BY dt.start_time ) AS next_start
          FROM ( -- Select distinct start times for all potentially relevant children
                 SELECT DISTINCT t_otc.data_provider, t_otc.stream_id, t_otc.start_time FROM taxonomies t_otc
                 WHERE t_otc.disabled_at IS NULL AND t_otc.start_time <= $effective_to
                 -- Anchor logic for children (find earliest relevant start)
                 AND t_otc.start_time >= COALESCE((SELECT MIN(t2_min.start_time) FROM taxonomies t2_min WHERE t2_min.disabled_at IS NULL AND t2_min.start_time <= $effective_from), 0)
               ) dt
      ) ot_child
        ON t1_child.data_provider = ot_child.data_provider
       AND t1_child.stream_id     = ot_child.stream_id
       AND t1_child.start_time    = ot_child.start_time
      WHERE
          t1_child.disabled_at IS NULL
      AND t2_child.group_sequence IS NULL -- Keep t1_child only if no higher sequence row found
      -- Interval Overlap Check: Ensure parent and child intervals overlap
      AND t1_child.start_time <= parent.group_sequence_end
      AND (COALESCE(ot_child.next_start, $max_int8) - 1) >= parent.group_sequence_start
      -- Apply child time range/anchor filters to t1_child
      AND t1_child.start_time <= $effective_to
      AND t1_child.start_time >= COALESCE(
            (SELECT t_anchor_child.start_time
             FROM taxonomies t_anchor_child
             WHERE t_anchor_child.data_provider = t1_child.data_provider -- Correlated
               AND t_anchor_child.stream_id     = t1_child.stream_id     -- Correlated
               AND t_anchor_child.disabled_at   IS NULL
               AND t_anchor_child.start_time   <= $effective_from
             ORDER BY t_anchor_child.start_time DESC, t_anchor_child.group_sequence DESC
             LIMIT 1
            ), 0
          )
    ),

    /*----------------------------------------------------------------------
     * PRIMITIVE_WEIGHTS CTE: Filters the hierarchy to find leaf nodes (primitives).
     *
     * Purpose: Extracts the final effective weight and validity interval for each
     * primitive stream that contributes to the composed result. A primitive may appear
     * multiple times if its effective weight changes due to taxonomy updates higher
     * up the tree.
     *--------------------------------------------------------------------*/
    primitive_weights AS (
      SELECT
          h.child_data_provider AS data_provider,
          h.child_stream_id     AS stream_id,
          h.raw_weight,
          h.group_sequence_start,
          h.group_sequence_end
      FROM hierarchy h
      -- Join with streams table to identify primitives
      WHERE EXISTS (
          SELECT 1 FROM streams s
          WHERE s.data_provider = h.child_data_provider
            AND s.stream_id     = h.child_stream_id
            AND s.stream_type   = 'primitive'
      )
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
    ),

    -- Step 7: Combine value and *effective* weight changes into a unified timeline.
    unified_events AS (
        SELECT
            pec.data_provider,
            pec.stream_id,
            pec.event_time,
            pec.delta_value,
            0::numeric(36,18) AS weight_delta
        FROM primitive_event_changes pec

        UNION ALL

        -- *Effective* Weight changes (deltas)
        SELECT
            ewc.data_provider,
            ewc.stream_id,
            ewc.event_time,
            0::numeric(36,18) AS delta_value,
            ewc.weight_delta
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
            COALESCE(LAG(value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time), 0::numeric(36,18)) as value_before_event,
            COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time), 0::numeric(36,18)) as weight_before_event
        FROM (
            SELECT
                data_provider,
                stream_id,
                event_time,
                delta_value,
                weight_delta,
                -- Cumulative value up to and including this event
                (SUM(delta_value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time))::numeric(36,18) as value_after_event,
                -- Cumulative weight up to and including this event
                (SUM(weight_delta) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time))::numeric(36,18) as weight_after_event
            FROM unified_events
        ) state_calc
    ),

    -- Step 9: Calculate final aggregated deltas per time point.
    final_deltas AS ( -- Renamed from new_final_deltas to match original naming convention
        SELECT
            event_time,
            SUM((delta_value * weight_before_event) + (weight_delta * value_before_event))::numeric(72, 18) AS delta_ws,
            SUM(weight_delta)::numeric(36, 18) AS delta_sw
        FROM primitive_state_timeline
        GROUP BY event_time
        HAVING SUM((delta_value * weight_before_event) + (weight_delta * value_before_event))::numeric(72, 18) != 0::numeric(72, 18)
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
        SELECT DISTINCT event_time AS time_point
        FROM final_deltas -- Already filtered for non-zero deltas
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
    SELECT event_time, value FROM result
    ORDER BY 1;
};