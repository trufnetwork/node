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

    -- for $row in WITH RECURSIVE
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
     * DELTA CALCULATION METHOD: Computes the composed value efficiently.
     *
     * Concept: Instead of calculating the full weighted average at every time point,
     * this method computes the *change* (delta) in the numerator (weighted sum of values, `delta_ws`)
     * and the *change* in the denominator (sum of weights, `delta_sw`) only when
     * these values actually change. The final value is derived from the cumulative
     * sums of these deltas. Special adjustments are made at root taxonomy boundaries.
     *---------------------------------------------------------------------*/

    -- Step 1a: Find the initial state (latest value) of each relevant primitive AT OR BEFORE $effective_from.
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
            JOIN primitive_weights pw_check -- Ensure primitive is relevant and event is within *a* valid weight interval
              ON pe_inner.data_provider = pw_check.data_provider
             AND pe_inner.stream_id = pw_check.stream_id
             AND pe_inner.event_time >= pw_check.group_sequence_start
             AND pe_inner.event_time <= pw_check.group_sequence_end
            WHERE pe_inner.event_time <= $effective_from -- At or before the start
              AND pe_inner.created_at <= $effective_frozen_at
        ) pe
        WHERE pe.rn = 1 -- Select the latest state
    ),

    -- Step 1b: Find distinct primitive events strictly WITHIN the interval ($from < time <= $to).
    -- Handles overshadowing based on created_at for events with the same event_time.
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

    -- Step 1c: Combine initial states and interval events into a single point series per primitive.
    all_primitive_points AS (
        SELECT
            ips.data_provider,
            ips.stream_id,
            ips.event_time, -- Use the original event time
            ips.value
        FROM initial_primitive_states ips

        UNION ALL

        SELECT
            pei.data_provider,
            pei.stream_id,
            pei.event_time,
            pei.value
        FROM primitive_events_in_interval pei
    ),

    -- Step 1d: Calculate the change in value (`delta_value`) for each primitive at its event times.
    -- Uses LAG to compare the current value with the previous one for the same primitive.
    -- Filters out points where the value didn't actually change.
    primitive_event_changes AS (
        SELECT * FROM (
            SELECT
                data_provider, stream_id, event_time, value,
                -- Calculate delta: current value - previous value (or just current value if first)
                COALESCE(
                    value - LAG(value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time),
                    value
                )::numeric(36,18) AS delta_value
            FROM all_primitive_points
        ) calc
        WHERE delta_value != 0::numeric(36,18) -- Only keep actual changes
    ),

    -- *** Optimized Weight Calculation and Application ***
    -- Step 2a: Determine the total weight active for each primitive over time.
    -- Generates weight change events (+weight at start, -weight at end+1)
    weight_changes AS (
        SELECT
            data_provider,
            stream_id,
            group_sequence_start AS change_time,
            raw_weight           AS weight_delta
        FROM primitive_weights
        WHERE raw_weight != 0::numeric(36,18)

        UNION ALL

        SELECT
            data_provider,
            stream_id,
            group_sequence_end + 1 AS change_time, -- Change occurs *after* the end
            -raw_weight            AS weight_delta
        FROM primitive_weights
        WHERE raw_weight != 0::numeric(36,18)
    ),

    -- Calculate the running total weight for each primitive at each weight change point.
    weight_timeline AS (
        SELECT
            wc1.data_provider,
            wc1.stream_id,
            wc1.change_time,
            -- Calculate cumulative sum using window function
            (SUM(wc1.weight_delta) OVER (
                PARTITION BY wc1.data_provider, wc1.stream_id
                ORDER BY wc1.change_time
            ))::numeric(36,18) AS total_weight_at_time
        FROM weight_changes wc1
    ),

    -- Define continuous segments where each primitive has a constant total weight.
    primitive_weight_segments AS (
        SELECT
            data_provider,
            stream_id,
            change_time AS segment_start,
            -- Segment ends just before the next change time
            LEAD(change_time, 1, $max_int8) OVER (PARTITION BY data_provider, stream_id ORDER BY change_time ASC) AS segment_end,
            total_weight_at_time
        FROM weight_timeline
        -- Filter out segments where weight is zero, as they don't contribute
        WHERE total_weight_at_time != 0::numeric(36,18)
    ),

    -- Step 2b: Join primitive value deltas with their corresponding active weight segment.
    -- This finds the weight that should be applied to each value change.
    weighted_event_deltas AS (
        SELECT
            pec.event_time,
            pec.delta_value,
            -- Find the weight segment active at the time of the value change
            COALESCE(pws.total_weight_at_time, 0::numeric(36,18))::numeric(36, 18) AS active_weight_sum
        FROM
            primitive_event_changes pec
        LEFT JOIN -- Use LEFT JOIN in case a value change happens outside any active weight segment (should be rare)
            primitive_weight_segments pws ON pec.data_provider = pws.data_provider
                                         AND pec.stream_id = pws.stream_id
                                         AND pec.event_time >= pws.segment_start -- Event time is within the segment
                                         AND pec.event_time < pws.segment_end    -- Segment end is exclusive
    ),

    -- Step 2c: Calculate the change in the weighted sum (`delta_ws`) caused by each event.
    -- This is the core component for the numerator of the weighted average.
    weighted_value_deltas AS (
        SELECT
            event_time,
            -- Sum the contributions of all primitive value changes occurring at the same time
            SUM(delta_value * active_weight_sum)::numeric(72, 18) AS delta_ws -- Weighted sum delta
        FROM weighted_event_deltas
        GROUP BY event_time
    ),

    -- Step 2d: Find the first time each primitive contributes a value.
    -- Used to determine when a primitive's weight becomes *effectively* active.
    first_value_times AS (
        SELECT
            data_provider,
            stream_id,
            MIN(event_time) as first_value_time
        FROM all_primitive_points -- Based on combined initial state and interval events
        GROUP BY data_provider, stream_id
    ),

    -- Step 2e: Calculate the change in the *effective sum of weights* (`delta_sw`).
    -- This considers when weights become active based on `first_value_time`.
    -- This is the core component for the denominator of the weighted average.
    eff_weight_deltas_unaggr AS (
        -- Positive delta when a weight interval becomes *effectively* active
        SELECT
            -- Effective start is the later of the interval start or the first value time
            GREATEST(pw.group_sequence_start, fvt.first_value_time) AS time_point,
            pw.raw_weight AS delta,
            pw.data_provider,
            pw.stream_id
        FROM primitive_weights pw
        INNER JOIN first_value_times fvt -- Only consider primitives that have values
          ON pw.data_provider = fvt.data_provider AND pw.stream_id = fvt.stream_id
        -- Ensure the effective interval is valid (start <= end)
        WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
          AND pw.raw_weight != 0::numeric(36,18)

        UNION ALL

        -- Negative delta when a weight interval becomes *effectively* inactive
        SELECT
            -- Effective end+1 determines when the weight stops applying
            pw.group_sequence_end + 1 AS time_point,
            -pw.raw_weight AS delta,
            pw.data_provider,
            pw.stream_id
        FROM primitive_weights pw
        INNER JOIN first_value_times fvt
          ON pw.data_provider = fvt.data_provider AND pw.stream_id = fvt.stream_id
        -- Ensure the effective interval is valid (start <= end)
        WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
          AND pw.raw_weight != 0::numeric(36,18)
    ),

    -- Aggregate effective weight changes occurring at the same time point.
    effective_weight_deltas AS (
        SELECT
            time_point,
            SUM(delta)::numeric(36,18) as effective_delta_sw -- Sum of weights delta
        FROM eff_weight_deltas_unaggr
        GROUP BY time_point
        HAVING SUM(delta)::numeric(36,18) != 0::numeric(36,18) -- Only keep points with actual change
    ),

    -- Step 1 (Find V_before): Find the last known value for each primitive just before its weight changes.
    prim_value_before_wgt_change AS (
        SELECT
            ewdu.time_point,
            ewdu.data_provider,
            ewdu.stream_id,
            ewdu.delta AS weight_change,
            -- Correlated subquery to find the value just before the weight change time_point
            (SELECT app.value
             FROM all_primitive_points app
             WHERE app.data_provider = ewdu.data_provider
               AND app.stream_id = ewdu.stream_id
               AND app.event_time < ewdu.time_point -- Strictly before the change
             ORDER BY app.event_time DESC -- Get the latest value before the change
             LIMIT 1
            ) AS primitive_value_before
        FROM eff_weight_deltas_unaggr ewdu
        WHERE ewdu.delta != 0::numeric(36,18) -- Only consider actual weight changes
    ),

    -- Step 2 & 3 (Calculate & Aggregate V*dW): Calculate the weighted sum delta arising purely from weight changes
    -- by multiplying the value before the change by the weight change, then summing per time point.
    weight_change_deltas AS (
       SELECT
           time_point AS event_time,
           -- Step 2: Calculate V_before * weight_change, handling NULL V_before (means no prior value)
           SUM(COALESCE(primitive_value_before, 0::numeric(36,18)) * weight_change)::numeric(72, 18) AS delta_ws
       FROM prim_value_before_wgt_change
       GROUP BY time_point
    ),

    -- Step 3: Combine all time points where either the weighted value sum (`delta_ws`)
    -- or the effective weight sum (`delta_sw`) changes. Also include user-requested times.
    all_combined_times AS (
        SELECT event_time as time_point FROM weighted_value_deltas
        UNION
        SELECT time_point FROM effective_weight_deltas ewd
        UNION
        SELECT event_time as time_point FROM weight_change_deltas -- Add times where V*dW changes
        UNION
        SELECT event_time as time_point FROM cleaned_event_times -- Ensures query bounds and anchor are present
    ),

    -- NOTE: This currently only includes deltas from primitive value changes (delta_ws)
    -- and effective weight sum changes (delta_sw). It is MISSING the delta_ws component
    -- that arises purely from weight changes (V * dW). This missing component needs
    -- to be calculated in `weight_change_deltas` and added here.
    combined_deltas AS (
        SELECT
            time_point as event_time,
            SUM(delta_ws)::numeric(72,18) as delta_ws,
            SUM(delta_sw)::numeric(36,18) as delta_sw
        FROM (
            -- Weighted value deltas (from primitive value changes)
            SELECT act.time_point, wvd.delta_ws, 0::numeric(36,18) AS delta_sw
            FROM weighted_value_deltas wvd
            JOIN all_combined_times act ON wvd.event_time = act.time_point

            UNION ALL

            -- Effective weight sum deltas (from weight changes)
            SELECT act.time_point, 0::numeric(72,18) AS delta_ws, ewd.effective_delta_sw AS delta_sw
            FROM effective_weight_deltas ewd
            JOIN all_combined_times act ON ewd.time_point = act.time_point
        ) combined
        GROUP BY time_point
    ),

        /*----------------------------------------------------------------------
     * FINAL_DELTAS CTE: Merges regular deltas with boundary adjustments.
     *
     * Purpose: Creates the definitive set of deltas (`delta_ws`, `delta_sw`) for each time point.
     * Must combine all three components that affect the final composed value:
     * 1. Changes due to primitive value updates
     * 2. Changes in the effective weight sum
     * 3. Changes in the weighted sum due purely to weight changes (V * dW)
     *---------------------------------------------------------------------*/
    final_deltas AS (
        -- Combine all sources of deltas:
        -- 1. Deltas from primitive value changes (`delta_ws`) and effective weight sum changes (`delta_sw`) from `combined_deltas`.
        -- 2. Deltas from weighted sum changes due *only* to weight changes (`V*dW`) from `weight_change_deltas`.
        -- Group by event_time to sum contributions if a time point has multiple delta sources.
        SELECT
            event_time,
            SUM(delta_ws)::numeric(72,18) as delta_ws,
            SUM(delta_sw)::numeric(36,18) as delta_sw
        FROM (
            -- Deltas from combined_deltas (value changes & weight sum changes)
            SELECT event_time, delta_ws, delta_sw
            FROM combined_deltas

            UNION ALL

            -- Deltas from weight_change_deltas (V*dW component)
            SELECT event_time, delta_ws, 0::numeric(36,18) AS delta_sw -- This CTE only calculates delta_ws
            FROM weight_change_deltas
        ) all_delta_sources
        GROUP BY event_time
    ),


    -- Step 5: Calculate the cumulative sum of the final deltas over time.
    -- `cum_ws` = running total weighted sum (numerator)
    -- `cum_sw` = running total sum of weights (denominator)
    cumulative_values AS (
        SELECT
            fd.event_time,
            (SUM(fd.delta_ws) OVER (ORDER BY fd.event_time ASC))::numeric(72,18) as cum_ws,
            (SUM(fd.delta_sw) OVER (ORDER BY fd.event_time ASC))::numeric(36,18) as cum_sw
        FROM final_deltas fd
        -- Include all relevant time points, ensuring rows exist even if delta is zero at that point
        JOIN all_combined_times act ON fd.event_time = act.time_point
    ),

    -- Step 6: Compute the aggregated value (Weighted Average) at each time point.
    -- Handles division by zero if the sum of weights is zero.
    aggregated AS (
        SELECT
            cv.event_time,
            CASE WHEN cv.cum_sw = 0::numeric(36,18) THEN 0::numeric(72,18)
                 -- Perform division using higher precision for intermediate calculation
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
    -- Step 7: Identify time points where the *final* composed value or weight sum actually changed.
    -- These are the "real" event points used as sources for LOCF.
    real_change_times AS (
        SELECT DISTINCT event_time AS time_point
        FROM final_deltas
        WHERE delta_ws != 0::numeric(72,18) OR delta_sw != 0::numeric(36,18)
    ),

    -- Calculate the anchor time needed for gap filling before the query range starts.
    anchor_time_calc AS (
        SELECT MAX(time_point) as anchor_time
        FROM real_change_times
        WHERE time_point < $effective_from -- Strictly before the requested start
    ),

    -- Step 8: Map each calculated time point in `aggregated` to its effective LOCF source time.
    final_mapping AS (
        SELECT
            agg.event_time,
            agg.value,
            -- Find the latest "real" change time at or before this aggregated time point
            (SELECT MAX(rct.time_point)
             FROM real_change_times rct
             WHERE rct.time_point <= agg.event_time) AS effective_time,
            -- Flag indicating if this aggregated time point itself corresponds to a real change
            EXISTS (SELECT 1 FROM real_change_times rct WHERE rct.time_point = agg.event_time) AS query_time_had_real_change
        FROM aggregated agg
    ),

    -- Filter the mapped results to include only the query range plus the necessary anchor point for LOCF.
    filtered_mapping AS (
        SELECT fm.*
        FROM final_mapping fm
        JOIN anchor_time_calc atc ON 1=1 -- Make anchor_time available
        WHERE
            -- Include rows within the requested query range [$from, $to]
            (fm.event_time >= $effective_from AND fm.event_time <= $effective_to)
            OR
            -- Include the anchor point row if it exists and matches an aggregated time
            (atc.anchor_time IS NOT NULL AND fm.event_time = atc.anchor_time)
    )

    /*----------------------------------------------------------------------
     * FINAL OUTPUT SELECTION
     *
     * Applies LOCF: Selects the `effective_time` if the current `event_time`
     * wasn't a real change point. Casts the value to final precision.
     * Filters out null times and orders the result.
     *---------------------------------------------------------------------*/
    SELECT
        -- Apply LOCF: Use the original time if it had a real change, otherwise use the last real change time.
        CASE
            WHEN fm.query_time_had_real_change THEN fm.event_time
            ELSE fm.effective_time
        END as event_time,
        fm.value::NUMERIC(36,18) -- Cast final result back to required precision
    FROM filtered_mapping fm
    -- Filter out potential null results if no data exists before the first delta
    WHERE CASE WHEN fm.query_time_had_real_change THEN fm.event_time ELSE fm.effective_time END IS NOT NULL
    ORDER BY 1; -- Order by the final event_time
    -- SELECT * from weight_timeline {
    --   NOTICE('weight_timeline' ||
    --   ' data_provider: ' || $row.data_provider::text || ' ' ||
    --   ' stream_id: ' || $row.stream_id::text || ' ' ||
    --   ' change_time: ' || $row.change_time::text || ' ' ||
    --   ' total_weight_at_time: ' || $row.total_weight_at_time::text);
    -- }
};