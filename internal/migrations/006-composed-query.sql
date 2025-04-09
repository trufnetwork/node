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
     * TOP_LEVEL_PERIODS & PERIOD_CHANGE_TIMES CTEs: Identify root taxonomy changes.
     *
     * Purpose: Determine the exact timestamps within the query range where the
     * definition (children or weights) of the *root* composed stream itself changes.
     * These points require special boundary adjustments in the delta calculation.
     *---------------------------------------------------------------------*/
    top_level_periods AS (
        -- Find distinct start times for the root stream's taxonomy definitions
        -- within the relevant time range + anchor, using the anti-join for overshadowing.
        SELECT DISTINCT
            t1.start_time AS period_start,
            COALESCE(ot.next_start, $max_int8) - 1 AS period_end
        FROM
            taxonomies t1
        LEFT JOIN taxonomies t2 -- Anti-join to get MAX(group_sequence) for each start_time
            ON t1.data_provider = t2.data_provider
            AND t1.stream_id = t2.stream_id
            AND t1.start_time = t2.start_time
            AND t1.group_sequence < t2.group_sequence
            AND t2.disabled_at IS NULL
        JOIN ( -- Join to get next start time for period_end calculation
            SELECT
                dt.start_time,
                LEAD(dt.start_time) OVER (ORDER BY dt.start_time) AS next_start
            FROM ( -- Distinct start times for the root stream within relevant range + anchor
                SELECT DISTINCT t_ot.start_time
                FROM taxonomies t_ot
                WHERE t_ot.data_provider = $data_provider
                  AND t_ot.stream_id = $stream_id
                  AND t_ot.disabled_at IS NULL
                  AND t_ot.start_time <= $effective_to
                  AND t_ot.start_time >= COALESCE( -- Anchor logic
                        (SELECT t2_anchor.start_time
                         FROM taxonomies t2_anchor
                         WHERE t2_anchor.data_provider = t_ot.data_provider
                           AND t2_anchor.stream_id = t_ot.stream_id
                           AND t2_anchor.disabled_at IS NULL
                           AND t2_anchor.start_time <= $effective_from
                         ORDER BY t2_anchor.start_time DESC, t2_anchor.group_sequence DESC
                         LIMIT 1),
                        0)
            ) dt
        ) ot ON t1.start_time = ot.start_time
        WHERE
            t1.data_provider = $data_provider -- Only the root stream
        AND t1.stream_id = $stream_id
        AND t1.disabled_at IS NULL
        AND t2.group_sequence IS NULL -- Keep only rows with max group_sequence
        AND t1.start_time <= $effective_to -- Apply range filter
        AND t1.start_time >= COALESCE( -- Anchor logic
            (SELECT t_anchor_base.start_time
             FROM taxonomies t_anchor_base
             WHERE t_anchor_base.data_provider = t1.data_provider
               AND t_anchor_base.stream_id = t1.stream_id
               AND t_anchor_base.disabled_at IS NULL
               AND t_anchor_base.start_time <= $effective_from
             ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC
             LIMIT 1),
            0)
    ),

    -- Extract the start times representing actual changes within the query range.
    period_change_times AS (
        SELECT period_start AS change_time
        FROM top_level_periods
        -- Exclude the initial state's start time if it falls before or at $effective_from
        WHERE period_start > (
                SELECT COALESCE(MIN(pstart.period_start), -1) -- Use -1 if no periods before/at from
                FROM top_level_periods pstart
                WHERE pstart.period_start <= $effective_from
              )
          AND period_start > $effective_from -- Only consider changes *after* the requested start
          AND period_start <= $effective_to -- Only consider changes *within* the requested range
    ),

    /*----------------------------------------------------------------------
     * CLEANED_EVENT_TIMES CTE: Gathers all essential timestamps for calculation.
     *
     * Purpose: Creates a distinct set of all time points where the composed
     * value might change. This includes primitive events, taxonomy changes,
     -- THINK: reconsider this. we don't need to artificially create a change relative to the request.
     * the query start time, and an anchor point before the query start.
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

            UNION

            -- 3. The start of the requested range itself
            -- THINK: reconsider this. we don't need to artificially create a change relative to the request.
            SELECT $effective_from AS event_time

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
            data_provider,
            stream_id,
            change_time,
            -- Cumulative sum of weight deltas defines the total weight active *after* this time
            -- THINK: reconsider this. SUM OVER with partitions have a bug on kwil side. maybe we should find an alternative 
            -- and remember to comment about it why we didn't use sum over.
            (SUM(weight_delta) OVER (PARTITION BY data_provider, stream_id ORDER BY change_time ASC))::numeric(36,18) AS total_weight_at_time
        FROM weight_changes
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
            pw.raw_weight AS delta
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
            -pw.raw_weight AS delta
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
        HAVING SUM(delta)::numeric(36,18) != 0 -- Only keep points with actual change
    ),

    -- Step 3: Combine all time points where either the weighted value sum (`delta_ws`)
    -- THINK: reconsider this. we should not need to include user-requested times.
    -- or the effective weight sum (`delta_sw`) changes. Also include user-requested times.
    all_combined_times AS (
        SELECT event_time as time_point FROM weighted_value_deltas
        UNION
        SELECT time_point FROM effective_weight_deltas
        UNION
        SELECT event_time as time_point FROM cleaned_event_times -- Ensures query bounds and anchor are present
    ),

    -- THINK: reconsider this. I don't believe we need to do anything relative to the root streams change
    -- at this point. The thing is that we only have points in time where any new event is created,
    -- be it a primitive event or a taxonomy change. If we already calculated this before, why we need
    -- to do it again here?
    /*----------------------------------------------------------------------
     * BOUNDARY ADJUSTMENT CALCULATIONS: Correct deltas at root taxonomy changes.
     *
     * Purpose: Calculates the precise change in `delta_ws` and `delta_sw` that
     * occurs exactly at the timestamps identified in `period_change_times`.
     * This is necessary because the simple delta logic might not capture the
     * instantaneous change correctly when the root stream's children/weights change.
     * The formula is: Adjustment = (State_At * Weight_At) - (State_Before * Weight_Before)
     *---------------------------------------------------------------------*/
    -- Calculate the state (value) of each relevant primitive just BEFORE each boundary change time.
    state_before_change AS (
        SELECT
            pct.change_time,
            pe.data_provider,
            pe.stream_id,
            pe.value AS value_before
        FROM period_change_times pct
        -- Correlated subquery to find the latest event *strictly before* the change time
        JOIN primitive_events pe
            ON pe.event_time = (
                SELECT MAX(pe_inner.event_time)
                FROM primitive_events pe_inner
                WHERE pe_inner.data_provider = pe.data_provider
                  AND pe_inner.stream_id = pe.stream_id
                  AND pe_inner.event_time < pct.change_time -- Strictly before
                  AND pe_inner.created_at <= $effective_frozen_at
            )
        -- Ensure this primitive was relevant (had weight) *before* the change.
        -- Check if change_time falls within or just after a weight interval's end.
        WHERE EXISTS (
            SELECT 1 FROM primitive_weights pw
            WHERE pw.data_provider = pe.data_provider
              AND pw.stream_id = pe.stream_id
              AND pct.change_time > pw.group_sequence_start
              AND pct.change_time <= pw.group_sequence_end + 1
        )
    ),

    -- Calculate the state (value) of each relevant primitive AT each boundary change time.
    -- THINK: let's reconsider everything relevant to recalculating states. We should be good already
    -- with delta ws and delta sw. values. I don't think we need these additional change tracks.
    -- or convince me otherwise.
    state_at_change AS (
        SELECT
            pct.change_time,
            pe.data_provider,
            pe.stream_id,
            pe.value AS value_at
        FROM period_change_times pct
        -- Correlated subquery to find the latest event *at or before* the change time
        JOIN primitive_events pe
            ON pe.event_time = (
                SELECT MAX(pe_inner.event_time)
                FROM primitive_events pe_inner
                WHERE pe_inner.data_provider = pe.data_provider
                  AND pe_inner.stream_id = pe.stream_id
                  AND pe_inner.event_time <= pct.change_time -- At or before
                  AND pe_inner.created_at <= $effective_frozen_at
            )
        -- Ensure this primitive is relevant (has weight) *at* the change time.
        WHERE EXISTS (
            SELECT 1 FROM primitive_weights pw
            WHERE pw.data_provider = pe.data_provider
              AND pw.stream_id = pe.stream_id
              AND pct.change_time >= pw.group_sequence_start
              AND pct.change_time <= pw.group_sequence_end
        )
    ),

    -- Determine the total weight applied to each primitive just BEFORE and AT the change point.
    -- This involves checking all weight intervals associated with the primitive.
    bound_wgt_changes_intermediate AS (
        SELECT
            pct.change_time,
            up.data_provider,
            up.stream_id,
            pw.raw_weight,
            pw.group_sequence_start,
            pw.group_sequence_end,
            -- Flag: Is this weight interval relevant *before* the change?
            CASE WHEN pw.raw_weight IS NULL THEN false ELSE (pct.change_time > pw.group_sequence_start AND pct.change_time <= pw.group_sequence_end + 1) END as is_relevant_before,
            -- Flag: Is this weight interval relevant *at* the change?
            CASE WHEN pw.raw_weight IS NULL THEN false ELSE (pct.change_time >= pw.group_sequence_start AND pct.change_time <= pw.group_sequence_end) END as is_relevant_at
        FROM period_change_times pct
        -- Combine every change time with every unique primitive involved in the composition
        JOIN ( SELECT DISTINCT data_provider, stream_id FROM primitive_weights ) up ON 1=1
        -- Join ALL weight intervals for the primitive (time filtering happens in CASE)
        LEFT JOIN primitive_weights pw
            ON pw.data_provider = up.data_provider
           AND pw.stream_id = up.stream_id
    ),
    -- Aggregate the weights based on the relevance flags to get the total weight before and at the change.
    boundary_weight_changes AS (
        SELECT
            change_time,
            data_provider,
            stream_id,
            -- Sum weights relevant before the change (use COALESCE for primitives with no weight)
            COALESCE(SUM(CASE WHEN is_relevant_before THEN raw_weight ELSE 0 END), 0)::numeric(36,18) AS weight_before,
            -- Sum weights relevant at the change
            COALESCE(SUM(CASE WHEN is_relevant_at THEN raw_weight ELSE 0 END), 0)::numeric(36,18) AS weight_at
        FROM bound_wgt_changes_intermediate
        GROUP BY change_time, data_provider, stream_id
    ),


    -- Calculate the adjustment deltas needed at each boundary change time.
    -- Adjustment = Sum_over_primitives[(Value_At * Weight_At) - (Value_Before * Weight_Before)]
    adjustment_deltas AS (
        SELECT
            bwc.change_time AS event_time,
            -- Calculate weighted sum adjustment (delta_ws)
            SUM(
                COALESCE(sac.value_at, 0) * bwc.weight_at -- Use 0 if no state found
                -
                COALESCE(sbc.value_before, 0) * bwc.weight_before
            )::numeric(72,18) AS delta_ws,
            -- Calculate weight sum adjustment (delta_sw)
            SUM(bwc.weight_at - bwc.weight_before)::numeric(36,18) AS delta_sw
        FROM boundary_weight_changes bwc
        -- Join with state before and state at (LEFT JOINs handle cases where a primitive might gain/lose relevance)
        LEFT JOIN state_before_change sbc
            ON bwc.change_time = sbc.change_time
            AND bwc.data_provider = sbc.data_provider
            AND bwc.stream_id = sbc.stream_id
        LEFT JOIN state_at_change sac
            ON bwc.change_time = sac.change_time
            AND bwc.data_provider = sac.data_provider
            AND bwc.stream_id = sac.stream_id
        WHERE
            -- Only include adjustments if there's an actual change in weighted value or weight sum for this primitive
            (COALESCE(sac.value_at, 0) * bwc.weight_at != COALESCE(sbc.value_before, 0) * bwc.weight_before)
            OR
            (bwc.weight_at != bwc.weight_before)
        GROUP BY bwc.change_time -- Aggregate adjustments across all primitives for the change time
    ),


    -- Step 4: Combine the regular value/weight deltas, aggregating by time point.
    combined_deltas AS (
        SELECT
            time_point as event_time,
            SUM(delta_ws)::numeric(72,18) as delta_ws,
            SUM(delta_sw)::numeric(36,18) as delta_sw
        FROM (
            -- Weighted value deltas (from primitive value changes)
            SELECT time_point, delta_ws, 0::numeric(36,18) AS delta_sw
            FROM weighted_value_deltas wvd
            JOIN all_combined_times act ON wvd.event_time = act.time_point

            UNION ALL

            -- Effective weight sum deltas (from weight changes)
            SELECT time_point, 0::numeric(72,18) AS delta_ws, effective_delta_sw AS delta_sw
            FROM effective_weight_deltas ewd
            JOIN all_combined_times act ON ewd.time_point = act.time_point
        ) combined
        GROUP BY event_time
    ),

    /*----------------------------------------------------------------------
     * FINAL_DELTAS CTE: Merges regular deltas with boundary adjustments.
     *
     * Purpose: Creates the definitive set of deltas (`delta_ws`, `delta_sw`)
     * for each time point. At boundary change times, the calculated
     * `adjustment_deltas` take precedence over any regular `combined_deltas`.
     *---------------------------------------------------------------------*/
    final_deltas AS (
        -- Select regular deltas for time points that are NOT boundary changes
        SELECT cd.event_time, cd.delta_ws, cd.delta_sw
        FROM combined_deltas cd
        WHERE NOT EXISTS (SELECT 1 FROM period_change_times pct WHERE pct.change_time = cd.event_time)

        UNION ALL

        -- Select the calculated adjustment deltas ONLY for the boundary change times
        SELECT ad.event_time, ad.delta_ws, ad.delta_sw
        FROM adjustment_deltas ad
        -- No need for WHERE clause here, adjustment_deltas only contains boundary times
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
        CROSS JOIN anchor_time_calc atc -- Make anchor_time available
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
};



-- NOTE: This function finds the single latest event ignoring overshadow,
-- then uses get_record_composed() at that time to apply overshadow logic.
-- It's simplified, may miss certain edge cases, but usually sufficient.

CREATE OR REPLACE ACTION get_last_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,       -- Upper bound for event_time
    $frozen_at INT8     -- Only consider events created on or before this
) PRIVATE VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    /*
     * Step 1: Basic setup
     */
    IF !is_allowed_to_read_all($data_provider, $stream_id, @caller, NULL, $before) {
        ERROR('Not allowed to read stream');
    }

    -- Check compose permissions
    if !is_allowed_to_compose_all($data_provider, $stream_id, NULL, $before) {
        ERROR('Not allowed to compose stream');
    }

    $max_int8 INT8 := 9223372036854775000;    -- "Infinity" sentinel
    $effective_before INT8 := COALESCE($before, $max_int8);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    $latest_event_time INT8;

    /*
     * Step 2: Recursively gather all children (ignoring overshadow),
     *         then identify primitive leaves.
     */
    for $row in WITH RECURSIVE all_taxonomies AS (
      /* 2a) Direct children of ($data_provider, $stream_id) */
      SELECT
        t.data_provider,
        t.stream_id,
        t.child_data_provider,
        t.child_stream_id
      FROM taxonomies t
      WHERE t.data_provider = $data_provider
        AND t.stream_id     = $stream_id

      UNION

      /* 2b) For each discovered child, gather its own children */
      SELECT
        at.child_data_provider AS data_provider,
        at.child_stream_id     AS stream_id,
        t.child_data_provider,
        t.child_stream_id
      FROM all_taxonomies at
      JOIN taxonomies t
        ON t.data_provider = at.child_data_provider
       AND t.stream_id     = at.child_stream_id
    ),
    primitive_leaves AS (
      /* Keep only references pointing to primitive streams */
      SELECT DISTINCT
        at.child_data_provider AS data_provider,
        at.child_stream_id     AS stream_id
      FROM all_taxonomies at
      JOIN streams s
        ON s.data_provider = at.child_data_provider
       AND s.stream_id     = at.child_stream_id
       AND s.stream_type   = 'primitive'
    ),
    /*
     * Step 3: In each primitive, pick the single latest event_time <= effective_before.
     *         ROW_NUMBER=1 => that "latest" champion. Tie-break by created_at DESC.
     */
    latest_events AS (
      SELECT
        pl.data_provider,
        pl.stream_id,
        pe.event_time,
        pe.value,
        pe.created_at,
        ROW_NUMBER() OVER (
          PARTITION BY pl.data_provider, pl.stream_id
          ORDER BY pe.event_time DESC, pe.created_at DESC
        ) AS rn
      FROM primitive_leaves pl
      JOIN primitive_events pe
        ON pe.data_provider = pl.data_provider
       AND pe.stream_id     = pl.stream_id
      WHERE pe.event_time   <= $effective_before
        AND pe.created_at   <= $effective_frozen_at
    ),
    latest_values AS (
      /* Step 4: Filter to rn=1 => the single latest event per (dp, sid) */
      SELECT
        data_provider,
        stream_id,
        event_time,
        value
      FROM latest_events
      WHERE rn = 1
    ),
    global_max AS (
      /* Step 5: Find the maximum event_time among all leaves */
      SELECT MAX(event_time) AS latest_time
      FROM latest_values
    )
    /* Step 6: Return the row(s) matching that global latest_time (pick first) */
    SELECT
      lv.event_time,
      lv.value::NUMERIC(36,18)
    FROM latest_values lv
    JOIN global_max gm
      ON lv.event_time = gm.latest_time
    {
        $latest_event_time := $row.event_time;
        break;  -- break out after storing
    }

    /*
     * Step 7: If we found latest_event_time, call get_record_composed() at
     *          [latest_event_time, latest_event_time] for overshadow logic.
     */
    IF $latest_event_time IS DISTINCT FROM NULL {
        for $row in get_record_composed($data_provider, $stream_id, $latest_event_time, $latest_event_time, $frozen_at) {
            return next $row.event_time, $row.value;
            break;
        }
    }

    /* If no events were found, no rows are returned */
};

-- NOTE: This function finds the single earliest event ignoring overshadow,
-- then uses get_record_composed() at that time. Simplified, but effective
-- for most common use cases; may have edge cases and is not highly optimized.

CREATE OR REPLACE ACTION get_first_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,       -- Lower bound for event_time
    $frozen_at INT8    -- Only consider events created on or before this
) PRIVATE VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
) {
    /*
     * Step 1: Basic setup
     */
    IF !is_allowed_to_read_all($data_provider, $stream_id, @caller, $after, NULL) {
        ERROR('Not allowed to read stream');
    }

    $max_int8 INT8 := 9223372036854775000;   -- "Infinity" sentinel
    $effective_after INT8 := COALESCE($after, 0);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

    $earliest_event_time INT8;

    /*
     * Step 2: Recursively gather all children (ignoring overshadow),
     *         then identify primitive leaves.
     */
    for $row in WITH RECURSIVE all_taxonomies AS (
      /* 2a) Direct children of ($data_provider, $stream_id) */
      SELECT
        t.data_provider,
        t.stream_id,
        t.child_data_provider,
        t.child_stream_id
      FROM taxonomies t
      WHERE t.data_provider = $data_provider
        AND t.stream_id     = $stream_id

      UNION

      /* 2b) For each discovered child, gather its own children */
      SELECT
        at.child_data_provider AS data_provider,
        at.child_stream_id     AS stream_id,
        t.child_data_provider,
        t.child_stream_id
      FROM all_taxonomies at
      JOIN taxonomies t
        ON t.data_provider = at.child_data_provider
       AND t.stream_id     = at.child_stream_id
    ),
    primitive_leaves AS (
      /* Keep only references pointing to primitive streams */
      SELECT DISTINCT
        at.child_data_provider AS data_provider,
        at.child_stream_id     AS stream_id
      FROM all_taxonomies at
      JOIN streams s
        ON s.data_provider = at.child_data_provider
       AND s.stream_id     = at.child_stream_id
       AND s.stream_type   = 'primitive'
    ),
    /*
     * Step 3: In each primitive, pick the single earliest event_time >= effective_after.
     *         ROW_NUMBER=1 => that "earliest" champion. Tie-break by created_at DESC.
     */
    earliest_events AS (
      SELECT
        pl.data_provider,
        pl.stream_id,
        pe.event_time,
        pe.value,
        pe.created_at,
        ROW_NUMBER() OVER (
          PARTITION BY pl.data_provider, pl.stream_id
          ORDER BY pe.event_time ASC, pe.created_at DESC
        ) AS rn
      FROM primitive_leaves pl
      JOIN primitive_events pe
        ON pe.data_provider = pl.data_provider
       AND pe.stream_id     = pl.stream_id
      WHERE pe.event_time   >= $effective_after
        AND pe.created_at   <= $effective_frozen_at
    ),
    earliest_values AS (
      /* Step 4: Filter to rn=1 => the single earliest event per (dp, sid) */
      SELECT
        data_provider,
        stream_id,
        event_time,
        value
      FROM earliest_events
      WHERE rn = 1
    ),
    global_min AS (
      /* Step 5: Find the minimum event_time among all leaves */
      SELECT MIN(event_time) AS earliest_time
      FROM earliest_values
    )
    /* Step 6: Return the row(s) matching that global earliest_time (pick first) */
    SELECT
      ev.event_time,
      ev.value::NUMERIC(36,18)
    FROM earliest_values ev
    JOIN global_min gm
      ON ev.event_time = gm.earliest_time
    {
        $earliest_event_time := $row.event_time;
        break;  -- break out after storing
    }

    /*
     * Step 7: If we have earliest_event_time, call get_record_composed() at
     *          [earliest_event_time, earliest_event_time].
     */
    IF $earliest_event_time IS DISTINCT FROM NULL {
        for $row in get_record_composed($data_provider, $stream_id, $earliest_event_time, $earliest_event_time, $frozen_at) {
            return next $row.event_time, $row.value;
            break;
        }
    }
};

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
    /*-----------------------------------------------------------
     * 1) Basic Setup & Permissions
     *----------------------------------------------------------*/
    IF !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }
    IF !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
        ERROR('Not allowed to compose stream');
    }

    -- We'll handle "infinite" cutoffs
    $max_int8 := 9223372036854775000;
    $effective_from := COALESCE($from, 0);
    $effective_to   := COALESCE($to,   $max_int8);
    $effective_frozen_at := COALESCE($frozen_at, $max_int8);

    -- try to get the base_time from the caller, or from metadata
    $effective_base_time INT8;
    if $base_time is not null {
        $effective_base_time := $base_time;
    } else {
        -- try to get from metadata
        $effective_base_time := get_latest_metadata_int($data_provider, $stream_id, 'default_base_time');
    }
    -- coalesce to 0 if still null, as it should be the first event ever
    $effective_base_time := COALESCE($effective_base_time, 0);

    -- Validate time range to avoid nonsensical queries
    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('Invalid time range: from (%s) > to (%s)', $from, $to));
    }

    NOTICE(format('effective_base_time: %s, effective_from: %s, effective_to: %s, effective_frozen_at: %s', $effective_base_time, $effective_from, $effective_to, $effective_frozen_at));

    /*-----------------------------------------------------------
     * 2) Recursively gather all dependent taxonomies,
     *    focusing on leaf primitives. (Same approach as
     *    get_record_composed, but we'll reuse the resulting
     *    "primitive_weights" CTE.)
     *----------------------------------------------------------*/
    RETURN WITH RECURSIVE

    /*----------------------------------------------------------------------
     * HIERARCHY: Build a tree of dependent child streams via taxonomies.
     * We do it in two steps:
     *   (1) Base Case for (data_provider, stream_id)
     *   (2) Recursive Step for each discovered child
     *
     * We'll attach an effective [start, end] interval to each row.
     * Overlapping or overshadowed rows are handled by ignoring older
     * group_sequences at the same start_time and by partitioning LEAD
     * over (dp, sid) to get the next distinct start_time.
     *---------------------------------------------------------------------*/
    hierarchy AS (
      /*------------------------------------------------------------------
       * (1) Base Case (Parent-Level)
       * We gather taxonomies for the PARENT (data_provider, stream_id)
       * in the requested [anchor, $effective_to] range.
       *
       * Partition by (data_provider, stream_id) in LEAD so that
       * any new start_time overshadowing the old version
       * effectively closes the old row's interval.
       *------------------------------------------------------------------*/
      SELECT
          base.data_provider           AS parent_data_provider,
          base.stream_id               AS parent_stream_id,
          base.child_data_provider,
          base.child_stream_id,
          base.weight                  AS raw_weight,
          base.start_time             AS group_sequence_start,
          COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end,
          base.max_group_sequence -- Added for debugging
      FROM (
          SELECT
              t.data_provider,
              t.stream_id,
              t.child_data_provider,
              t.child_stream_id,
              t.start_time,
              t.group_sequence,
              t.weight,
              -- overshadow older group_sequence rows at the same start_time
              MAX(t.group_sequence) OVER (
                  PARTITION BY t.data_provider, t.stream_id, t.start_time
              ) AS max_group_sequence
          FROM taxonomies t
          WHERE t.data_provider = $data_provider
            AND t.stream_id     = $stream_id
            AND t.disabled_at   IS NULL
            AND t.start_time <= $effective_to
            AND t.start_time >= COALESCE(
                (
                  -- Find the most recent taxonomy at or before $effective_from
                  SELECT t2.start_time
                  FROM taxonomies t2
                  WHERE t2.data_provider = t.data_provider
                    AND t2.stream_id     = t.stream_id
                    AND t2.disabled_at   IS NULL
                    AND t2.start_time   <= $effective_from
                  ORDER BY t2.start_time DESC, t2.group_sequence DESC
                  LIMIT 1
                ), 0
            )
      ) base
      JOIN (
          -- Create ordered_times to get the next distinct start_time
          SELECT
              dt.data_provider,
              dt.stream_id,
              dt.start_time,
              LEAD(dt.start_time) OVER (
                  PARTITION BY dt.data_provider, dt.stream_id
                  ORDER BY dt.start_time
              ) AS next_start
          FROM (
              -- Distinct start_times for each (dp, sid)
              SELECT DISTINCT
                  t.data_provider,
                  t.stream_id,
                  t.start_time
              FROM taxonomies t
              WHERE t.data_provider = $data_provider
                AND t.stream_id     = $stream_id
                AND t.disabled_at   IS NULL
                AND t.start_time   <= $effective_to
                AND t.start_time   >= COALESCE(
                    (
                      SELECT t2.start_time
                      FROM taxonomies t2
                      WHERE t2.data_provider = t.data_provider
                        AND t2.stream_id     = t.stream_id
                        AND t2.disabled_at   IS NULL
                        AND t2.start_time   <= $effective_from
                      ORDER BY t2.start_time DESC, t2.group_sequence DESC
                      LIMIT 1
                    ), 0
                )
          ) dt
      ) ot ON base.data_provider = ot.data_provider
       AND base.stream_id     = ot.stream_id
       AND base.start_time    = ot.start_time
      -- Include only the latest group_sequence row for each start_time
      WHERE base.group_sequence = base.max_group_sequence

      /*--------------------------------------------------------------------
       * (2) Recursive Step (Child-Level)
       * For each child discovered, look up that child's own taxonomies
       * and overshadow older versions for that child.
       * Partition by (data_provider, stream_id, child_dp, child_sid)
       * so multiple changes overshadow older ones.
       *--------------------------------------------------------------------*/
      UNION ALL
      SELECT
          parent.parent_data_provider,
          parent.parent_stream_id,
          child.child_data_provider,
          child.child_stream_id,
          (parent.raw_weight * child.weight)::NUMERIC(36,18) AS raw_weight,
          GREATEST(parent.group_sequence_start, child.start_time)   AS group_sequence_start,
          LEAST(parent.group_sequence_end,   child.group_sequence_end) AS group_sequence_end
      FROM
          hierarchy parent
      -- Join parent with potential child taxonomies
      JOIN taxonomies child
        ON child.data_provider = parent.child_data_provider
       AND child.stream_id     = parent.child_stream_id
      WHERE
          child.start_time         <= parent.group_sequence_end
          AND child.group_sequence_end >= parent.group_sequence_start
          AND child.group_sequence = child.max_group_sequence
    ),

    /*----------------------------------------------------------------------
     * 3) Identify LEAF nodes (primitive streams) and their weights/intervals.
     *--------------------------------------------------------------------*/
    primitive_weights AS (
      SELECT
          h.child_data_provider AS data_provider,
          h.child_stream_id     AS stream_id,
          h.raw_weight,
          h.group_sequence_start,
          h.group_sequence_end
      FROM hierarchy h
      WHERE EXISTS (
          SELECT 1 FROM streams s
          WHERE s.data_provider = h.child_data_provider
            AND s.stream_id     = h.child_stream_id
            AND s.stream_type   = 'primitive'
      )
    ),

    /*----------------------------------------------------------------------
     * 4) Consolidate intervals. We may have multiple or overlapping
     * [start, end] intervals for each primitive stream. We'll merge them:
     *   Step 1: Order by start_time
     *   Step 2: Detect where intervals have a gap
     *   Step 3: Assign group IDs for contiguous intervals
     *   Step 4: Merge intervals in each group
     *---------------------------------------------------------------------*/
    ordered_intervals AS (
      SELECT
          data_provider,
          stream_id,
          group_sequence_start,
          group_sequence_end,
          ROW_NUMBER() OVER (
              PARTITION BY data_provider, stream_id
              ORDER BY group_sequence_start
          ) AS rn
      FROM primitive_weights
    ),

    group_boundaries AS (
      SELECT
          data_provider,
          stream_id,
          group_sequence_start,
          group_sequence_end,
          rn,
          CASE
            WHEN rn = 1 THEN 1  -- first interval is a new group
            WHEN group_sequence_start > LAG(group_sequence_end) OVER (
                PARTITION BY data_provider, stream_id
                ORDER BY group_sequence_start, group_sequence_end DESC
            ) + 1 THEN 1        -- there's a gap, start a new group
            ELSE 0              -- same group as previous
          END AS is_new_group
      FROM ordered_intervals
    ),

    groups AS (
      SELECT
          data_provider,
          stream_id,
          group_sequence_start,
          group_sequence_end,
          SUM(is_new_group) OVER (
              PARTITION BY data_provider, stream_id
              ORDER BY group_sequence_start
          ) AS group_id
      FROM group_boundaries
    ),

    stream_intervals AS (
      SELECT
          data_provider,
          stream_id,
          MIN(group_sequence_start) AS group_sequence_start,  -- earliest start in group
          MAX(group_sequence_end)   AS group_sequence_end     -- latest end in group
      FROM groups
      GROUP BY data_provider, stream_id, group_id
    ),

    primitive_streams AS (
      SELECT DISTINCT
          data_provider,
          stream_id
      FROM stream_intervals
    ),

    /*----------------------------------------------------------------------
     * 5) Gather relevant events from each consolidated interval.
     * We only pull events that fall within each [start, end] and within
     * the user's requested range, plus a potential "anchor" event to
     * capture a baseline prior to $effective_from.
     *  
     * We then keep only the LATEST record (via row_number=1) if multiple
     * events exist at the same event_time with different creation times.
     *---------------------------------------------------------------------*/
    relevant_events AS (
      SELECT
          pe.data_provider,
          pe.stream_id,
          pe.event_time,
          pe.value,
          pe.created_at,
          ROW_NUMBER() OVER (
              PARTITION BY pe.data_provider, pe.stream_id, pe.event_time
              ORDER BY pe.created_at DESC
          ) as rn
      FROM primitive_events pe
      JOIN stream_intervals si
         ON pe.data_provider = si.data_provider
        AND pe.stream_id     = si.stream_id
      WHERE pe.created_at   <= $effective_frozen_at
        AND pe.event_time   <= LEAST(si.group_sequence_end, $effective_to)
        -- Anchor: include the latest event at/just before the interval start
        AND (
            pe.event_time >= GREATEST(si.group_sequence_start, $effective_from)
            OR pe.event_time = (
                SELECT MAX(pe2.event_time)
                FROM primitive_events pe2
                WHERE pe2.data_provider = pe.data_provider
            AND pe2.stream_id     = pe.stream_id
                AND pe2.event_time   <= GREATEST(si.group_sequence_start, $effective_from)
            )
        )
    ),

    requested_primitive_records AS (
      SELECT
          data_provider,
          stream_id,
          event_time,
          value
      FROM relevant_events
      WHERE rn = 1  -- pick the most recent creation for each event_time
    ),

    selected_base_times AS (
      SELECT
        ps.data_provider,
        ps.stream_id,
        COALESCE(
          -- 1) The largest event_time <= base_time
          (SELECT MAX(pe.event_time)
          FROM primitive_events pe
          WHERE pe.data_provider = ps.data_provider
            AND pe.stream_id     = ps.stream_id
            AND pe.created_at   <= $effective_frozen_at
            AND pe.event_time   <= $effective_base_time
          ),
          -- 2) If none found, the smallest event_time > base_time
          (SELECT MIN(pe.event_time)
          FROM primitive_events pe
          WHERE pe.data_provider = ps.data_provider
            AND pe.stream_id     = ps.stream_id
            AND pe.created_at   <= $effective_frozen_at
            AND pe.event_time   >  $effective_base_time
          )
        ) AS chosen_time
      FROM primitive_streams ps
    ),

    raw_base_events AS (
      SELECT
        pe.data_provider,
        pe.stream_id,
        pe.event_time,
        pe.value,
        pe.created_at,
        ROW_NUMBER() OVER(
          PARTITION BY pe.data_provider, pe.stream_id, pe.event_time
          ORDER BY pe.created_at DESC
        ) AS overshadow_rank
      FROM primitive_events pe
      JOIN selected_base_times st
        ON st.data_provider = pe.data_provider
      AND st.stream_id     = pe.stream_id
      AND st.chosen_time   = pe.event_time      -- only rows for that chosen_time
      WHERE pe.created_at <= $effective_frozen_at
    ),
    base_values AS (
      SELECT
        rbe.data_provider,
        rbe.stream_id,
        rbe.value AS base_value
      FROM raw_base_events rbe
      WHERE rbe.overshadow_rank = 1  -- pick the top version if duplicates
    ),


    -- primitive streams without a base value
    -- TODO: check if we should really error out if there are
    --       any primitive streams without a base value
    -- primitive_streams_without_base_value AS (
    --   SELECT
    --       data_provider,
    --       stream_id
    --   FROM primitive_streams
    --   WHERE data_provider = $data_provider
    --     AND stream_id = $stream_id
    --     AND NOT EXISTS (
    --       SELECT 1 FROM primitive_stream_base_values
    --       WHERE data_provider = ps.data_provider
    --       AND stream_id = ps.stream_id
    -- ),

    /*----------------------------------------------------------------------
     * 6) Final Weighted Aggregation
     * We need every relevant time point (both event times AND taxonomy
     * change points) to compute a proper time series. For each time, we
     * calculate the weighted value across all primitive streams.
     *---------------------------------------------------------------------*/

    -- Collect all event times + taxonomy transitions
    all_event_times AS (
      SELECT DISTINCT event_time FROM requested_primitive_records
      UNION
      SELECT DISTINCT group_sequence_start
      FROM primitive_weights
    ),

    -- Filter to the requested time range, plus one "anchor" point
    cleaned_event_times AS (
      SELECT DISTINCT event_time
      FROM all_event_times
      WHERE event_time > $effective_from

      UNION

      -- Anchor at or before from
      SELECT event_time FROM (
          SELECT event_time
          FROM all_event_times
          WHERE event_time <= $effective_from
          ORDER BY event_time DESC
          LIMIT 1
      ) anchor_event
    ),

    -- For each (time × stream), find the "current" (most recent) event_time
    latest_event_times AS (
      SELECT
          re.event_time,
          es.data_provider,
          es.stream_id,
          MAX(le.event_time) AS latest_event_time
      FROM cleaned_event_times re
      -- Evaluate every stream at every time point
      JOIN (
          SELECT DISTINCT data_provider, stream_id
          FROM primitive_weights
      ) es ON true -- cross join alternative
      LEFT JOIN requested_primitive_records le
         ON le.data_provider = es.data_provider
        AND le.stream_id     = es.stream_id
        AND le.event_time   <= re.event_time
      GROUP BY re.event_time, es.data_provider, es.stream_id
    ),

    -- Retrieve actual values for each (time × stream)
    stream_values AS (
      SELECT
          let.event_time,
          let.data_provider,
          let.stream_id,
          (le.value * 100::NUMERIC(36,18) / psbv.base_value) AS value
      FROM latest_event_times let
      LEFT JOIN requested_primitive_records le
        ON le.data_provider  = let.data_provider
       AND le.stream_id      = let.stream_id
       AND le.event_time     = let.latest_event_time
      LEFT JOIN base_values psbv
        ON psbv.data_provider = let.data_provider
       AND psbv.stream_id     = let.stream_id
    ),

    -- Multiply each stream's value by its taxonomy weight
    weighted_values AS (
      SELECT
          sv.event_time,
          sv.value * pw.raw_weight AS weighted_value,
          pw.raw_weight
      FROM stream_values sv
      JOIN primitive_weights pw
        ON sv.data_provider = pw.data_provider
       AND sv.stream_id     = pw.stream_id
       AND sv.event_time   BETWEEN pw.group_sequence_start AND pw.group_sequence_end
      WHERE sv.value IS NOT NULL
    ),

    -- Finally, compute weighted average for each time point
    aggregated AS (
      SELECT
          event_time,
          CASE WHEN SUM(raw_weight)::NUMERIC(36,18) = 0::NUMERIC(36,18)
               THEN 0::NUMERIC(36,18)
               ELSE SUM(weighted_value)::NUMERIC(36,18) / SUM(raw_weight)::NUMERIC(36,18)
          END AS value
      FROM weighted_values
      GROUP BY event_time
    )

    SELECT
        event_time,
        value::NUMERIC(36,18)
    FROM aggregated
    ORDER BY event_time;
};
