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

    -- Define boundary defaults
    $max_int8 := 9223372036854775000;          -- "Infinity" sentinel for INT8
    $effective_from := COALESCE($from, 0);      -- Lower bound, default 0
    $effective_to := COALESCE($to, $max_int8);  -- Upper bound, default "infinity"
    $effective_frozen_at := COALESCE($frozen_at, $max_int8);

    -- Validate time range to avoid nonsensical queries
    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('Invalid time range: from (%s) > to (%s)', $from, $to));
    }

    -- -- Check permissions; raises error if unauthorized
    -- IF !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
    --     ERROR('Not allowed to read stream');
    -- }

    -- -- Check compose permissions
    -- if !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
    --     ERROR('Not allowed to compose stream');
    -- }

    RETURN WITH RECURSIVE
    /*----------------------------------------------------------------------
     * HIERARCHY: Build the tree of dependent streams.
     * Uses a self-join (anti-join) pattern to select the rows with the
     * maximum group_sequence for each (dp, sid, start_time) without relying
     * on potentially buggy MAX() OVER(PARTITION BY...). It joins t1 to t2
     * where t2 has a higher sequence, and keeps t1 only if no such t2 exists.
     *---------------------------------------------------------------------*/
    hierarchy AS (
      -- Base Case (Parent-Level) - Using self-join anti-pattern
      SELECT
          t1.data_provider AS parent_data_provider,
          t1.stream_id AS parent_stream_id,
          t1.child_data_provider,
          t1.child_stream_id,
          t1.weight AS raw_weight,
          t1.start_time AS group_sequence_start,
          COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
      FROM
          taxonomies t1
      -- Anti-join: Find if a row t2 exists with a higher group_sequence for the same key
      LEFT JOIN taxonomies t2
        ON t1.data_provider = t2.data_provider
       AND t1.stream_id = t2.stream_id
       AND t1.start_time = t2.start_time
       AND t1.group_sequence < t2.group_sequence -- t2 must have a strictly higher sequence
       AND t2.disabled_at IS NULL -- Don't consider disabled rows as 'higher'
      -- Join to get the next start time for interval end calculation
      JOIN (
          SELECT
              dt.data_provider, dt.stream_id, dt.start_time,
              LEAD(dt.start_time) OVER (PARTITION BY dt.data_provider, dt.stream_id ORDER BY dt.start_time) AS next_start
          FROM ( SELECT DISTINCT t_ot.data_provider, t_ot.stream_id, t_ot.start_time FROM taxonomies t_ot
                 WHERE t_ot.data_provider = $data_provider AND t_ot.stream_id = $stream_id AND t_ot.disabled_at IS NULL AND t_ot.start_time <= $effective_to
                 AND t_ot.start_time >= COALESCE((SELECT t2_anchor.start_time FROM taxonomies t2_anchor WHERE t2_anchor.data_provider=t_ot.data_provider AND t2_anchor.stream_id=t_ot.stream_id AND t2_anchor.disabled_at IS NULL AND t2_anchor.start_time<=$effective_from ORDER BY t2_anchor.start_time DESC, t2_anchor.group_sequence DESC LIMIT 1),0)
               ) dt
      ) ot
        ON t1.data_provider = ot.data_provider
       AND t1.stream_id     = ot.stream_id
       AND t1.start_time    = ot.start_time
      WHERE
          t1.data_provider = $data_provider -- Filter for the specific parent
      AND t1.stream_id     = $stream_id
      AND t1.disabled_at   IS NULL
      AND t2.group_sequence IS NULL -- Keep t1 only if NO row t2 with a higher sequence was found
      -- Apply time range/anchor logic to t1
      AND t1.start_time <= $effective_to
      AND t1.start_time >= COALESCE(
            (SELECT t_anchor_base.start_time
             FROM taxonomies t_anchor_base
             WHERE t_anchor_base.data_provider = t1.data_provider -- Use t1 alias here
               AND t_anchor_base.stream_id     = t1.stream_id     -- Use t1 alias here
               AND t_anchor_base.disabled_at   IS NULL
               AND t_anchor_base.start_time   <= $effective_from
             ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC
             LIMIT 1
            ), 0
          )

      UNION ALL

      -- Recursive Step (Child-Level) - Using self-join anti-pattern
      SELECT
          parent.parent_data_provider,
          parent.parent_stream_id,
          t1_child.child_data_provider,
          t1_child.child_stream_id,
          (parent.raw_weight * t1_child.weight)::NUMERIC(36,18) AS raw_weight,
          GREATEST(parent.group_sequence_start, t1_child.start_time) AS group_sequence_start,
          LEAST(parent.group_sequence_end, (COALESCE(ot_child.next_start, $max_int8) - 1)) AS group_sequence_end
      FROM
          hierarchy parent
      -- Join parent with potential child rows
      JOIN taxonomies t1_child
        ON t1_child.data_provider = parent.child_data_provider
       AND t1_child.stream_id     = parent.child_stream_id
      -- Anti-join: Find if a row t2_child exists with a higher sequence for this child
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
          FROM ( SELECT DISTINCT t_otc.data_provider, t_otc.stream_id, t_otc.start_time FROM taxonomies t_otc
                 WHERE t_otc.disabled_at IS NULL AND t_otc.start_time <= $effective_to AND t_otc.start_time >= COALESCE((SELECT MIN(t2_min.start_time) FROM taxonomies t2_min WHERE t2_min.disabled_at IS NULL AND t2_min.start_time <= $effective_from), 0)
               ) dt
      ) ot_child
        ON t1_child.data_provider = ot_child.data_provider
       AND t1_child.stream_id     = ot_child.stream_id
       AND t1_child.start_time    = ot_child.start_time
      WHERE
          t1_child.disabled_at IS NULL
      AND t2_child.group_sequence IS NULL -- Keep t1_child only if no higher sequence row found
      -- Apply interval overlap check between parent and child (t1_child)
      AND t1_child.start_time <= parent.group_sequence_end
      AND (COALESCE(ot_child.next_start, $max_int8) - 1) >= parent.group_sequence_start
      -- Apply child time range/anchor filters to t1_child
      AND t1_child.start_time <= $effective_to
      AND t1_child.start_time >= COALESCE(
            (SELECT t_anchor_child.start_time
             FROM taxonomies t_anchor_child
             WHERE t_anchor_child.data_provider = t1_child.data_provider -- Use t1_child alias
               AND t_anchor_child.stream_id     = t1_child.stream_id     -- Use t1_child alias
               AND t_anchor_child.disabled_at   IS NULL
               AND t_anchor_child.start_time   <= $effective_from
             ORDER BY t_anchor_child.start_time DESC, t_anchor_child.group_sequence DESC
             LIMIT 1
            ), 0
          )
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
     * Identify top-level taxonomy periods defined by start_time for the root stream.
     * These define the boundaries where the entire active configuration changes.
     *---------------------------------------------------------------------*/
    top_level_periods AS (
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
            FROM ( -- Distinct start times for the root stream within relevant range
                SELECT DISTINCT t_ot.start_time
                FROM taxonomies t_ot
                WHERE t_ot.data_provider = $data_provider
                  AND t_ot.stream_id = $stream_id
                  AND t_ot.disabled_at IS NULL
                  AND t_ot.start_time <= $effective_to
                  AND t_ot.start_time >= COALESCE(
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
            t1.data_provider = $data_provider
        AND t1.stream_id = $stream_id
        AND t1.disabled_at IS NULL
        AND t2.group_sequence IS NULL -- Keep only rows with max group_sequence for their start_time
        AND t1.start_time <= $effective_to -- Apply range filter
        AND t1.start_time >= COALESCE(
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

    /*----------------------------------------------------------------------
     * Identify the exact times when top-level taxonomy periods change.
     * Adjustments are needed at these points (excluding the initial state).
     *---------------------------------------------------------------------*/
    period_change_times AS (
        SELECT period_start AS change_time
        FROM top_level_periods
        -- Exclude the very first period start time within the effective range,
        -- as no "change" happens there, it's the initial state.
        WHERE period_start > (
                SELECT COALESCE(MIN(period_start), -1) -- Use -1 if no periods found before/at from
                FROM top_level_periods
                WHERE period_start <= $effective_from
              )
          AND period_start > $effective_from -- Only consider changes *after* the requested start
          AND period_start <= $effective_to -- Only consider changes *within* the requested range
    ),

    /*----------------------------------------------------------------------
     * Cleaned Event Times: Determine all necessary time points for output.
     * Includes event times and taxonomy starts within the range, plus an anchor.
     *---------------------------------------------------------------------*/
    cleaned_event_times AS (
        SELECT DISTINCT event_time
        FROM (
            -- Event times strictly within the requested range
            SELECT pe.event_time
            FROM primitive_events pe
            JOIN primitive_weights pw -- Only consider events from relevant primitives
              ON pe.data_provider = pw.data_provider
             AND pe.stream_id = pw.stream_id
             AND pe.event_time >= pw.group_sequence_start -- Check event falls within *a* valid weight interval
             AND pe.event_time <= pw.group_sequence_end
            WHERE pe.event_time > $effective_from
              AND pe.event_time <= $effective_to
              AND pe.created_at <= $effective_frozen_at -- Apply frozen_at early

            UNION

            -- Taxonomy start times strictly within the requested range
            SELECT pw.group_sequence_start AS event_time
            FROM primitive_weights pw
            WHERE pw.group_sequence_start > $effective_from
              AND pw.group_sequence_start <= $effective_to

            UNION

            -- Ensure the start of the requested range is included
            SELECT $effective_from AS event_time

        ) all_times_in_range

        UNION

        -- The latest relevant time point AT or BEFORE effective_from (anchor)
        SELECT event_time FROM (
            SELECT event_time
            FROM (
                -- Latest primitive event at or before start
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

                -- Latest taxonomy start at or before start
                SELECT pw.group_sequence_start AS event_time
                FROM primitive_weights pw
                WHERE pw.group_sequence_start <= $effective_from

            ) all_times_before
            ORDER BY event_time DESC
            LIMIT 1
        ) as anchor_event
    ),


    /*----------------------------------------------------------------------
     * DELTA METHOD STEPS
     *---------------------------------------------------------------------*/

    -- Step 1a: Find the latest state AT OR BEFORE $effective_from
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
            JOIN primitive_weights pw_check -- Ensure we only consider primitives relevant to the composed stream
              ON pe_inner.data_provider = pw_check.data_provider
             AND pe_inner.stream_id = pw_check.stream_id
             AND pe_inner.event_time >= pw_check.group_sequence_start -- Check event falls within *a* valid weight interval for this state snapshot
             AND pe_inner.event_time <= pw_check.group_sequence_end
            WHERE pe_inner.event_time <= $effective_from -- Event at or before the start
              AND pe_inner.created_at <= $effective_frozen_at
        ) pe
        WHERE pe.rn = 1
    ),

    -- Step 1b: Find distinct events strictly WITHIN the interval ($from < time <= $to)
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
                    ORDER BY pe_inner.created_at DESC -- Tie-break duplicate event_times by created_at
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

    -- Step 1c: Combine initial states and interval events
    -- Use the actual event_time from the initial state, not $effective_from
    all_primitive_points AS (
        SELECT
            ips.data_provider,
            ips.stream_id,
            ips.event_time, -- Use the original event_time from the initial state
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

    -- Step 1d: Calculate value deltas per primitive stream, filtering zeros
    primitive_event_changes AS (
        SELECT * FROM (
            SELECT
                data_provider, stream_id, event_time, value,
                COALESCE(
                    value - LAG(value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time),
                    value
                )::numeric(36,18) AS delta_value
            FROM all_primitive_points
        ) calc
        WHERE delta_value != 0::numeric(36,18) -- Filter out non-changing events
    ),

    -- *** OPTIMIZED WEIGHT CALCULATION STEPS ***
    -- Step 2a.1: Generate weight change events (+weight at start, -weight at end+1)
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
            group_sequence_end + 1 AS change_time,
            -raw_weight            AS weight_delta
        FROM primitive_weights
        WHERE raw_weight != 0::numeric(36,18)
    ),

    -- Step 2a.2: Calculate the running total weight active at each change point
    weight_timeline AS (
        SELECT
            data_provider,
            stream_id,
            change_time,
            (SUM(weight_delta) OVER (PARTITION BY data_provider, stream_id ORDER BY change_time ASC))::numeric(36,18) AS total_weight_at_time
        FROM weight_changes
    ),

    -- Step 2a.3: Define non-overlapping segments with constant total weight
    primitive_weight_segments AS (
        SELECT
            data_provider,
            stream_id,
            change_time AS segment_start,
            LEAD(change_time, 1, $max_int8) OVER (PARTITION BY data_provider, stream_id ORDER BY change_time ASC) AS segment_end,
            total_weight_at_time
        FROM weight_timeline
    ),

    -- Step 2b: Join value delta events with the weight segments to find the applicable weight
    weighted_event_deltas AS (
        SELECT
            pec.event_time,
            pec.delta_value,
            COALESCE(pws.total_weight_at_time, 0::numeric(36,18))::numeric(36, 18) AS active_weight_sum
        FROM
            primitive_event_changes pec
        LEFT JOIN
            primitive_weight_segments pws ON pec.data_provider = pws.data_provider
                                         AND pec.stream_id = pws.stream_id
                                         AND pec.event_time >= pws.segment_start
                                         AND pec.event_time < pws.segment_end
    ),

    -- Step 2c: Calculate the final sum of (value_delta * weight_at_that_time) per event_time
    weighted_value_deltas AS (
        SELECT
            event_time,
            SUM(delta_value * active_weight_sum)::numeric(72, 18) AS delta_ws -- Weighted sum delta
        FROM weighted_event_deltas
        GROUP BY event_time
    ),

    -- Step 2d: Find the first time each primitive stream contributes a value
    -- (Used to calculate effective weight changes accurately)
    first_value_times AS (
        SELECT
            data_provider,
            stream_id,
            MIN(event_time) as first_value_time
        FROM all_primitive_points -- Use the corrected definition with actual initial state time
        GROUP BY data_provider, stream_id
    ),

    -- Step 2e: Calculate the *effective* weight deltas based on first value time
    -- This represents the change in the SUM of weights active at any given time.
    eff_weight_deltas_unaggr AS (
        -- Positive delta at the start of an effective interval
        SELECT
            GREATEST(pw.group_sequence_start, fvt.first_value_time) AS time_point,
            pw.raw_weight AS delta
        FROM primitive_weights pw
        INNER JOIN first_value_times fvt
          ON pw.data_provider = fvt.data_provider AND pw.stream_id = fvt.stream_id
        -- Only consider intervals that actually start (start <= end)
        WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
          AND pw.raw_weight != 0::numeric(36,18) -- Keep precision 36 here

        UNION ALL

        -- Negative delta at the end+1 of an effective interval
        SELECT
            pw.group_sequence_end + 1 AS time_point,
            -pw.raw_weight AS delta
        FROM primitive_weights pw
        INNER JOIN first_value_times fvt
          ON pw.data_provider = fvt.data_provider AND pw.stream_id = fvt.stream_id
        -- Only consider intervals that actually start (start <= end)
        WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
          AND pw.raw_weight != 0::numeric(36,18)
    ),

    effective_weight_deltas AS (
        SELECT
            time_point,
            SUM(delta)::numeric(36,18) as effective_delta_sw -- Sum of weights delta
        FROM eff_weight_deltas_unaggr
        GROUP BY time_point
    ),

    -- Step 3: Generate all unique time points where either the weighted value sum or the effective weight sum changes
    all_combined_times AS (
        SELECT event_time as time_point FROM weighted_value_deltas
        UNION
        SELECT time_point FROM effective_weight_deltas
        UNION
        SELECT event_time as time_point FROM cleaned_event_times -- Ensure user requested times are included
    ),
    
    /*----------------------------------------------------------------------
  * Calculate the state (value) of each relevant primitive just BEFORE
  * each taxonomy change point.
  *---------------------------------------------------------------------*/
  state_before_change AS (
      SELECT
          pct.change_time,
          pe.data_provider,
          pe.stream_id,
          pe.value AS value_before
      FROM period_change_times pct
      JOIN primitive_events pe
          -- Find the latest event strictly BEFORE the change time
          ON pe.event_time = (
              SELECT MAX(pe_inner.event_time)
              FROM primitive_events pe_inner
              WHERE pe_inner.data_provider = pe.data_provider
                AND pe_inner.stream_id = pe.stream_id
                AND pe_inner.event_time < pct.change_time -- Strictly before
                AND pe_inner.created_at <= $effective_frozen_at
          )
      WHERE EXISTS ( -- Ensure this primitive was relevant *before* the change
          SELECT 1 FROM primitive_weights pw
          WHERE pw.data_provider = pe.data_provider
            AND pw.stream_id = pe.stream_id
            AND pct.change_time > pw.group_sequence_start -- Change time is after the weight start
            AND pct.change_time <= pw.group_sequence_end + 1 -- Change time is within or just after the weight end
      )
  ),

    /*----------------------------------------------------------------------
    * Calculate the state (value) of each relevant primitive AT
    * each taxonomy change point (using the configuration active AT that time).
    *---------------------------------------------------------------------*/
    state_at_change AS (
        SELECT
            pct.change_time,
            pe.data_provider,
            pe.stream_id,
            pe.value AS value_at
        FROM period_change_times pct
        JOIN primitive_events pe
            -- Find the latest event AT or BEFORE the change time
            ON pe.event_time = (
                SELECT MAX(pe_inner.event_time)
                FROM primitive_events pe_inner
                WHERE pe_inner.data_provider = pe.data_provider
                  AND pe_inner.stream_id = pe.stream_id
                  AND pe_inner.event_time <= pct.change_time -- At or before
                  AND pe_inner.created_at <= $effective_frozen_at
            )
        WHERE EXISTS ( -- Ensure this primitive is relevant *at* the change time
            SELECT 1 FROM primitive_weights pw
            WHERE pw.data_provider = pe.data_provider
              AND pw.stream_id = pe.stream_id
              AND pct.change_time >= pw.group_sequence_start -- Change time is within the weight period
              AND pct.change_time <= pw.group_sequence_end
        )
    ),

    /*----------------------------------------------------------------------
    * Determine the weights of each primitive just BEFORE and AT the change point.
    * Step 1: Pre-calculate boolean flags for interval matching.
    *---------------------------------------------------------------------*/
    bound_wgt_changes_intermediate AS (
        SELECT
            pct.change_time,
            up.data_provider,
            up.stream_id,
            pw.raw_weight,
            -- Evaluate conditions relative to pw interval, handling potential NULL pw
            CASE WHEN pw.raw_weight IS NULL THEN false ELSE (pct.change_time > pw.group_sequence_start AND pct.change_time <= pw.group_sequence_end + 1) END as is_relevant_before,
            CASE WHEN pw.raw_weight IS NULL THEN false ELSE (pct.change_time >= pw.group_sequence_start AND pct.change_time <= pw.group_sequence_end) END as is_relevant_at
        FROM period_change_times pct
        JOIN ( -- Use CROSS JOIN to combine every change time with every unique primitive
            -- All unique primitive streams involved
            SELECT DISTINCT data_provider, stream_id FROM primitive_weights
        ) up ON 1=1
        LEFT JOIN primitive_weights pw -- Join ALL weight intervals for the primitive
            ON pw.data_provider = up.data_provider
            AND pw.stream_id = up.stream_id
            -- No time filtering here, handled in CASE above
    ),
    /*----------------------------------------------------------------------
    * Step 2: Aggregate using the pre-calculated flags.
    *---------------------------------------------------------------------*/
    boundary_weight_changes AS (
        SELECT
            change_time,
            data_provider,
            stream_id,
            -- Use the flags to apply MAX
            COALESCE(MAX(CASE WHEN is_relevant_before THEN raw_weight ELSE NULL END), 0::numeric(36,18))::numeric(36,18) AS weight_before,
            COALESCE(MAX(CASE WHEN is_relevant_at THEN raw_weight ELSE NULL END), 0::numeric(36,18))::numeric(36,18) AS weight_at
        FROM bound_wgt_changes_intermediate
        GROUP BY change_time, data_provider, stream_id
    ),

    /*----------------------------------------------------------------------
    * Calculate the adjustment deltas needed at each boundary change.
    * Adjustment = (State At * Weight At) - (State Before * Weight Before)
    *---------------------------------------------------------------------*/
    adjustment_deltas AS (
        SELECT
            bwc.change_time AS event_time,
            -- Calculate weighted sum adjustment (delta_ws)
            SUM(
                COALESCE(sac.value_at, 0::numeric(36,18)) * bwc.weight_at
                -
                COALESCE(sbc.value_before, 0::numeric(36,18)) * bwc.weight_before
            )::numeric(72,18) AS delta_ws,
            -- Calculate weight sum adjustment (delta_sw)
            SUM(bwc.weight_at - bwc.weight_before)::numeric(36,18) AS delta_sw
        FROM boundary_weight_changes bwc
        LEFT JOIN state_before_change sbc
            ON bwc.change_time = sbc.change_time
            AND bwc.data_provider = sbc.data_provider
            AND bwc.stream_id = sbc.stream_id
        LEFT JOIN state_at_change sac
            ON bwc.change_time = sac.change_time
            AND bwc.data_provider = sac.data_provider
            AND bwc.stream_id = sac.stream_id
        WHERE
            -- Only include if there's a change in either weighted value or weight
            (COALESCE(sac.value_at, 0::numeric(36,18)) * bwc.weight_at != COALESCE(sbc.value_before, 0::numeric(36,18)) * bwc.weight_before)
            OR
            (bwc.weight_at != bwc.weight_before)
        GROUP BY bwc.change_time
    ),


    -- Step 4: Combine the value deltas and weight deltas
    combined_deltas AS (
        SELECT event_time, delta_ws, 0::numeric(36,18) AS delta_sw FROM weighted_value_deltas
        UNION ALL
        -- Alias time_point as event_time and select the correctly named weight delta column
        SELECT time_point as event_time, 0::numeric(72,18) AS delta_ws, effective_delta_sw AS delta_sw FROM effective_weight_deltas
    ),

    /*----------------------------------------------------------------------
     * FINAL DELTAS: Combine regular deltas with boundary adjustment deltas.
     *---------------------------------------------------------------------*/
    final_deltas AS (
        SELECT
            event_time,
            SUM(delta_ws) as delta_ws,
            SUM(delta_sw) as delta_sw
        FROM (
            SELECT event_time, delta_ws, delta_sw FROM combined_deltas
            UNION ALL
            SELECT event_time, delta_ws, delta_sw FROM adjustment_deltas
        ) all_deltas
        GROUP BY event_time
    ),

    -- Step 5: Calculate the cumulative sum of deltas over time
    cumulative_values AS (
        SELECT
            event_time,
            (SUM(delta_ws) OVER (ORDER BY event_time ASC))::numeric(72,18) as cum_ws,
            (SUM(delta_sw) OVER (ORDER BY event_time ASC))::numeric(36,18) as cum_sw
        FROM final_deltas -- Use final_deltas which includes adjustments
    ),

    -- Step 6: Compute the aggregated value (Weighted Average) at each event time
    aggregated AS (
        SELECT
            event_time,
            CASE WHEN cum_sw = 0::numeric(36,18) THEN 0::numeric(72,18)
                 -- Cast cum_sw to match cum_ws precision before division
                 ELSE cum_ws / cum_sw::numeric(72,18)
            END AS value
        FROM cumulative_values
    ),

    -- Step 7: Identify time points representing actual data origins or effective weight changes
    -- These times are used to determine the correct source timestamp for LOCF lookups.
    real_change_times AS (
        -- Time of the initial state before the interval (if one exists)
        SELECT event_time as time_point
        FROM initial_primitive_states -- Use the actual event_time here, not $effective_from

        UNION -- UNION for distinct

        -- Times of actual primitive events strictly WITHIN the interval
        SELECT event_time as time_point
        FROM primitive_events_in_interval

        UNION -- UNION for distinct

        -- Times where an effective weight interval STARTED
        SELECT time_point
        FROM effective_weight_deltas
        WHERE effective_delta_sw > 0::numeric(36,18) -- Only consider positive deltas (starts)
    ),

    -- Step 8: Map query times to their effective data timestamp for LOCF
    final_mapping AS (
        SELECT
            agg.event_time,
            agg.value,
            -- Find the latest time point AT or BEFORE the query time where a REAL change happened
            (SELECT MAX(rct.time_point)
             FROM real_change_times rct
             WHERE rct.time_point <= agg.event_time) AS effective_time,
            -- Check if the query time itself corresponds to a REAL change
            EXISTS (SELECT 1 FROM real_change_times rct WHERE rct.time_point = agg.event_time) AS query_time_had_real_change
        FROM aggregated agg
    )
    -- SELECT * FROM hierarchy {
    --   NOTICE('DEBUG hierarchy: ' || $row.parent_data_provider::text || ' ' || $row.parent_stream_id::text || ' ' || $row.child_data_provider::text || ' ' || $row.child_stream_id::text || ' ' || $row.group_sequence_start::text || ' ' || $row.group_sequence_end::text || ' ' || $row.raw_weight::text);
    -- }

    -- Final Output: Select the effective timestamp and the calculated value
    SELECT
        CASE
            WHEN fm.query_time_had_real_change THEN fm.event_time -- If the query time had a real change, use it
            ELSE fm.effective_time -- Otherwise, use the time of the last real change (LOCF source time)
        END as event_time,
        fm.value::NUMERIC(36,18) -- Cast final result back down to required precision
    FROM final_mapping fm
    -- Filter to only the rows corresponding to the user's requested time range [$from, $to]
    WHERE fm.event_time >= $effective_from AND fm.event_time <= $effective_to
    -- Filter out results corresponding to queries before the very first actual data point/delta
    AND CASE WHEN fm.query_time_had_real_change THEN fm.event_time ELSE fm.effective_time END IS NOT NULL
    ORDER BY 1; -- Order by the first column (the calculated event_time)
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
      ) ot
        ON base.data_provider = ot.data_provider
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
          LEAST(parent.group_sequence_end,   child.group_sequence_end) AS group_sequence_end,
          child.max_group_sequence -- Added for debugging
      FROM hierarchy parent
      JOIN (
          SELECT
              base.data_provider,
              base.stream_id,
              base.child_data_provider,
              base.child_stream_id,
              base.start_time,
              base.group_sequence,
              base.weight,
              COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
          FROM (
              SELECT
                  t.data_provider,
                  t.stream_id,
                  t.child_data_provider,
                  t.child_stream_id,
                  t.start_time,
                  t.group_sequence,
                  t.weight,
                  MAX(t.group_sequence) OVER (
                      PARTITION BY t.data_provider, t.stream_id, t.start_time
                  ) AS max_group_sequence
              FROM taxonomies t
              WHERE t.disabled_at IS NULL
                AND t.start_time <= $effective_to
                AND t.start_time >= COALESCE(
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
          ) base
          JOIN (
              SELECT
                  dt.data_provider,
                  dt.stream_id,
                  dt.start_time,
                  LEAD(dt.start_time) OVER (
                      PARTITION BY dt.data_provider, dt.stream_id
                      ORDER BY dt.start_time
                  ) AS next_start
              FROM (
                  SELECT DISTINCT
                      t.data_provider,
                      t.stream_id,
                      t.start_time
                  FROM taxonomies t
                  WHERE t.disabled_at IS NULL
                    AND t.start_time <= $effective_to
                    AND t.start_time >= COALESCE(
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
          ) ot
            ON base.data_provider = ot.data_provider
           AND base.stream_id     = ot.stream_id
           AND base.start_time    = ot.start_time
          WHERE base.group_sequence = base.max_group_sequence
      ) child
        ON child.data_provider = parent.child_data_provider
       AND child.stream_id     = parent.child_stream_id
      WHERE child.start_time         <= parent.group_sequence_end
        AND child.group_sequence_end >= parent.group_sequence_start
        AND child.group_sequence = child.max_group_sequence
    ),

    /*----------------------------------------------------------------------
     * 3) Identify only LEAF nodes (streams of type 'primitive').
     * We keep their [start, end] intervals to figure out which events
     * we actually need. 
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
