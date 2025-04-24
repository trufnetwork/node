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
    -- Note: Base time logic differs slightly from get_record_composed which defaults to 0.
    -- Here we might need to query the first actual event if metadata is missing.
    -- For simplicity and consistency with original get_index, let's keep COALESCE to 0 for now,
    -- but consider revising if a true 'first event' base is needed when metadata is absent.
    $effective_base_time := COALESCE($effective_base_time, 0);

    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('Invalid time range: from (%s) > to (%s)', $from, $to));
    }

    -- Permissions check (consider if compose permissions are needed here too)
    IF !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }
    IF !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
        ERROR('Not allowed to compose stream');
    }


    -- For detailed explanations of the CTEs below (hierarchy, primitive_weights,
    -- cleaned_event_times, initial_primitive_states, primitive_events_in_interval,
    -- all_primitive_points, first_value_times, effective_weight_changes, unified_events),
    -- please refer to the comments in the `get_record_composed` action
    -- in 006-composed-query.sql. The logic is largely identical.

    RETURN WITH RECURSIVE
    hierarchy AS (
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
      LEFT JOIN taxonomies t2
        ON t1.data_provider = t2.data_provider
       AND t1.stream_id = t2.stream_id
       AND t1.start_time = t2.start_time
       AND t1.group_sequence < t2.group_sequence
       AND t2.disabled_at IS NULL
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
          t1.data_provider = $data_provider
      AND t1.stream_id     = $stream_id
      AND t1.disabled_at   IS NULL
      AND t2.group_sequence IS NULL
      AND t1.start_time <= $effective_to
      AND t1.start_time >= COALESCE(
            (SELECT t_anchor_base.start_time
             FROM taxonomies t_anchor_base
             WHERE t_anchor_base.data_provider = t1.data_provider
               AND t_anchor_base.stream_id     = t1.stream_id
               AND t_anchor_base.disabled_at   IS NULL
               AND t_anchor_base.start_time   <= $effective_from
             ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC
             LIMIT 1
            ), 0
          )

      UNION ALL

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
      JOIN taxonomies t1_child
        ON t1_child.data_provider = parent.child_data_provider
       AND t1_child.stream_id     = parent.child_stream_id
      LEFT JOIN taxonomies t2_child
        ON t1_child.data_provider = t2_child.data_provider
       AND t1_child.stream_id = t2_child.stream_id
       AND t1_child.start_time = t2_child.start_time
       AND t1_child.group_sequence < t2_child.group_sequence
       AND t2_child.disabled_at IS NULL
      JOIN (
          SELECT
              dt.data_provider, dt.stream_id, dt.start_time,
              LEAD(dt.start_time) OVER ( PARTITION BY dt.data_provider, dt.stream_id ORDER BY dt.start_time ) AS next_start
          FROM ( SELECT DISTINCT t_otc.data_provider, t_otc.stream_id, t_otc.start_time FROM taxonomies t_otc
                 WHERE t_otc.disabled_at IS NULL AND t_otc.start_time <= $effective_to
                 AND t_otc.start_time >= COALESCE((SELECT MIN(t2_min.start_time) FROM taxonomies t2_min WHERE t2_min.disabled_at IS NULL AND t2_min.start_time <= $effective_from), 0)
               ) dt
      ) ot_child
        ON t1_child.data_provider = ot_child.data_provider
       AND t1_child.stream_id     = ot_child.stream_id
       AND t1_child.start_time    = ot_child.start_time
      WHERE
          t1_child.disabled_at IS NULL
      AND t2_child.group_sequence IS NULL
      AND t1_child.start_time <= parent.group_sequence_end
      AND (COALESCE(ot_child.next_start, $max_int8) - 1) >= parent.group_sequence_start
      AND t1_child.start_time <= $effective_to
      AND t1_child.start_time >= COALESCE(
            (SELECT t_anchor_child.start_time
             FROM taxonomies t_anchor_child
             WHERE t_anchor_child.data_provider = t1_child.data_provider
               AND t_anchor_child.stream_id     = t1_child.stream_id
               AND t_anchor_child.disabled_at   IS NULL
               AND t_anchor_child.start_time   <= $effective_from
             ORDER BY t_anchor_child.start_time DESC, t_anchor_child.group_sequence DESC
             LIMIT 1
            ), 0
          )
    ),

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

    -- Base Value Calculation: Determine the base value for each primitive stream.
    -- This value is used to normalize raw values into index values (typically 100 at base time).
    primitive_base_values AS (
        SELECT
            bv_calc.data_provider,
            bv_calc.stream_id,
            -- Use COALESCE for safety, though base value should ideally always exist if stream has data.
            COALESCE(bv_calc.value, 1::numeric(36,18))::numeric(36,18) AS base_value -- Default to 1 if somehow no base value found to avoid division by zero.
        FROM (
            SELECT
                p_base.data_provider,
                p_base.stream_id,
                p_base.value,
                ROW_NUMBER() OVER (
                    PARTITION BY p_base.data_provider, p_base.stream_id
                    ORDER BY
                        -- Prioritize event exactly at base time
                        CASE WHEN p_base.event_time = $effective_base_time THEN 0 ELSE 1 END ASC,
                        -- Then latest event at or before base time
                        CASE WHEN p_base.event_time <= $effective_base_time THEN p_base.event_time END DESC NULLS LAST,
                        -- Then earliest event after base time
                        CASE WHEN p_base.event_time > $effective_base_time THEN p_base.event_time END ASC NULLS LAST,
                        -- Tie-break by creation time
                        p_base.created_at DESC
                ) as rn
            FROM primitive_events p_base
            WHERE EXISTS ( -- Ensure the primitive is part of the hierarchy
                SELECT 1 FROM primitive_weights pw_base
                WHERE pw_base.data_provider = p_base.data_provider AND pw_base.stream_id = p_base.stream_id
            )
            AND p_base.created_at <= $effective_frozen_at
        ) bv_calc
        WHERE bv_calc.rn = 1
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
        WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
          AND pw.raw_weight != 0::numeric(36,18)
    ),

    -- Combine indexed value changes and weight changes.
    unified_events AS (
        SELECT
            pec.data_provider,
            pec.stream_id,
            pec.event_time,
            pec.delta_indexed_value, -- Use delta_indexed_value here
            0::numeric(36,18) AS weight_delta
        FROM primitive_event_changes pec

        UNION ALL

        SELECT
            ewc.data_provider,
            ewc.stream_id,
            ewc.event_time,
            0::numeric(36,18) AS delta_indexed_value, -- Zero indexed value change for weight events
            ewc.weight_delta
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
            COALESCE(LAG(indexed_value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time), 0::numeric(36,18)) as indexed_value_before_event,
            COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time), 0::numeric(36,18)) as weight_before_event
        FROM (
            SELECT
                data_provider,
                stream_id,
                event_time,
                delta_indexed_value,
                weight_delta,
                -- Cumulative indexed value up to and including this event
                (SUM(delta_indexed_value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time))::numeric(36,18) as indexed_value_after_event,
                -- Cumulative weight up to and including this event
                (SUM(weight_delta) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time))::numeric(36,18) as weight_after_event
            FROM unified_events
        ) state_calc
    ),

    -- Calculate final aggregated deltas using indexed values.
    final_deltas AS (
        SELECT
            event_time,
            -- Calculate delta for the weighted sum numerator using indexed values
            SUM((delta_indexed_value * weight_before_event) + (weight_delta * indexed_value_before_event))::numeric(72, 18) AS delta_ws_indexed,
            SUM(weight_delta)::numeric(36, 18) AS delta_sw
        FROM primitive_state_timeline
        GROUP BY event_time
        HAVING SUM((delta_indexed_value * weight_before_event) + (weight_delta * indexed_value_before_event))::numeric(72, 18) != 0::numeric(72, 18)
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
        SELECT DISTINCT event_time AS time_point
        FROM final_deltas
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
