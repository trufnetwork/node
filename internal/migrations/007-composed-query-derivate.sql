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
    $data_provider  := LOWER($data_provider);
    $lower_caller  := LOWER(@caller);
    /*
     * Step 1: Basic setup
     */
    IF !is_allowed_to_read_all($data_provider, $stream_id, $lower_caller, NULL, $before) {
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
        AND t.disabled_at IS NULL

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
       AND t.disabled_at IS NULL
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
    $data_provider  := LOWER($data_provider);
    $lower_caller  := LOWER(@caller);
    /*
     * Step 1: Basic setup
     */
    IF !is_allowed_to_read_all($data_provider, $stream_id, $lower_caller, $after, NULL) {
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
        AND t.disabled_at IS NULL

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
       AND t.disabled_at IS NULL
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

    -- Permissions check
    IF !is_allowed_to_read_all($data_provider, $stream_id, $lower_caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }
    IF !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
        ERROR('Not allowed to compose stream');
    }

    -- If both $from and $to are NULL, find the latest event time
    IF $from IS NULL AND $to IS NULL {
        $actual_latest_event_time INT8;
        $found_latest_event BOOLEAN := FALSE;

        FOR $last_record_row IN get_last_record_composed($data_provider, $stream_id, NULL, $effective_frozen_at) {
            $actual_latest_event_time := $last_record_row.event_time;
            $found_latest_event := TRUE;
            BREAK;
        }

        IF $found_latest_event {
            $effective_from := $actual_latest_event_time;
            $effective_to   := $actual_latest_event_time;
        } ELSE {
            RETURN;
        }
    }
    
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
     * HIERARCHY CTE: FIXED TO USE EFFECTIVE WEIGHTS
     * This now properly calculates effective weights that preserve 
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
          WHERE s.data_provider = h.descendant_dp
            AND s.stream_id     = h.descendant_sid
            AND s.stream_type   = 'primitive'
      )
    ),

    /*----------------------------------------------------------------------
     * REST OF THE QUERY REMAINS THE SAME
     * The primitive_weights and subsequent CTEs will now use the corrected
     * effective weights instead of the flawed raw weight multiplication.
     *--------------------------------------------------------------------*/
    primitive_weights AS (
      SELECT
          hpp.child_data_provider AS data_provider,
          hpp.child_stream_id     AS stream_id,
          hpp.raw_weight,
          hpp.group_sequence_start,
          hpp.group_sequence_end
      FROM hierarchy_primitive_paths hpp
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

    distinct_primitives_for_base AS (
       SELECT DISTINCT data_provider, stream_id
        FROM all_primitive_points
    ),

    primitive_base_values AS (
        SELECT
            dp.data_provider,
            dp.stream_id,
             COALESCE(
                 (SELECT pe.value FROM primitive_events pe
                  WHERE pe.data_provider = dp.data_provider
                    AND pe.stream_id = dp.stream_id
                    AND pe.event_time = $effective_base_time
                    AND pe.created_at <= $effective_frozen_at
                  ORDER BY pe.created_at DESC LIMIT 1),

                 (SELECT pe.value FROM primitive_events pe
                  WHERE pe.data_provider = dp.data_provider
                    AND pe.stream_id = dp.stream_id
                    AND pe.event_time < $effective_base_time
                    AND pe.created_at <= $effective_frozen_at
                   ORDER BY pe.event_time DESC, pe.created_at DESC LIMIT 1),

                  (SELECT pe.value FROM primitive_events pe
                   WHERE pe.data_provider = dp.data_provider
                    AND pe.stream_id = dp.stream_id
                    AND pe.event_time > $effective_base_time
                    AND pe.created_at <= $effective_frozen_at
                   ORDER BY pe.event_time ASC, pe.created_at DESC LIMIT 1),

                  1::numeric(36,18)
             )::numeric(36,18) AS base_value
        FROM distinct_primitives_for_base dp
    ),

    primitive_event_changes AS (
        SELECT
            calc.data_provider,
            calc.stream_id,
            calc.event_time,
            calc.value,
            calc.delta_value,
            CASE
                WHEN COALESCE(pbv.base_value, 0::numeric(36,18)) = 0::numeric(36,18) THEN 0::numeric(36,18)
                ELSE (calc.delta_value * 100::numeric(36,18) / pbv.base_value)::numeric(36,18)
            END AS delta_indexed_value
        FROM (
            SELECT data_provider, stream_id, event_time, value,
                    COALESCE(value - LAG(value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time), value)::numeric(36,18) AS delta_value
            FROM all_primitive_points
        ) calc
        JOIN primitive_base_values pbv
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
            AND pw.group_sequence_end < ($max_int8 - 1)
    ),

    unified_events AS (
        SELECT
            pec.data_provider,
            pec.stream_id,
            pec.event_time,
            pec.delta_indexed_value,
            0::numeric(36,18) AS weight_delta,
            1 AS event_type_priority
        FROM primitive_event_changes pec

        UNION ALL

        SELECT
            ewc.data_provider,
            ewc.stream_id,
            ewc.event_time,
            0::numeric(36,18) AS delta_indexed_value,
            ewc.weight_delta,
            2 AS event_type_priority
        FROM effective_weight_changes ewc
    ),

    primitive_state_timeline AS (
        SELECT
            data_provider,
            stream_id,
            event_time,
            delta_indexed_value,
            weight_delta,
            COALESCE(LAG(indexed_value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as indexed_value_before_event,
            COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as weight_before_event
        FROM (
            SELECT
                data_provider,
                stream_id,
                event_time,
                delta_indexed_value,
                weight_delta,
                event_type_priority,
                (SUM(delta_indexed_value) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as indexed_value_after_event,
                (SUM(weight_delta) OVER (PARTITION BY data_provider, stream_id ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as weight_after_event
            FROM unified_events
        ) state_calc
    ),

    final_deltas AS (
        SELECT
            event_time,
            SUM(
                (delta_indexed_value * weight_before_event) +
                (indexed_value_before_event * weight_delta) +
                (delta_indexed_value * weight_delta)
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

    cumulative_values AS (
        SELECT
            act.time_point as event_time,
            (COALESCE((SUM(fd.delta_ws_indexed) OVER (ORDER BY act.time_point ASC))::numeric(72,18), 0::numeric(72,18))) as cum_ws_indexed,
            (COALESCE((SUM(fd.delta_sw) OVER (ORDER BY act.time_point ASC))::numeric(36,18), 0::numeric(36,18))) as cum_sw
        FROM all_combined_times act
        LEFT JOIN final_deltas fd ON fd.event_time = act.time_point
    ),

    aggregated AS (
        SELECT cv.event_time,
               CASE WHEN cv.cum_sw = 0::numeric(36,18) THEN 0::numeric(72,18)
                    ELSE cv.cum_ws_indexed / cv.cum_sw::numeric(72,18)
                   END AS value
        FROM cumulative_values cv
    ),

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

    range_check AS (
        SELECT EXISTS (
            SELECT 1 FROM final_mapping fm_check
            WHERE fm_check.event_time >= $effective_from
              AND fm_check.event_time <= $effective_to
        ) AS range_has_direct_hits
    ),

    locf_applied AS (
        SELECT
            fm.*,
            rc.range_has_direct_hits,
            atc.anchor_time,
            CASE
                WHEN fm.query_time_had_real_change THEN fm.event_time
                ELSE fm.effective_time
            END as final_event_time
        FROM filtered_mapping fm
        JOIN range_check rc ON 1=1
        JOIN anchor_time_calc atc ON 1=1
    ),

    direct_hits AS (
        SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
        FROM locf_applied la
        WHERE la.event_time >= $effective_from
          AND la.event_time <= $effective_to
          AND la.final_event_time IS NOT NULL
    ),
    anchor_hit AS (
      SELECT final_event_time as event_time, value::NUMERIC(36,18) as value
      FROM locf_applied la
      WHERE la.anchor_time IS NOT NULL
        AND la.event_time = la.anchor_time
        AND $effective_from > la.anchor_time
        AND la.final_event_time IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM locf_applied dh
            WHERE dh.event_time = $effective_from
        )
    ),
    result AS (
        SELECT event_time, value FROM direct_hits
        UNION ALL
        SELECT event_time, value FROM anchor_hit
    )
    SELECT event_time, value FROM result
    ORDER BY 1;
};

-- Returns the base value for a composed stream at or around base_time.
-- This is a helper function for get_record_composed_index readability.
CREATE OR REPLACE ACTION internal_get_base_value(
    $data_provider TEXT,
    $stream_id     TEXT,
    $effective_base_time     INT8,   -- already pre-resolved "effective base time"
    $effective_frozen_at     INT8    -- created_at cutoff (can be NULL ⇢ infinity)
) PRIVATE VIEW RETURNS (NUMERIC(36,18)) {
    -- doesn't check for access control, as it's private and not responsible for
    -- any access control checks

    -- Try to find an exact match at base_time
    $found_exact := FALSE;
    $exact_value NUMERIC(36,18);
    for $row in get_record_composed($data_provider, $stream_id, $effective_base_time, $effective_base_time, $effective_frozen_at) {
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
    for $row in get_last_record_composed($data_provider, $stream_id, $effective_base_time, $effective_frozen_at) {
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
    for $row in get_first_record_composed($data_provider, $stream_id, $effective_base_time, $effective_frozen_at) {
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