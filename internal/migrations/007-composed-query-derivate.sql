-- NOTE: This function finds the single latest event ignoring overshadow,
-- then uses get_record_composed() at that time to apply overshadow logic.
-- It's simplified, may miss certain edge cases, but usually sufficient.

CREATE OR REPLACE ACTION get_last_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,       -- Upper bound for event_time
    $frozen_at INT8,    -- Only consider events created on or before this
    $use_cache BOOL DEFAULT false     -- Whether to use cache (default: false)
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
  $stream_ref := get_stream_id($data_provider, $stream_id);

  IF !is_allowed_to_read_all_core($stream_ref, $lower_caller, NULL, $before) {
      ERROR('Not allowed to read stream');
  }

  -- Set default value for enable_cache
  $effective_enable_cache := COALESCE($use_cache, false);
  $effective_enable_cache := $effective_enable_cache AND $frozen_at IS NULL; -- frozen queries bypass cache

  if $effective_enable_cache {
      -- we use before as from, because if we have data for that, it automatically means
      -- that we can answer this query
      $effective_enable_cache := helper_check_cache($data_provider, $stream_id, $before, NULL);
  }

  -- If using cache, get the most recent cached record
  if $effective_enable_cache {
      for $row in tn_cache.get_cached_last_before($data_provider, $stream_id, $before) {
          RETURN NEXT $row.event_time, $row.value;
      }
      RETURN;
  }

  -- Original logic fallback (cache miss or cache disabled)

  $max_int8 INT8 := 9223372036854775000;    -- "Infinity" sentinel
  $effective_before INT8 := COALESCE($before, $max_int8);
  $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);

  $latest_event_time INT8;

  /*
  * Step 2: Recursively gather all children (ignoring overshadow),
  *         then identify primitive leaves.
  */
  for $row in WITH RECURSIVE all_taxonomies AS (
    /* 2a) Direct children of $stream_ref */
    SELECT
      t.stream_ref,
      t.child_stream_ref
    FROM taxonomies t
    WHERE t.stream_ref = $stream_ref
      AND t.disabled_at IS NULL

    UNION

    /* 2b) For each discovered child, gather its own children */
    SELECT
      at.child_stream_ref AS stream_ref,
      t.child_stream_ref
    FROM all_taxonomies at
    JOIN taxonomies t
      ON t.stream_ref = at.child_stream_ref
    AND t.disabled_at IS NULL
  ),
  primitive_leaves AS (
    /* Keep only references pointing to primitive streams */
    SELECT DISTINCT
      at.child_stream_ref AS stream_ref
    FROM all_taxonomies at
    JOIN streams s
      ON s.id = at.child_stream_ref
    AND s.stream_type = 'primitive'
  ),
  /*
  * Step 3: In each primitive, pick the single latest event_time <= effective_before.
  *         ROW_NUMBER=1 => that "latest" champion. Tie-break by created_at DESC.
  */
  latest_events AS (
    SELECT
      pl.stream_ref,
      pe.event_time,
      pe.value,
      pe.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY pl.stream_ref
        ORDER BY pe.event_time DESC, pe.created_at DESC
      ) AS rn
    FROM primitive_leaves pl
    JOIN primitive_events pe
      ON pe.stream_ref = pl.stream_ref
    WHERE pe.event_time   <= $effective_before
      AND pe.created_at   <= $effective_frozen_at
  ),
  latest_values AS (
    /* Step 4: Filter to rn=1 => the single latest event per stream_ref */
    SELECT
      stream_ref,
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
      for $row in get_record_composed($data_provider, $stream_id, $latest_event_time, $latest_event_time, $frozen_at, $use_cache) {
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
    $frozen_at INT8,   -- Only consider events created on or before this
    $use_cache BOOL DEFAULT false    -- Whether to use cache (default: false)
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
    IF !is_allowed_to_read_all_core($stream_ref, $lower_caller, $after, NULL) {
        ERROR('Not allowed to read stream');
    }

    -- Check compose permissions
    if !is_allowed_to_compose_all_core($stream_ref, $after, NULL) {
        ERROR('Not allowed to compose stream');
    }

    -- Set default value for enable_cache
    $effective_enable_cache := COALESCE($use_cache, false);
    $effective_enable_cache := $effective_enable_cache AND $frozen_at IS NULL; -- frozen queries bypass cache

    if $effective_enable_cache {
        -- we use after as to, because if we have data for that, it automatically means
        -- that we can answer this query
        $effective_enable_cache := helper_check_cache($data_provider, $stream_id, $after, NULL);
    }

    -- If using cache, get the earliest cached record
    if $effective_enable_cache {
        -- Get cached data from the after time and return the earliest
        for $row in tn_cache.get_cached_first_after($data_provider, $stream_id, $after) {
            RETURN NEXT $row.event_time, $row.value;
        }

        RETURN;
    }

    -- Original logic fallback (cache miss or cache disabled)

    $max_int8 INT8 := 9223372036854775000;   -- "Infinity" sentinel
    $effective_after INT8 := COALESCE($after, 0);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    $earliest_event_time INT8;

    /*
     * Step 2: Recursively gather all children (ignoring overshadow),
     *         then identify primitive leaves.
     */
   for $row in WITH RECURSIVE all_taxonomies AS (
      /* 2a) Direct children of $stream_ref */
      SELECT
        t.stream_ref,
        t.child_stream_ref
      FROM taxonomies t
      WHERE t.stream_ref = $stream_ref
        AND t.disabled_at IS NULL

      UNION

      /* 2b) For each discovered child, gather its own children */
      SELECT
        at.child_stream_ref AS stream_ref,
        t.child_stream_ref
      FROM all_taxonomies at
      JOIN taxonomies t
        ON t.stream_ref = at.child_stream_ref
      AND t.disabled_at IS NULL
    ),
    primitive_leaves AS (
      /* Keep only references pointing to primitive streams */
      SELECT DISTINCT
        at.child_stream_ref AS stream_ref
      FROM all_taxonomies at
      JOIN streams s
        ON s.id = at.child_stream_ref
      AND s.stream_type = 'primitive'
    ),
    /*
    * Step 3: In each primitive, pick the single earliest event_time >= effective_after.
    *         ROW_NUMBER=1 => that "earliest" champion. Tie-break by created_at DESC.
    */
    earliest_events AS (
      SELECT
        pl.stream_ref,
        pe.event_time,
        pe.value,
        pe.created_at,
        ROW_NUMBER() OVER (
          PARTITION BY pl.stream_ref
          ORDER BY pe.event_time ASC, pe.created_at DESC
        ) AS rn
      FROM primitive_leaves pl
      JOIN primitive_events pe
        ON pe.stream_ref = pl.stream_ref
      WHERE pe.event_time   >= $effective_after
        AND pe.created_at   <= $effective_frozen_at
    ),
    earliest_values AS (
      /* Step 4: Filter to rn=1 => the single earliest event per stream_ref */
      SELECT
        stream_ref,
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
        for $row in get_record_composed($data_provider, $stream_id, $earliest_event_time, $earliest_event_time, $frozen_at, $use_cache) {
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
    $base_time INT8,
    $use_cache BOOL DEFAULT false     -- Whether to use cache (default: false)
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
  $stream_ref := get_stream_id($data_provider, $stream_id);

  -- Fail-fast guard: ensure stream exists before calling _core function
  IF $stream_ref IS NULL {
      ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
  }

  -- Base time determination: Use parameter, metadata, or first event time.
  $effective_base_time INT8;
  if $base_time is not null {
      $effective_base_time := $base_time;
  } else {
      $effective_base_time := get_latest_metadata_int_core($stream_ref, 'default_base_time');
  }
  -- Note: Base time logic differs slightly from get_record_composed which defaults to 0.
  -- Here we might need to query the first actual event if metadata is missing.
  -- For simplicity and consistency with original get_index, let's keep COALESCE to 0 for now,
  -- but consider revising if a true 'first event' base is needed when metadata is absent.
  $effective_base_time := COALESCE($effective_base_time, 0);

  IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
      ERROR(format('Invalid time range: from (%s) > to (%s)', $from, $to));
  }

  -- Permissions check (must be done before cache logic)
  IF !is_allowed_to_read_all_core($stream_ref, $lower_caller, $from, $to) {
      ERROR('Not allowed to read stream');
  }
  IF !is_allowed_to_compose_all_core($stream_ref, $from, $to) {
      ERROR('Not allowed to compose stream');
  }

  -- Set default value for enable_cache
  $effective_enable_cache := COALESCE($use_cache, false);
  -- frozen queries and arbitrary base time bypass cache
  $effective_enable_cache := $effective_enable_cache AND $frozen_at IS NULL AND $base_time IS NULL;

  if $effective_enable_cache {
      -- Check if we have pre-calculated index values in cache
      $effective_enable_cache := helper_check_cache($data_provider, $stream_id, $from, $to);
  }

  -- If using pre-calculated index cache, return directly
  if $effective_enable_cache {
      for $row in tn_cache.get_cached_index_data($data_provider, $stream_id, $from, $to) {
          RETURN NEXT $row.event_time, $row.value;
      }
      RETURN;
  }

  -- Original logic fallback (cache miss or cache disabled, or custom base_time)

  -- If both $from and $to are NULL, we find the latest event time
  -- and set $effective_from and $effective_to to this single point.
  IF $from IS NULL AND $to IS NULL {
      $actual_latest_event_time INT8;
      $found_latest_event BOOLEAN := FALSE;

      FOR $last_record_row IN get_last_record_composed($data_provider, $stream_id, NULL, $effective_frozen_at, $use_cache) {
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
  * ANCHOR CTE: Compute the anchor time once for reuse throughout the query
  *
  * Purpose: Find the latest taxonomy start_time at or before $effective_from
  * for the target stream to establish a baseline for filtering.
  *---------------------------------------------------------------------*/
  anchor AS (
      SELECT COALESCE(
          (SELECT t_anchor_base.start_time FROM taxonomies t_anchor_base
           WHERE t_anchor_base.stream_ref = $stream_ref
             AND t_anchor_base.disabled_at IS NULL AND t_anchor_base.start_time <= $effective_from
           ORDER BY t_anchor_base.start_time DESC, t_anchor_base.group_sequence DESC LIMIT 1),
          0 -- Default anchor to 0
      ) AS anchor_time
  ),

  /*----------------------------------------------------------------------
  * RELEVANT_PARENTS CTE: Find all streams in the subtree of the target stream
  *---------------------------------------------------------------------*/
  relevant_parents AS (
      -- Base case: the target stream itself
      SELECT $stream_ref AS stream_ref
      UNION ALL
      -- Recursive case: traverse descendants (children of already discovered streams)
      SELECT DISTINCT t.child_stream_ref
      FROM taxonomies t
      JOIN relevant_parents rp ON t.stream_ref = rp.stream_ref
      WHERE t.disabled_at IS NULL
  ),

  /*----------------------------------------------------------------------
  * PARENT_DISTINCT_START_TIMES CTE:
  *---------------------------------------------------------------------*/
  parent_distinct_start_times AS (
      SELECT DISTINCT
          t.stream_ref,
          t.start_time
      FROM taxonomies t
      JOIN relevant_parents rp ON t.stream_ref = rp.stream_ref
      WHERE t.disabled_at IS NULL
  ),

  /*----------------------------------------------------------------------
  * PARENT_NEXT_STARTS CTE:
  *---------------------------------------------------------------------*/
  parent_next_starts AS (
      SELECT
          stream_ref,
          start_time,
          LEAD(start_time) OVER (PARTITION BY stream_ref ORDER BY start_time) as next_start_time
      FROM parent_distinct_start_times
  ),

  /*----------------------------------------------------------------------
  * TAXONOMY_TRUE_SEGMENTS CTE:
  *---------------------------------------------------------------------*/
  taxonomy_true_segments AS (
      SELECT
          t.stream_ref,
          t.child_stream_ref,
          t.weight_for_segment,
          t.segment_start,
          COALESCE(pns.next_start_time, $max_int8) - 1 AS segment_end
      FROM (
          SELECT
              tx.stream_ref,
              tx.child_stream_ref,
              tx.weight AS weight_for_segment,
              tx.start_time AS segment_start
          FROM taxonomies tx
          JOIN (
              SELECT
                  stream_ref, start_time,
                  MAX(group_sequence) as max_gs
              FROM taxonomies
              WHERE disabled_at IS NULL
              GROUP BY stream_ref, start_time
          ) max_gs_filter
          ON tx.stream_ref = max_gs_filter.stream_ref
        AND tx.start_time = max_gs_filter.start_time
        AND tx.group_sequence = max_gs_filter.max_gs
          WHERE tx.disabled_at IS NULL
      ) t
      JOIN parent_next_starts pns
        ON t.stream_ref = pns.stream_ref
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
        tts.stream_ref AS root_stream_ref,
        tts.child_stream_ref AS descendant_stream_ref,
        tts.weight_for_segment AS raw_weight,
        tts.weight_for_segment AS effective_weight, -- For level 1, effective = raw
        tts.segment_start AS path_start,
        tts.segment_end AS path_end,
        1 AS level
    FROM taxonomy_true_segments tts
    WHERE tts.stream_ref = $stream_ref
      AND tts.segment_end >= (SELECT anchor_time FROM anchor)
      AND tts.segment_start <= $effective_to

    UNION ALL

    -- Recursive Step: Calculate effective weights properly
    SELECT
        h.root_stream_ref,
        tts.child_stream_ref AS descendant_stream_ref,
        (h.raw_weight * tts.weight_for_segment)::NUMERIC(36,18) AS raw_weight,
        -- Calculate effective weight = parent_effective_weight × (child_weight / sibling_sum)
        (h.effective_weight * (
            tts.weight_for_segment / 
            -- Calculate sum of sibling weights for normalization
            (SELECT SUM(sibling_tts.weight_for_segment) 
            FROM taxonomy_true_segments sibling_tts 
            WHERE sibling_tts.stream_ref = h.descendant_stream_ref
              AND sibling_tts.segment_start = tts.segment_start
              AND sibling_tts.segment_end = tts.segment_end)::NUMERIC(36,18)
        ))::NUMERIC(36,18) AS effective_weight,
        GREATEST(h.path_start, tts.segment_start) AS path_start,
        LEAST(h.path_end, tts.segment_end) AS path_end,
        h.level + 1
    FROM
        hierarchy h
    JOIN taxonomy_true_segments tts
        ON h.descendant_stream_ref = tts.stream_ref
    WHERE
        GREATEST(h.path_start, tts.segment_start) <= LEAST(h.path_end, tts.segment_end)
        AND LEAST(h.path_end, tts.segment_end) >= (SELECT anchor_time FROM anchor)
        AND GREATEST(h.path_start, tts.segment_start) <= $effective_to
        AND h.level < 10 -- Recursion depth limit
  ),

  /*----------------------------------------------------------------------
  * HIERARCHY_PRIMITIVE_PATHS CTE: Updated to use effective_weight
  *--------------------------------------------------------------------*/
  hierarchy_primitive_paths AS (
    SELECT
        h.descendant_stream_ref AS primitive_stream_ref,
        h.effective_weight AS raw_weight, -- Use effective_weight instead of raw_weight
        h.path_start AS group_sequence_start,
        h.path_end AS group_sequence_end
    FROM hierarchy h
    WHERE EXISTS (
        SELECT 1 FROM streams s
        WHERE s.id = h.descendant_stream_ref
          AND s.stream_type = 'primitive'
    )
  ),

  /*----------------------------------------------------------------------
  * PRIMITIVE_WEIGHTS CTE: Passes through each distinct segment of a primitive's activity.
  * (Modified to be a pass-through like in 006-composed-query.sql)
  *--------------------------------------------------------------------*/
  primitive_weights AS (
    SELECT
        hpp.primitive_stream_ref,
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
            ON pe.stream_ref = pw.primitive_stream_ref
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
                ON pe.stream_ref = pw.primitive_stream_ref
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
          pe.stream_ref,
          pe.event_time,
          pe.value
      FROM (
          SELECT
              pe_inner.stream_ref,
              pe_inner.event_time,
              pe_inner.value,
              ROW_NUMBER() OVER (
                  PARTITION BY pe_inner.stream_ref
                  ORDER BY pe_inner.event_time DESC, pe_inner.created_at DESC
              ) as rn
          FROM primitive_events pe_inner
          WHERE pe_inner.event_time <= $effective_from
            AND EXISTS (
                SELECT 1 FROM primitive_weights pw_exists
                WHERE pw_exists.primitive_stream_ref = pe_inner.stream_ref
            )
            AND pe_inner.created_at <= $effective_frozen_at
      ) pe
      WHERE pe.rn = 1
  ),

  primitive_events_in_interval AS (
      SELECT
          pe.stream_ref,
          pe.event_time,
          pe.value
      FROM (
          SELECT
              pe_inner.stream_ref,
              pe_inner.event_time,
              pe_inner.created_at,
              pe_inner.value,
              ROW_NUMBER() OVER (
                  PARTITION BY pe_inner.stream_ref, pe_inner.event_time
                  ORDER BY pe_inner.created_at DESC
              ) as rn
          FROM primitive_events pe_inner
          JOIN primitive_weights pw_check
              ON pe_inner.stream_ref = pw_check.primitive_stream_ref
            AND pe_inner.event_time >= pw_check.group_sequence_start
            AND pe_inner.event_time <= pw_check.group_sequence_end
          WHERE pe_inner.event_time > $effective_from
              AND pe_inner.event_time <= $effective_to
              AND pe_inner.created_at <= $effective_frozen_at
      ) pe
      WHERE pe.rn = 1
  ),

  all_primitive_points AS (
      SELECT stream_ref, event_time, value FROM initial_primitive_states
      UNION ALL
      SELECT stream_ref, event_time, value FROM primitive_events_in_interval
  ),

  -- Defines the set of primitives that have data points within the requested
  -- time range and therefore require a base value calculation.
  distinct_primitives_for_base AS (
    SELECT DISTINCT stream_ref
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
          dp.stream_ref,
          COALESCE(
              -- Priority 1: Exact match
              (SELECT pe.value FROM primitive_events pe
                WHERE pe.stream_ref = dp.stream_ref
                  AND pe.event_time = $effective_base_time
                  AND pe.created_at <= $effective_frozen_at
                ORDER BY pe.created_at DESC LIMIT 1),

              -- Priority 2: Latest Before
              (SELECT pe.value FROM primitive_events pe
                WHERE pe.stream_ref = dp.stream_ref
                  AND pe.event_time < $effective_base_time
                  AND pe.created_at <= $effective_frozen_at
                ORDER BY pe.event_time DESC, pe.created_at DESC LIMIT 1),

              -- Priority 3: Earliest After
                (SELECT pe.value FROM primitive_events pe
                WHERE pe.stream_ref = dp.stream_ref
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
          calc.stream_ref,
          calc.event_time,
          calc.value,
          calc.delta_value,
          -- Calculate the change in indexed value. Handle potential division by zero if base_value is 0.
          CASE
              WHEN COALESCE(pbv.base_value, 0::numeric(36,18)) = 0::numeric(36,18) THEN 0::numeric(36,18) -- Or handle as error/null depending on requirements
              ELSE (calc.delta_value * 100::numeric(36,18) / pbv.base_value)::numeric(36,18)
          END AS delta_indexed_value
      FROM (
          SELECT stream_ref, event_time, value,
                  COALESCE(value - LAG(value) OVER (PARTITION BY stream_ref ORDER BY event_time), value)::numeric(36,18) AS delta_value
          FROM all_primitive_points
      ) calc
      JOIN primitive_base_values pbv -- Join to get the base value for normalization
          ON calc.stream_ref = pbv.stream_ref
      WHERE calc.delta_value != 0::numeric(36,18)
  ),

  first_value_times AS (
      SELECT
          stream_ref,
          MIN(event_time) as first_value_time
      FROM all_primitive_points
      GROUP BY stream_ref
  ),

  effective_weight_changes AS (
      SELECT
          pw.primitive_stream_ref as stream_ref,
          GREATEST(pw.group_sequence_start, fvt.first_value_time) AS event_time,
          pw.raw_weight AS weight_delta
      FROM primitive_weights pw
      INNER JOIN first_value_times fvt
          ON pw.primitive_stream_ref = fvt.stream_ref
      WHERE GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
        AND pw.raw_weight != 0::numeric(36,18)

      UNION ALL

      SELECT
          pw.primitive_stream_ref as stream_ref,
          pw.group_sequence_end + 1 AS event_time,
          -pw.raw_weight AS weight_delta
      FROM primitive_weights pw
      INNER JOIN first_value_times fvt
          ON pw.primitive_stream_ref = fvt.stream_ref
      WHERE 
          GREATEST(pw.group_sequence_start, fvt.first_value_time) <= pw.group_sequence_end
          AND pw.raw_weight != 0::numeric(36,18)
          -- don't emit closing delta for open interval
          AND pw.group_sequence_end < ($max_int8 - 1)
  ),

  -- Combine indexed value changes and weight changes.
  unified_events AS (
      SELECT
          pec.stream_ref,
          pec.event_time,
          pec.delta_indexed_value,  -- Use delta_indexed_value here
          0::numeric(36,18) AS weight_delta,
          1 AS event_type_priority -- Value changes first
      FROM primitive_event_changes pec

      UNION ALL

      SELECT
          ewc.stream_ref,
          ewc.event_time,
          0::numeric(36,18) AS delta_indexed_value, -- Zero indexed value change for weight events
          ewc.weight_delta,
          2 AS event_type_priority -- Weight changes second
      FROM effective_weight_changes ewc
  ),

  -- Calculate state timeline using indexed values.
  primitive_state_timeline AS (
      SELECT
          stream_ref,
          event_time,
          delta_indexed_value,
          weight_delta,
          -- Calculate indexed value and weight *before* this event
          COALESCE(LAG(indexed_value_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as indexed_value_before_event,
          COALESCE(LAG(weight_after_event, 1, 0::numeric(36,18)) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC), 0::numeric(36,18)) as weight_before_event
      FROM (
          SELECT
              stream_ref,
              event_time,
              delta_indexed_value,
              weight_delta,
              event_type_priority, -- Select for ordering
              -- Cumulative indexed value up to and including this event
              (SUM(delta_indexed_value) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as indexed_value_after_event,
              -- Cumulative weight up to and including this event
              (SUM(weight_delta) OVER (PARTITION BY stream_ref ORDER BY event_time ASC, event_type_priority ASC))::numeric(36,18) as weight_after_event
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


-- Returns the base value for a composed stream at or around base_time.
-- This is a helper function for get_record_composed_index readability.
CREATE OR REPLACE ACTION internal_get_base_value(
    $data_provider TEXT,
    $stream_id     TEXT,
    $effective_base_time     INT8,   -- already pre-resolved "effective base time"
    $effective_frozen_at     INT8,   -- created_at cutoff (can be NULL ⇢ infinity)
    $use_cache     BOOL              -- Whether to use cache (passed through to called functions)
) PRIVATE VIEW RETURNS (NUMERIC(36,18)) {
    -- doesn't check for access control, as it's private and not responsible for
    -- any access control checks

    -- Try to find an exact match at base_time
    $found_exact := FALSE;
    $exact_value NUMERIC(36,18);
    for $row in get_record_composed($data_provider, $stream_id, $effective_base_time, $effective_base_time, $effective_frozen_at, $use_cache) {
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
    for $row in get_last_record_composed($data_provider, $stream_id, $effective_base_time, $effective_frozen_at, $use_cache) {
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
    for $row in get_first_record_composed($data_provider, $stream_id, $effective_base_time, $effective_frozen_at, $use_cache) {
        $after_value := $row.value;
        $found_after := TRUE;
        break;
    }
    
    if $found_after {
        return $after_value;
    }

    -- if no value is found, return 1
    return 1::NUMERIC(36,18);
};