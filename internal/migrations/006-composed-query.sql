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

          -- Intersection: child's start_time must overlap parent's interval
          GREATEST(parent.group_sequence_start, child.start_time)   AS group_sequence_start,
          LEAST(parent.group_sequence_end,   child.group_sequence_end) AS group_sequence_end
      FROM hierarchy parent
      JOIN (
          /* 2a) Same "distinct start_time" fix at child level */
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
                      -- Lower bound at or before $effective_from
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
              /* Distinct start_times again, child-level */
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
      -- Overlap check: child's range must intersect parent's range
      WHERE child.start_time         <= parent.group_sequence_end
        AND child.group_sequence_end >= parent.group_sequence_start
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
          le.value
      FROM latest_event_times let
      LEFT JOIN requested_primitive_records le
        ON le.data_provider  = let.data_provider
       AND le.stream_id      = let.stream_id
       AND le.event_time     = let.latest_event_time
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

          -- Intersection: child's start_time must overlap parent's interval
          GREATEST(parent.group_sequence_start, child.start_time)   AS group_sequence_start,
          LEAST(parent.group_sequence_end,   child.group_sequence_end) AS group_sequence_end
      FROM hierarchy parent
      JOIN (
          /* 2a) Same "distinct start_time" fix at child level */
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
                      -- Lower bound at or before $effective_from
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
              /* Distinct start_times again, child-level */
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
      -- Overlap check: child's range must intersect parent's range
      WHERE child.start_time         <= parent.group_sequence_end
        AND child.group_sequence_end >= parent.group_sequence_start
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
