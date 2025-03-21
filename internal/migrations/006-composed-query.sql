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

    -- Check permissions; raises error if unauthorized
    IF !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }

    -- Check compose permissions
    if !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
        ERROR('Not allowed to compose stream');
    }

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
       * effectively closes the old row’s interval.
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
        AND pe.event_time >= GREATEST(si.group_sequence_start, $effective_from)
        OR pe.event_time = (
            SELECT MAX(pe2.event_time)
            FROM primitive_events pe2
            WHERE pe2.data_provider = pe.data_provider
        AND pe2.stream_id     = pe.stream_id
              AND pe2.event_time   <= GREATEST(si.group_sequence_start, $effective_from)
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



/**
 * get_last_record_composed: Retrieves the most recent data point from a composed stream.
 * Uses the same hierarchy traversal and aggregation logic as get_record_composed.
 */
CREATE OR REPLACE ACTION get_last_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $before INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read permissions
    if !is_allowed_to_read_all($data_provider, $stream_id, @caller, NULL, $before) {
        ERROR('Not allowed to read stream');
    }
    
    $max_int8 INT8 := 9223372036854775000;  -- INT8 max for "infinity"
    $effective_before INT8 := COALESCE($before, $max_int8);
    $effective_frozen_at INT8 := COALESCE($frozen_at, $max_int8);
    
    -- Define constraints to improve readability
    if $before IS NULL {
        $before := $max_int8;
    }

    ERROR('not implemented');
;

/**
 * get_first_record_composed: Placeholder for finding first record in composed stream.
 * Will determine the first record based on child stream values and weights.
 */
CREATE OR REPLACE ACTION get_first_record_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $after INT8,
    $frozen_at INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read permissions
    if !is_allowed_to_read_all($data_provider, $stream_id, @caller, $after, NULL) {
        ERROR('Not allowed to read stream');
    }
    
    $max_int8 INT8 := 9223372036854775000;  -- INT8 max for "infinity"
    ERROR('not implemented');
    -- RETURN 
    -- WITH RECURSIVE

    -- -- Find taxonomy sequences that apply to our query
    -- selected_taxonomy_sequences AS (
    --     SELECT 
    --         t.data_provider,
    --         t.stream_id,
    --         t.start_time,
    --         t.sequence,
    --         ROW_NUMBER() OVER (PARTITION BY t.start_time ORDER BY t.sequence DESC) AS rn
    --     FROM taxonomies t
    --     WHERE t.disabled_at IS NULL
    --       AND t.data_provider = $data_provider
    --       AND t.stream_id = $stream_id
    --       AND ($after IS NULL OR t.start_time >= $after)
    -- ),

    -- -- Keep only the latest sequence for each time point
    -- latest_sequences AS (
    --     SELECT
    --         data_provider,
    --         stream_id,
    --         start_time,
    --         sequence
    --     FROM selected_taxonomy_sequences
    --     WHERE rn = 1
    -- ),

    -- -- Get the child stream references from selected taxonomy sequences
    -- all_taxonomies AS (
    --     SELECT
    --         t.data_provider,
    --         t.stream_id,
    --         t.start_time AS sequence_start,
    --         t.weight,
    --         t.sequence,
    --         t.child_data_provider,
    --         t.child_stream_id
    --     FROM taxonomies t
    --     JOIN latest_sequences lv
    --       ON t.data_provider = lv.data_provider
    --      AND t.stream_id = lv.stream_id
    --      AND t.start_time = lv.start_time
    --      AND t.sequence = lv.sequence
    -- ),

    -- -- Calculate validity periods for each taxonomy sequence
    -- main_sequences AS (
    --     SELECT 
    --         data_provider,
    --         stream_id,
    --         sequence_start,
    --         COALESCE(
    --             LEAD(sequence_start) OVER (
    --                 PARTITION BY data_provider, stream_id
    --                 ORDER BY sequence_start
    --             ) - 1,
    --             $max_int8
    --         ) AS sequence_end
    --     FROM all_taxonomies
    --     GROUP BY data_provider, stream_id, sequence_start
    -- ),

    -- -- Connect child streams with their validity periods
    -- main_direct_children AS (
    --     SELECT
    --         t.data_provider,
    --         t.stream_id,
    --         m.sequence_start,
    --         m.sequence_end,
    --         t.child_data_provider,
    --         t.child_stream_id,
    --         t.weight
    --     FROM all_taxonomies t
    --     JOIN main_sequences m
    --     ON t.data_provider = m.data_provider 
    --     AND t.stream_id = m.stream_id 
    --     AND t.sequence_start = m.sequence_start
    -- ),

    -- -- Recursively traverse the hierarchy to find all primitive streams
    -- hierarchy AS (
    --     -- Base case: direct children of target stream
    --     SELECT
    --         m.child_data_provider AS data_provider,
    --         m.child_stream_id AS stream_id,
    --         m.weight AS raw_weight,
    --         m.sequence_start AS sequence_start,
    --         m.sequence_end AS sequence_end
    --     FROM main_direct_children m
    --     WHERE m.data_provider = $data_provider
    --       AND m.stream_id = $stream_id

    --     UNION ALL

    --     -- Recursive step: follow each branch down to its leaves
    --     SELECT
    --         c.child_data_provider,
    --         c.child_stream_id,
    --         (parent.raw_weight * c.weight)::NUMERIC(36,18) AS raw_weight,
    --         GREATEST(parent.sequence_start, c.sequence_start) AS sequence_start,
    --         LEAST(parent.sequence_end, c.sequence_end) AS sequence_end
    --     FROM hierarchy parent
    --     INNER JOIN main_direct_children c
    --       ON c.data_provider = parent.data_provider
    --      AND c.stream_id = parent.stream_id
    --      -- Only follow connections with overlapping validity periods
    --      AND c.sequence_start <= parent.sequence_end
    --      AND c.sequence_end >= parent.sequence_start
    --     WHERE parent.sequence_start <= parent.sequence_end
    -- ),

    -- -- Filter to only leaf nodes (primitive streams) in the hierarchy
    -- primitive_weights AS (
    --     SELECT h.*
    --     FROM hierarchy h
    --     WHERE NOT EXISTS (
    --         SELECT 1
    --         FROM taxonomies tx
    --         WHERE tx.data_provider = h.data_provider
    --           AND tx.stream_id = h.stream_id
    --           AND tx.disabled_at IS NULL
    --           AND tx.start_time <= h.sequence_end
    --     )
    --     AND h.sequence_start <= h.sequence_end
    -- ),

    -- -- Extract unique primitive streams from the hierarchy
    -- effective_streams AS (
    --     SELECT DISTINCT data_provider, stream_id
    --     FROM primitive_weights
    -- ),

    -- -- Find the earliest event after the specified timestamp for each primitive stream
    -- earliest_events AS (
    --     SELECT 
    --         es.data_provider,
    --         es.stream_id,
    --         (
    --             SELECT MIN(pe.event_time) 
    --             FROM primitive_events pe
    --             WHERE pe.data_provider = es.data_provider
    --               AND pe.stream_id = es.stream_id
    --               AND ($after IS NULL OR pe.event_time >= $after)
    --               AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
    --               -- Ensure we get the latest created record at this event_time
    --               AND pe.created_at = (
    --                   SELECT MAX(created_at) 
    --                   FROM primitive_events 
    --                   WHERE data_provider = pe.data_provider
    --                     AND stream_id = pe.stream_id
    --                     AND event_time = pe.event_time
    --                     AND ($frozen_at IS NULL OR created_at <= $frozen_at)
    --               )
    --         ) AS event_time
    --     FROM effective_streams es
    -- ),

    -- -- Get the actual values for the earliest events
    -- primitive_values AS (
    --     SELECT 
    --         ee.data_provider,
    --         ee.stream_id,
    --         ee.event_time,
    --         pe.value
    --     FROM earliest_events ee
    --     JOIN primitive_events pe ON 
    --         pe.data_provider = ee.data_provider
    --         AND pe.stream_id = ee.stream_id
    --         AND pe.event_time = ee.event_time
    --         AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
    --     WHERE ee.event_time IS NOT NULL
    -- ),

    -- -- Apply weights based on taxonomy and time validity
    -- weighted_values AS (
    --     SELECT
    --         pv.event_time,
    --         (pv.value * pw.raw_weight)::NUMERIC(36,18) AS weighted_value,
    --         pw.raw_weight
    --     FROM primitive_values pv
    --     JOIN primitive_weights pw
    --       ON pv.data_provider = pw.data_provider
    --      AND pv.stream_id = pw.stream_id
    --      AND pv.event_time BETWEEN pw.sequence_start AND pw.sequence_end
    -- ),

    -- -- Aggregate the values for each time point
    -- aggregated AS (
    --     SELECT
    --         event_time,
    --         CASE WHEN SUM(raw_weight)::NUMERIC(36,18) = 0::NUMERIC(36,18)
    --              THEN 0::NUMERIC(36,18)
    --              ELSE SUM(weighted_value)::NUMERIC(36,18) / SUM(raw_weight)::NUMERIC(36,18)
    --         END AS value
    --     FROM weighted_values
    --     GROUP BY event_time
    -- ),
    
    -- -- Get the earliest aggregated value
    -- earliest_record AS (
    --     SELECT event_time, value
    --     FROM aggregated
    --     ORDER BY event_time ASC
    --     LIMIT 1
    -- )

    -- SELECT event_time, value::NUMERIC(36,18) FROM earliest_record;
};

/**
 * get_base_value_composed: Placeholder for finding base value in composed stream.
 * Will calculate base value from child streams at the specified time.
 */
CREATE OR REPLACE ACTION get_base_value_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $base_time INT8,
    $frozen_at INT8
) PRIVATE view returns (value NUMERIC(36,18)) {
    -- Check read permissions
    if !is_allowed_to_read_all($data_provider, $stream_id, @caller, NULL, $base_time) {
        ERROR('Not allowed to read stream');
    }
    
    -- If base_time is null, try to get it from metadata
    $effective_base_time INT8 := $base_time;
    if $effective_base_time IS NULL {
        -- First try to get base_time from metadata
        $found_metadata := FALSE;
        for $row in SELECT value_i 
            FROM metadata 
            WHERE data_provider = $data_provider 
            AND stream_id = $stream_id 
            AND metadata_key = 'default_base_time' 
            AND disabled_at IS NULL
            ORDER BY created_at DESC 
            LIMIT 1 {
            $effective_base_time := $row.value_i;
            $found_metadata := TRUE;
            break;
        }
        
        -- If still null after checking metadata, get the first ever record
        if !$found_metadata OR $effective_base_time IS NULL {
            $found_value NUMERIC(36,18);
            $found := FALSE;
            
            -- Execute the function and store results in variables
            $first_time INT8;
            $first_value NUMERIC(36,18);
            for $record in get_first_record_composed($data_provider, $stream_id, NULL, $frozen_at) {
                $first_time := $record.event_time;
                $first_value := $record.value;
                $found := TRUE;
                break;
            }
            
            if $found {
                return $first_value;
            } else {
                -- If no values found, error out
                ERROR('no base value found: no records in stream');
            }
        }
    }
    
    -- Try to find an exact match at base_time
    $found_exact := FALSE;
    $exact_value NUMERIC(36,18);
    for $row in get_record_composed($data_provider, $stream_id, $effective_base_time, $effective_base_time, $frozen_at) {
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
    for $row in get_last_record_composed($data_provider, $stream_id, $effective_base_time, $frozen_at) {
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
    for $row in get_first_record_composed($data_provider, $stream_id, $effective_base_time, $frozen_at) {
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

/**
 * get_index_composed: Placeholder for index calculation in composed streams.
 * Will calculate index values using the formula: (current_value/base_value)*100
 */
CREATE OR REPLACE ACTION get_index_composed(
    $data_provider TEXT,
    $stream_id TEXT,
    $from INT8,
    $to INT8,
    $frozen_at INT8,
    $base_time INT8
) PRIVATE view returns table(
    event_time INT8,
    value NUMERIC(36,18)
) {
    -- Check read permissions
    if !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }
    
    -- If base_time is not provided, try to get it from metadata
    $effective_base_time INT8 := $base_time;
    if $effective_base_time IS NULL {
        $found_metadata := FALSE;
        for $row in SELECT value_i 
            FROM metadata 
            WHERE data_provider = $data_provider 
            AND stream_id = $stream_id 
            AND metadata_key = 'default_base_time' 
            AND disabled_at IS NULL
            ORDER BY created_at DESC 
            LIMIT 1 {
            $effective_base_time := $row.value_i;
            $found_metadata := TRUE;
            break;
        }
    }

    -- Get the base value
    $base_value NUMERIC(36,18) := get_base_value_composed($data_provider, $stream_id, $effective_base_time, $frozen_at);

    -- Check if base value is zero to avoid division by zero
    if $base_value = 0::NUMERIC(36,18) {
        ERROR('base value is 0');
    }

    -- Calculate the index for each record through loop and RETURN NEXT
    -- This avoids nested SQL queries and uses proper action calling patterns
    for $record in get_record_composed($data_provider, $stream_id, $from, $to, $frozen_at) {
        $indexed_value NUMERIC(36,18) := ($record.value * 100::NUMERIC(36,18)) / $base_value;
        RETURN NEXT $record.event_time, $indexed_value;
    }
};

