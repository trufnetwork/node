CREATE OR REPLACE ACTION get_record_composed(
    $data_provider TEXT,  -- Stream owner
    $stream_id TEXT,      -- Target composed stream
    $from INT8,           -- Start of time range (inclusive)
    $to INT8,             -- End of time range (inclusive)
    $frozen_at INT8       -- sequence cutoff timestamp
) PRIVATE VIEW
RETURNS TABLE(
    event_time INT8,
    value NUMERIC(36,18)
)  {
    -- Define constants
    $max_int8 := 9223372036854775000;  -- INT8 max for "infinity"
    $effective_from := COALESCE($from, 0);
    $effective_to := COALESCE($to, $max_int8);

    IF $from IS NOT NULL AND $to IS NOT NULL AND $from > $to {
        ERROR(format('from: %s > to: %s', $from, $to));
    }

    -- Check read permissions
    IF !is_allowed_to_read_all($data_provider, $stream_id, @caller, $from, $to) {
        ERROR('Not allowed to read stream');
    }

    -- Check compose permissions
    if !is_allowed_to_compose_all($data_provider, $stream_id, $from, $to) {
        ERROR('Not allowed to compose stream');
    }

    RETURN WITH RECURSIVE

 hierarchy AS (

  /*--------------------------------------------------------------------
   * 1) Base Case (Parent-Level Versioning)
   *
   * We gather taxonomies for the PARENT (data_provider, stream_id)
   * in the relevant [anchor, $effective_to] range.
   *
   * Here we partition ONLY by (data_provider, stream_id) when doing LEAD().
   * => That ensures a new start_time fully overshadows the old version for
   *    the entire parent from that time forward.
   *--------------------------------------------------------------------*/
  SELECT
    base.data_provider as parent_data_provider,
    base.stream_id as parent_stream_id,
    base.child_data_provider,
    base.child_stream_id,
    base.weight as raw_weight,
    base.start_time AS group_sequence_start,

    -- (CHANGED IN BASE CASE)
    -- Next parent's start_time is found by partitioning only on (parent_data_provider, parent_stream_id).
    -- That means any new row at t.start_time => overshadow old parent version from that time forward.
    COALESCE(
      LEAD(base.start_time) OVER (
        PARTITION BY base.data_provider, base.stream_id
        ORDER BY base.start_time
      ),
      $max_int8
    ) - 1 AS group_sequence_end

  FROM (
    SELECT
      t.data_provider,
      t.stream_id,
      t.child_data_provider,
      t.child_stream_id,
      t.start_time,
      t.group_sequence,
      t.weight,
      -- overshadow older group_sequence for the same parent + same start_time
      MAX(t.group_sequence) OVER (
        PARTITION BY t.data_provider, t.stream_id, t.start_time
      ) AS max_group_sequence,
      -- get the next start_time for the group_sequence
      LEAD(t.start_time) OVER (
        PARTITION BY t.data_provider, t.stream_id, t.group_sequence
        ORDER BY t.start_time
      ) AS next_start

    FROM taxonomies t
    WHERE t.data_provider = $data_provider
      AND t.stream_id     = $stream_id
      AND t.disabled_at   IS NULL
      AND t.start_time <= $effective_to
      AND t.start_time >= COALESCE(
            (
              SELECT t2.start_time
              FROM taxonomies t2
              WHERE t2.data_provider = $data_provider
                AND t2.stream_id = $stream_id
                AND t2.disabled_at IS NULL
                AND t2.start_time < $effective_from
              ORDER BY t2.start_time DESC, t2.group_sequence DESC
              LIMIT 1
            ),
            0
        )
  ) base
  WHERE base.group_sequence = base.max_group_sequence


  /*--------------------------------------------------------------------
   * 2) Recursive Step (Child-Level Versioning)
   *
   * For each child discovered, look up that child's own taxonomies
   * and overshadow older versions for that *child*.
   * => Partition by (data_provider, stream_id, child_data_provider, child_stream_id).
   *    This way, if you do multiple changes for the same child, older ones overshadow.
   *--------------------------------------------------------------------*/
  UNION ALL

  SELECT
    parent.parent_data_provider,
    parent.parent_stream_id,
    child.child_data_provider,
    child.child_stream_id,
    (parent.raw_weight * child.weight)::NUMERIC(36,18) AS raw_weight,

    GREATEST(parent.group_sequence_start, child.start_time) AS group_sequence_start,
    LEAST(parent.group_sequence_end, child.group_sequence_end) AS group_sequence_end

  FROM hierarchy parent
  JOIN (
    /* 2a) Subselect with window functions to label the child's taxonomies */
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
      ) AS max_group_sequence,
      -- get the next start_time for the group_sequence
      LEAD(t.start_time) OVER (
        PARTITION BY t.data_provider, t.stream_id, t.group_sequence
        ORDER BY t.start_time
      ) AS next_start,

      COALESCE(
        LEAD(t.start_time) OVER (
          PARTITION BY t.data_provider, t.stream_id, t.child_data_provider, t.child_stream_id
          ORDER BY t.start_time
        ),
        $max_int8
      ) - 1 AS group_sequence_end

    FROM taxonomies t
    WHERE t.disabled_at IS NULL
      AND t.start_time <= $effective_to
      AND t.start_time >= COALESCE(
            (
              SELECT t2.start_time
              FROM taxonomies t2
              WHERE t2.data_provider = $data_provider
                AND t2.stream_id = $stream_id
                AND t2.disabled_at IS NULL
                AND t2.start_time < $effective_from
              ORDER BY t2.start_time DESC, t2.group_sequence DESC
              LIMIT 1
            ),
            0
        )
  ) child
    ON child.data_provider = parent.child_data_provider
   AND child.stream_id     = parent.child_stream_id

  WHERE child.group_sequence = child.max_group_sequence

    -- Intersect validity: child's start_time must overlap parent's range
    AND child.start_time <= parent.group_sequence_end
    AND (COALESCE(child.next_start, $max_int8) - 1) >= parent.group_sequence_start

)  -- end of CTE 'hierarchy'
,

    -- Filter to only leaf nodes (primitive streams) in the hierarchy
    primitive_weights AS (
        SELECT 
            h.child_data_provider as data_provider,
            h.child_stream_id as stream_id,
            h.raw_weight,
            h.group_sequence_start,
            h.group_sequence_end
        FROM hierarchy h
        WHERE EXISTS (
            SELECT 1
            FROM streams s
            WHERE s.data_provider = h.child_data_provider
              AND s.stream_id = h.child_stream_id
              AND s.stream_type = 'primitive'
        )
    ),

    -- Extract unique primitive streams from the hierarchy
    effective_streams AS (
        SELECT DISTINCT data_provider, stream_id
        FROM primitive_weights
    ),

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
    
    -- Detect group boundaries using lag
    group_boundaries AS (
        SELECT
            data_provider,
            stream_id,
            group_sequence_start,
            group_sequence_end,
            rn,
            CASE
                WHEN rn = 1 THEN 1
                WHEN group_sequence_start > LAG(group_sequence_end) OVER (
                    PARTITION BY data_provider, stream_id
                    ORDER BY group_sequence_start
                ) + 1 THEN 1
                ELSE 0
            END AS is_new_group
        FROM ordered_intervals
    ),
    
    -- Assign group IDs
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
    
    -- Aggregate by group to get the merged intervals
    stream_intervals AS (
        SELECT
            data_provider,
            stream_id,
            MIN(group_sequence_start) AS group_sequence_start,
            MAX(group_sequence_end) AS group_sequence_end
        FROM groups
        GROUP BY data_provider, stream_id, group_id
    ),

    -- Pre-filter to only the events needed for calculation
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
         AND pe.stream_id = si.stream_id
        WHERE ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
          AND pe.event_time <= LEAST(si.group_sequence_end, $effective_to)
          AND (
              pe.event_time >= GREATEST(si.group_sequence_start, $effective_from)
              OR 
              pe.event_time = (
                 SELECT MAX(pe2.event_time)
                 FROM primitive_events pe2
                 WHERE pe2.data_provider = pe.data_provider
                   AND pe2.stream_id = pe.stream_id
                   AND pe2.event_time <= $effective_from
              )
          )
    ),

    -- Get only the latest group_sequence of each event
    latest_events AS (
        SELECT data_provider, stream_id, event_time, value
        FROM relevant_events
        WHERE rn = 1
    ),

    -- get all distinct event times from the latest events
    all_event_times AS (
        SELECT DISTINCT event_time FROM latest_events
        UNION
        
        -- explicitly include taxonomy weight change points
        SELECT DISTINCT group_sequence_start
        FROM primitive_weights
    ),

    cleaned_event_times AS (
        SELECT DISTINCT event_time
        FROM all_event_times
        WHERE event_time > $effective_from
        
        UNION
        
        SELECT event_time FROM (
            SELECT DISTINCT event_time
            FROM all_event_times
            WHERE event_time <= $effective_from
            ORDER BY event_time DESC
            LIMIT 1
        ) as anchor_event
    ),

    latest_event_times AS (
        SELECT
            re.event_time,
            es.data_provider,
            es.stream_id,
            MAX(le.event_time) AS latest_event_time
        FROM cleaned_event_times re
        JOIN effective_streams es ON 1=1 -- cross join
         LEFT JOIN latest_events le
          ON le.data_provider = es.data_provider
         AND le.stream_id = es.stream_id
         AND le.event_time <= re.event_time
        WHERE (re.event_time >= $effective_from OR re.event_time = (
              SELECT MAX(event_time) 
              FROM cleaned_event_times 
              WHERE event_time < $effective_from
            ))
          AND ($to IS NULL OR re.event_time <= $to)
        GROUP BY re.event_time, es.data_provider, es.stream_id
    ),

    stream_values AS (
        SELECT
            let.event_time,
            let.data_provider,
            let.stream_id,
            le.value
        FROM latest_event_times let
        LEFT JOIN latest_events le
          ON le.data_provider = let.data_provider
         AND le.stream_id = let.stream_id
         AND le.event_time = let.latest_event_time
    ),

    weighted_values AS (
        SELECT
            sv.event_time,
            (sv.value * pw.raw_weight)::NUMERIC(36,18) AS weighted_value,
            pw.raw_weight
        FROM stream_values sv
        JOIN primitive_weights pw
          ON sv.data_provider = pw.data_provider
         AND sv.stream_id = pw.stream_id
         AND sv.event_time BETWEEN pw.group_sequence_start AND pw.group_sequence_end
        WHERE sv.value IS NOT NULL
    ),

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
    
    -- Define constraints to improve readability
    if $before IS NULL {
        $before := $max_int8;
    }

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
    --       AND t.start_time <= $before
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
    --     WHERE EXISTS (
    --         SELECT 1
    --         FROM streams s
    --         WHERE s.data_provider = h.parent_data_provider
    --           AND s.stream_id = h.parent_stream_id
    --           AND s.stream_type = 'primitive'
    --     )
    -- ),

    -- -- Extract unique primitive streams from the hierarchy
    -- effective_streams AS (
    --     SELECT DISTINCT data_provider, stream_id
    --     FROM primitive_weights
    -- ),

    -- -- Find the latest event before the specified timestamp for each primitive stream
    -- latest_events AS (
    --     SELECT 
    --         es.data_provider,
    --         es.stream_id,
    --         (
    --             SELECT MAX(pe.event_time) 
    --             FROM primitive_events pe
    --             WHERE pe.data_provider = es.data_provider
    --               AND pe.stream_id = es.stream_id
    --               AND pe.event_time < $before
    --               AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
    --               -- Ensure we get the latest created record at this event_time
    --               AND pe.created_at = (
    --                   SELECT MAX(created_at) 
    --                   FROM primitive_events 
    --                   WHERE data_provider = es.data_provider
    --                     AND stream_id = es.stream_id
    --                     AND event_time = pe.event_time
    --                     AND ($frozen_at IS NULL OR created_at <= $frozen_at)
    --               )
    --         ) AS event_time
    --     FROM effective_streams es
    -- ),

    -- -- Get the actual values for the latest events
    -- primitive_values AS (
    --     SELECT 
    --         le.data_provider,
    --         le.stream_id,
    --         le.event_time,
    --         pe.value
    --     FROM latest_events le
    --     JOIN primitive_events pe ON 
    --         pe.data_provider = le.data_provider
    --         AND pe.stream_id = le.stream_id
    --         AND pe.event_time = le.event_time
    --         AND ($frozen_at IS NULL OR pe.created_at <= $frozen_at)
    --     WHERE le.event_time IS NOT NULL
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
    
    -- -- Get the latest aggregated value
    -- latest_record AS (
    --     SELECT event_time, value
    --     FROM aggregated
    --     ORDER BY event_time DESC
    --     LIMIT 1
    -- )

    -- SELECT event_time, value::NUMERIC(36,18) FROM latest_record;
};

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

