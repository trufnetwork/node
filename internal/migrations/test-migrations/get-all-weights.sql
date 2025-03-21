/**
 * get_all_weights_for_query: Returns all weights for substreams of a given stream.
 * This is used to get the influence of each substream for a given query.
 */
CREATE OR REPLACE ACTION get_all_weights_for_query(
    $data_provider TEXT,
    $stream_id TEXT,
    $from_time INT,
    $to_time INT
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT,
    start_time INT,
    end_time INT,
    weight NUMERIC(36,18)
) {
    return WITH RECURSIVE
      -- 1. Identify relevant taxonomy group_sequences for the target stream
      latest_group_sequence_before AS (
        SELECT MAX(start_time) AS start_time
        FROM taxonomies
        WHERE data_provider = $data_provider
          AND stream_id    = $stream_id
          AND disabled_at IS NULL
          AND start_time::INT <= $from_time::INT
      ),
      future_group_sequences AS (
        SELECT start_time
        FROM taxonomies
        WHERE data_provider = $data_provider
          AND stream_id    = $stream_id
          AND disabled_at IS NULL
          AND start_time::INT > $from_time::INT
          AND start_time::INT <= $to_time::INT  -- Only consider group_sequences within the query time range
      ),
      group_sequence_starts AS (
        SELECT start_time 
        FROM latest_group_sequence_before
        WHERE start_time IS NOT NULL  -- Only include if we found a group_sequence
        UNION 
        SELECT start_time FROM future_group_sequences
        UNION
        -- Include to_time+1 as a boundary to ensure proper clamping of intervals
        SELECT ($to_time::INT + 1)::INT AS start_time WHERE $to_time IS NOT NULL
      ),
      -- Handle the case where no group_sequence exists before or at from_time
      -- but there are future group_sequences - use the earliest future group_sequence
      earliest_future_group_sequence AS (
        SELECT MIN(start_time) AS start_time
        FROM taxonomies
        WHERE data_provider = $data_provider
          AND stream_id    = $stream_id
          AND disabled_at IS NULL
          AND start_time::INT > $from_time::INT
          AND start_time::INT <= $to_time::INT
      ),
      effective_group_sequences AS (
        SELECT start_time
        FROM group_sequence_starts
        UNION
        -- If no group_sequence before $from_time, use earliest future group_sequence (if any)
        SELECT start_time 
        FROM earliest_future_group_sequence
        WHERE start_time IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM latest_group_sequence_before WHERE start_time IS NOT NULL)
      ),
      -- 2. Compute main stream's group_sequence segments with end_time as next_group_sequence_start - 1
      main_group_sequences AS (
        SELECT 
          vs.start_time AS segment_start,
          -- Replace LEAST with CASE WHEN
          CASE
            WHEN (LEAD(vs.start_time) OVER (ORDER BY vs.start_time) - 1)::INT < $to_time::INT 
            THEN (LEAD(vs.start_time) OVER (ORDER BY vs.start_time) - 1)::INT
            ELSE $to_time::INT
          END AS segment_end
        FROM effective_group_sequences vs
        ORDER BY vs.start_time
      ),
      -- Get all taxonomy entries for the target stream's selected group_sequences (children of the main stream)
      main_taxonomy_entries AS (
        SELECT 
          t.data_provider,
          t.stream_id,
          t.child_data_provider,
          t.child_stream_id,
          t.weight::NUMERIC(36,18) AS weight,
          t.start_time,
          -- Replace COALESCE with CASE WHEN
          CASE
            WHEN m.segment_end IS NULL THEN $to_time::INT
            ELSE m.segment_end::INT
          END AS segment_end
        FROM taxonomies t
        JOIN main_group_sequences m 
          ON t.start_time::INT = m.segment_start::INT
        WHERE t.data_provider = $data_provider
          AND t.stream_id    = $stream_id
          AND t.disabled_at IS NULL
      ),
      -- Total weight of children for each parent stream in each group_sequence (for normalization)
      weight_sums AS (
        SELECT 
          data_provider, 
          stream_id, 
          start_time, 
          SUM(weight)::NUMERIC(36,18) AS total_weight
        FROM taxonomies
        WHERE disabled_at IS NULL
        GROUP BY data_provider, stream_id, start_time
      ),
      -- 3. Prepare all taxonomy entries (for all streams) with next_start and total_weight
      all_group_sequences AS (
        SELECT 
          data_provider,
          stream_id,
          start_time,
          LEAD(start_time) OVER (
            PARTITION BY data_provider, stream_id
            ORDER BY start_time
          ) AS next_start
        FROM taxonomies
        WHERE disabled_at IS NULL
      ),
      taxonomy_entries AS (
        SELECT 
          t.data_provider,
          t.stream_id,
          t.child_data_provider,
          t.child_stream_id,
          t.weight::NUMERIC(36,18) AS weight,
          t.start_time,
          v.next_start AS next_start,   -- if v.next_start is null then it remains null
          w.total_weight::NUMERIC(36,18) AS total_weight
        FROM taxonomies t
        LEFT JOIN all_group_sequences v
          ON t.data_provider = v.data_provider
         AND t.stream_id    = v.stream_id
         AND t.start_time   = v.start_time
        JOIN weight_sums w
          ON t.data_provider = w.data_provider
         AND t.stream_id    = w.stream_id
         AND t.start_time   = w.start_time
        WHERE t.disabled_at IS NULL
      ),
      -- 4. Recursive expansion of the taxonomy hierarchy to compute effective weights
      breakdown AS (
        -- Anchor: initial substreams of the target stream for each relevant main segment
        SELECT 
          me.child_data_provider AS data_provider,
          me.child_stream_id     AS stream_id,
          (me.weight::NUMERIC(36,18) / 
            -- We could use NULLIF, but it's not supported
            (CASE 
              WHEN w.total_weight::NUMERIC(36,18) = 0::NUMERIC(36,18) THEN NULL 
              ELSE w.total_weight::NUMERIC(36,18)
            END)
          )::NUMERIC(36,18) AS effective_weight,
          -- We could use GREATEST, but it's not supported
          CASE
            WHEN me.start_time::INT > $from_time::INT THEN me.start_time::INT
            ELSE $from_time::INT
          END AS start_time,
          -- We could use LEAST, but it's not supported
          CASE
            WHEN me.segment_end::INT < $to_time::INT THEN me.segment_end::INT
            ELSE $to_time::INT
          END AS end_time
        FROM main_taxonomy_entries me
        JOIN weight_sums w 
          ON me.data_provider = w.data_provider
         AND me.stream_id    = w.stream_id
         AND me.start_time   = w.start_time
        WHERE 
          -- We could use GREATEST, but it's not supported
          CASE 
            WHEN me.start_time::INT > $from_time::INT THEN me.start_time::INT
            ELSE $from_time::INT
          END <= $to_time::INT
        UNION ALL
        -- Recursive step: join each current node with its child taxonomy entries (if any)
        SELECT 
          te.child_data_provider AS data_provider,
          te.child_stream_id     AS stream_id,
          (parent.effective_weight::NUMERIC(36,18) * 
            (te.weight::NUMERIC(36,18) / 
              -- We could use NULLIF, but it's not supported
              (CASE
                WHEN te.total_weight::NUMERIC(36,18) = 0::NUMERIC(36,18) THEN NULL
                ELSE te.total_weight::NUMERIC(36,18)
              END)
            )::NUMERIC(36,18)
          )::NUMERIC(36,18) AS effective_weight,
          -- We could use GREATEST, but it's not supported
          CASE
            WHEN
              (CASE 
                WHEN parent.start_time::INT > te.start_time::INT THEN parent.start_time::INT
                ELSE te.start_time::INT
              END) > $from_time::INT
            THEN 
              (CASE 
                WHEN parent.start_time::INT > te.start_time::INT THEN parent.start_time::INT
                ELSE te.start_time::INT
              END)
            ELSE 
              $from_time::INT
          END AS start_time,
          -- We could use LEAST, but it's not supported
          CASE
            WHEN 
              (CASE 
                WHEN te.next_start IS NULL THEN parent.end_time::INT
                WHEN parent.end_time::INT <= (te.next_start - 1)::INT THEN parent.end_time::INT
                ELSE (te.next_start - 1)::INT
              END) < $to_time::INT
            THEN 
              (CASE 
                WHEN te.next_start IS NULL THEN parent.end_time::INT
                WHEN parent.end_time::INT <= (te.next_start - 1)::INT THEN parent.end_time::INT
                ELSE (te.next_start - 1)::INT
              END)
            ELSE 
              $to_time::INT
          END AS end_time
        FROM breakdown parent
        JOIN taxonomy_entries te
          ON te.data_provider = parent.data_provider
         AND te.stream_id    = parent.stream_id
         -- Child group_sequence must overlap the parent's current segment
         AND te.start_time::INT <= parent.end_time::INT
         AND (te.next_start IS NULL OR te.next_start::INT > parent.start_time::INT)
        WHERE parent.start_time::INT <= $to_time::INT -- Exclude segments that start after $to_time
          AND parent.end_time::INT >= $from_time::INT -- Exclude segments that end before $from_time
      ),
      -- 5. Filter to get only primitive substreams (no further children)
      primitive_weights AS (
        SELECT 
          data_provider, 
          stream_id, 
          start_time, 
          end_time, 
          effective_weight::NUMERIC(36,18) AS effective_weight
        FROM breakdown b
        WHERE NOT EXISTS (
          SELECT 1 FROM taxonomy_entries te
          WHERE te.data_provider = b.data_provider
            AND te.stream_id    = b.stream_id
            AND te.start_time::INT <= b.end_time::INT
            AND (te.next_start IS NULL OR te.next_start::INT > b.start_time::INT)
        )
        AND start_time::INT <= $to_time::INT -- Ensure we only return segments within our query range
        AND end_time::INT >= $from_time::INT
      )
    SELECT 
      data_provider,
      stream_id,
      start_time,
      end_time,
      effective_weight::NUMERIC(36,18) AS weight
    FROM primitive_weights
    WHERE start_time::INT <= end_time::INT -- Ensure valid time ranges
    ORDER BY start_time, data_provider, stream_id;
};
