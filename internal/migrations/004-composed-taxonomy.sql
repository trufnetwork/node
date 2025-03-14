/**
 * insert_taxonomy: Creates a new taxonomy version for a composed stream.
 * Validates input arrays, increments version, and inserts child stream relationships.
 */
CREATE OR REPLACE ACTION insert_taxonomy(
    $data_provider TEXT,            -- The data provider of the parent stream.
    $stream_id TEXT,                -- The stream ID of the parent stream.
    $child_data_providers TEXT[],   -- The data providers of the child streams.
    $child_stream_ids TEXT[],       -- The stream IDs of the child streams.
    $weights NUMERIC(36,18)[],      -- The weights of the child streams.
    $start_date INT                 -- The start date of the taxonomy.
) PUBLIC view returns (result bool) {
    -- ensure it's a composed stream
    if is_primitive_stream($data_provider, $stream_id) == true {
        ERROR('stream is not a composed stream');
    }

    -- Ensure the wallet is allowed to write
    if is_wallet_allowed_to_write($data_provider, $stream_id, @caller) == false {
        ERROR('wallet not allowed to write');
    }
 
    -- Determine the number of child records provided.
    $num_children := array_length($child_stream_ids);

    -- Validate that all child arrays have the same length.
    if $num_children IS NULL OR $num_children == 0 OR
    $num_children != array_length($child_data_providers) OR
    $num_children != array_length($weights) {
        error('All child arrays must be of the same length');
    }

    -- Retrieve the current version for this parent and increment it by 1.
    $new_version := get_current_version($data_provider, $stream_id, true) + 1;

    FOR $i IN 1..$num_children {
        $child_data_provider_value := $child_data_providers[$i];
        $child_stream_id_value := $child_stream_ids[$i];
        $weight_value := $weights[$i];

        INSERT INTO taxonomies (
            data_provider,
            stream_id,
            taxonomy_id,
            child_data_provider,
            child_stream_id,
            weight,
            created_at,
            disabled_at,
            version,
            start_time
        ) VALUES (
            $data_provider,
            $stream_id,
            uuid_generate_kwil(@txid||$i::TEXT), -- Generate a new UUID for the taxonomy.
            $child_data_provider_value,
            $child_stream_id_value,
            $weight_value,
            @height,             -- Use the current block height for created_at.
            NULL,               -- New record is active.
            $new_version,          -- Use the new version for all child records.
            $start_date          -- Start date of the taxonomy.
        );
    }
    return true;
};

/**
 * get_current_version: Helper to find the latest taxonomy version.
 * When $show_disabled is false, only active (non-disabled) records are considered.
 */
CREATE OR REPLACE ACTION get_current_version(
    $data_provider TEXT,
    $stream_id TEXT,
    $show_disabled bool
) private view returns (result int) {
    if $show_disabled == false {
        for $row in SELECT version
        FROM taxonomies
        WHERE data_provider = $data_provider
        AND stream_id = $stream_id
        AND disabled_at IS NULL
        ORDER BY version DESC
        LIMIT 1 {
            return $row.version;
        }
    } else {
        for $row in SELECT version
        FROM taxonomies
        WHERE data_provider = $data_provider
        AND stream_id = $stream_id
        ORDER BY version DESC
        LIMIT 1 {
            return $row.version;
        }
    }
    return 0;
};

CREATE OR REPLACE ACTION describe_taxonomies(
    $data_provider TEXT,    -- Parent data provider
    $stream_id TEXT,        -- Parent stream id
    $latest_version BOOL    -- If true, only the latest (active) version is returned
) PUBLIC view returns table(
    data_provider TEXT,         -- Parent data provider
    stream_id TEXT,             -- Parent stream id
    child_data_provider TEXT,   -- Child data provider
    child_stream_id TEXT,       -- Child stream id
    weight NUMERIC(36,18),
    created_at INT,
    version INT,
    start_date INT             -- Aliased from start_time
) {
    if $latest_version == true {
        $version := get_current_version($data_provider, $stream_id, false);
        return SELECT
            t.data_provider,
            t.stream_id,
            t.child_data_provider,
            t.child_stream_id,
            t.weight,
            t.created_at,
            t.version,
            t.start_time AS start_date
        FROM taxonomies t
        WHERE t.disabled_at IS NULL
            AND t.data_provider = $data_provider
            AND t.stream_id = $stream_id
            AND t.version = $version
        ORDER BY t.created_at DESC;
    } else {
        return SELECT
            t.data_provider,
            t.stream_id,
            t.child_data_provider,
            t.child_stream_id,
            t.weight,
            t.created_at,
            t.version,
            t.start_time AS start_date
        FROM taxonomies t
        WHERE t.disabled_at IS NULL
            AND t.data_provider = $data_provider
            AND t.stream_id = $stream_id
        ORDER BY t.version DESC;
    }
};

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
      -- 1. Identify relevant taxonomy versions for the target stream
      latest_version_before AS (
        SELECT MAX(start_time) AS start_time
        FROM taxonomies
        WHERE data_provider = $data_provider
          AND stream_id    = $stream_id
          AND disabled_at IS NULL
          AND start_time::INT <= $from_time::INT
      ),
      future_versions AS (
        SELECT start_time
        FROM taxonomies
        WHERE data_provider = $data_provider
          AND stream_id    = $stream_id
          AND disabled_at IS NULL
          AND start_time::INT > $from_time::INT
          AND start_time::INT <= $to_time::INT  -- Only consider versions within the query time range
      ),
      version_starts AS (
        SELECT start_time 
        FROM latest_version_before
        WHERE start_time IS NOT NULL  -- Only include if we found a version
        UNION 
        SELECT start_time FROM future_versions
        UNION
        -- Include to_time+1 as a boundary to ensure proper clamping of intervals
        SELECT ($to_time::INT + 1)::INT AS start_time WHERE $to_time IS NOT NULL
      ),
      -- Handle the case where no version exists before or at from_time
      -- but there are future versions - use the earliest future version
      earliest_future_version AS (
        SELECT MIN(start_time) AS start_time
        FROM taxonomies
        WHERE data_provider = $data_provider
          AND stream_id    = $stream_id
          AND disabled_at IS NULL
          AND start_time::INT > $from_time::INT
          AND start_time::INT <= $to_time::INT
      ),
      effective_versions AS (
        SELECT start_time
        FROM version_starts
        UNION
        -- If no version before $from_time, use earliest future version (if any)
        SELECT start_time 
        FROM earliest_future_version
        WHERE start_time IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM latest_version_before WHERE start_time IS NOT NULL)
      ),
      -- 2. Compute main stream's version segments with end_time as next_version_start - 1
      main_versions AS (
        SELECT 
          vs.start_time AS segment_start,
          -- Replace LEAST with CASE WHEN
          CASE
            WHEN (LEAD(vs.start_time) OVER (ORDER BY vs.start_time) - 1)::INT < $to_time::INT 
            THEN (LEAD(vs.start_time) OVER (ORDER BY vs.start_time) - 1)::INT
            ELSE $to_time::INT
          END AS segment_end
        FROM effective_versions vs
        ORDER BY vs.start_time
      ),
      -- Get all taxonomy entries for the target stream's selected versions (children of the main stream)
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
        JOIN main_versions m 
          ON t.start_time::INT = m.segment_start::INT
        WHERE t.data_provider = $data_provider
          AND t.stream_id    = $stream_id
          AND t.disabled_at IS NULL
      ),
      -- Total weight of children for each parent stream in each version (for normalization)
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
      all_versions AS (
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
        LEFT JOIN all_versions v
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
         -- Child version must overlap the parent's current segment
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
