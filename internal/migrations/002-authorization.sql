/**
 * is_allowed_to_read: Checks if a wallet can read a specific stream.
 * Considers stream visibility and explicit read permissions.
 */
CREATE OR REPLACE ACTION is_allowed_to_read(
    $data_provider TEXT,
    $stream_id TEXT,
    $wallet_address TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns (is_allowed BOOL) {
    $data_provider := LOWER($data_provider);
    $lowercase_wallet_address TEXT := LOWER($wallet_address);

    -- Extension agent has unrestricted read access for caching purposes
    if $lowercase_wallet_address = 'extension_agent' {
        return true;
    }

    -- Resolve stream ref (get_stream_id returns NULL if stream doesn't exist)
    $stream_ref := get_stream_id($data_provider, $stream_id);
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Delegate to private version (core logic)
    return is_allowed_to_read_core($stream_ref, $lowercase_wallet_address, $active_from, $active_to);
};

/**
 * is_allowed_to_read_core: Private version that uses stream ref directly.
 * Checks if a wallet can read a specific stream using stream reference.
 * This is the core implementation called by the public version.
 */
CREATE OR REPLACE ACTION is_allowed_to_read_core(
    $stream_ref INT,
    $wallet_address TEXT,
    $active_from INT,
    $active_to INT
) PRIVATE view returns (is_allowed BOOL) {
    $lowercase_wallet_address TEXT := LOWER($wallet_address);

    -- Extension agent has unrestricted read access for caching purposes
    if $lowercase_wallet_address = 'extension_agent' {
        return true;
    }

    -- if it's the owner, return true
    if is_stream_owner_core($stream_ref, $lowercase_wallet_address) {
        return true;
    }

    -- Check if the stream is private
    $read_visibility INT := get_latest_metadata_int_core($stream_ref, 'read_visibility');
    -- public by default
    if $read_visibility IS NULL {
        $read_visibility := 0;
    }

    if $read_visibility = 0 {
        -- short circuit if the stream is not private
        return true;
    }

    -- Check if the wallet is allowed to read the stream
    if get_latest_metadata_ref_core($stream_ref, 'allow_read_wallet', $lowercase_wallet_address) IS DISTINCT FROM NULL {
        -- wallet is allowed to read the stream
        return true;
    }

    -- none of the above authorized, so return false
    return false;
};


/**
 * is_allowed_to_compose_core: Private version that uses stream refs directly.
 * Checks if one stream can compose another stream using stream references.
 * This is the core implementation called by the public version.
 */
CREATE OR REPLACE ACTION is_allowed_to_compose_core(
    $stream_ref INT,
    $composing_stream_ref INT,
    $active_from INT,
    $active_to INT
) PRIVATE view returns (is_allowed BOOL) {
    -- check if it's from the same data provider
    $stream_owner := get_latest_metadata_ref_core($stream_ref, 'stream_owner', NULL);
    $composing_stream_owner := get_latest_metadata_ref_core($composing_stream_ref, 'stream_owner', NULL);
    if $stream_owner != $composing_stream_owner {
        ERROR('Composing stream must be from the same data provider');
    }

    -- Check if the stream is private
    $compose_visibility INT := get_latest_metadata_int_core($stream_ref, 'compose_visibility');
    -- public by default
    if $compose_visibility IS NULL {
        $compose_visibility := 0;
    }

    if $compose_visibility = 0 {
        -- the stream is public for composing
        return true;
    }

    -- Get composing stream ID for permission check
    $composing_stream_id TEXT;
    for $row in SELECT s.stream_id FROM streams s WHERE s.id = $composing_stream_ref LIMIT 1 {
        $composing_stream_id := $row.stream_id;
    }

    -- Check if the wallet is allowed to compose the stream
    if get_latest_metadata_ref_core($stream_ref, 'allow_compose_stream', $composing_stream_id) IS DISTINCT FROM NULL {
        -- wallet is allowed to compose the stream
        return true;
    }

    -- none of the above authorized, so return false
    return false;
};

/**
 * is_allowed_to_compose: Checks if a wallet can compose a stream.
 * Considers compose visibility and explicit compose permissions.
 */
CREATE OR REPLACE ACTION is_allowed_to_compose(
    $data_provider TEXT,
    $stream_id TEXT,
    $composing_data_provider TEXT,
    $composing_stream_id TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns (is_allowed BOOL) {
    $data_provider := LOWER($data_provider);
    $composing_data_provider := LOWER($composing_data_provider);

    -- Resolve stream refs (get_stream_id returns NULL if stream doesn't exist)
    $stream_ref := get_stream_id($data_provider, $stream_id);
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    $composing_stream_ref := get_stream_id($composing_data_provider, $composing_stream_id);
    IF $composing_stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $composing_data_provider || ' stream_id=' || $composing_stream_id);
    }

    -- Delegate to private version (core logic)
    return is_allowed_to_compose_core($stream_ref, $composing_stream_ref, $active_from, $active_to);
};

/**
 * is_allowed_to_read_all_core: Core private implementation for hierarchical read permissions.
 * Checks if a wallet can read a stream and all its substreams using stream reference.
 * This contains the complex recursive CTE logic for checking hierarchical permissions.
 */
CREATE OR REPLACE ACTION is_allowed_to_read_all_core(
    $stream_ref INT,
    $wallet_address TEXT,
    $active_from INT,
    $active_to INT
) PRIVATE view returns (is_allowed BOOL) {
    $wallet_address := LOWER($wallet_address);

    -- Extension agent has unrestricted read access for caching purposes
    if $wallet_address = 'extension_agent' {
        return true;
    }

    $max_int8 INT := 9223372036854775000;
    $effective_active_from INT := COALESCE($active_from, 0);
    $effective_active_to INT := COALESCE($active_to, $max_int8);

    -- by default, the wallet is allowed to read all
    $result BOOL := true;
    -- Check for missing or unauthorized substreams using recursive CTE
    for $counts in with recursive
        -- effective_taxonomies holds, for every parent-child link that is active,
        -- the rows that are considered effective given the time window.
    substreams AS (
      /*------------------------------------------------------------------
       * (1) Base Case: overshadow logic for ($data_provider, $stream_id).
       *     - For each distinct start_time, pick the row with the max group_sequence.
       *     - next_start is used to define [group_sequence_start, group_sequence_end].
       *------------------------------------------------------------------*/
      SELECT
          base.stream_ref,
          base.child_stream_ref,

          -- The interval during which this row is active:
          base.start_time            AS group_sequence_start,
          COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end

      FROM (
          SELECT
              t.stream_ref,
              t.child_stream_ref,
              t.start_time,
              t.group_sequence,
              MAX(t.group_sequence) OVER (
                  PARTITION BY t.stream_ref, t.start_time
              ) AS max_group_sequence
          FROM taxonomies t
          WHERE t.stream_ref    = $stream_ref
            AND t.disabled_at  IS NULL
            AND t.start_time   <= $effective_active_to
            AND t.start_time   >= COALESCE((
                  -- Find the most recent taxonomy at or before effective_active_from
                  SELECT t2.start_time
                  FROM taxonomies t2
                  WHERE t2.stream_ref = t.stream_ref
                    AND t2.disabled_at   IS NULL
                    AND t2.start_time   <= $effective_active_from
                  ORDER BY t2.start_time DESC, t2.group_sequence DESC
                  LIMIT 1
                ), 0
            )
      ) base
      JOIN (
          /* Distinct start_times for top-level (dp, sid), used for LEAD() */
          SELECT
              dt.stream_ref,
              dt.start_time,
              LEAD(dt.start_time) OVER (
                  PARTITION BY dt.stream_ref
                  ORDER BY dt.start_time
              ) AS next_start
          FROM (
              SELECT DISTINCT
                  t.stream_ref,
                  t.start_time
              FROM taxonomies t
              WHERE t.stream_ref = $stream_ref
                AND t.disabled_at   IS NULL
                AND t.start_time   <= $effective_active_to
                AND t.start_time   >= COALESCE((
                      SELECT t2.start_time
                      FROM taxonomies t2
                      WHERE t2.stream_ref = t.stream_ref
                        AND t2.disabled_at   IS NULL
                        AND t2.start_time   <= $effective_active_from
                      ORDER BY t2.start_time DESC, t2.group_sequence DESC
                      LIMIT 1
                    ), 0
                )
          ) dt
      ) ot
        ON base.stream_ref = ot.stream_ref
       AND base.start_time    = ot.start_time
      WHERE base.group_sequence = base.max_group_sequence

      UNION

      /*------------------------------------------------------------------
       * (2) Recursive Child-Level Overshadow:
       *     For each discovered child, gather overshadow rows for that child
       *     and produce intervals that overlap the parent's own active interval.
       *------------------------------------------------------------------*/
      SELECT
          -- promote the child to the parent
          parent.child_stream_ref,
          child.child_stream_ref,

          -- Intersection of parent's active interval and child's:
          GREATEST(parent.group_sequence_start, child.start_time)   AS group_sequence_start,
          LEAST   (parent.group_sequence_end,   child.group_sequence_end) AS group_sequence_end

      FROM substreams parent
      JOIN (
          /* Child overshadow logic, same pattern as above but for child dp/sid. */
          SELECT
              base.stream_ref,
              base.child_stream_ref,
              base.start_time,
              COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
          FROM (
              SELECT
                  t.stream_ref,
                  t.child_stream_ref,
                  t.start_time,
                  t.group_sequence,
                  MAX(t.group_sequence) OVER (
                      PARTITION BY t.stream_ref, t.start_time
                  ) AS max_group_sequence
              FROM taxonomies t
              WHERE t.disabled_at IS NULL
                AND t.start_time <= $effective_active_to
                AND t.start_time >= COALESCE((
                      -- Most recent taxonomy at or before effective_from
                      SELECT t2.start_time
                      FROM taxonomies t2
                      WHERE t2.stream_ref = t.stream_ref
                        AND t2.disabled_at   IS NULL
                        AND t2.start_time   <= $effective_active_from
                      ORDER BY t2.start_time DESC, t2.group_sequence DESC
                      LIMIT 1
                    ), 0
                )
          ) base
          JOIN (
              /* Distinct start_times at child level */
              SELECT
                  dt.stream_ref,
                  dt.start_time,
                  LEAD(dt.start_time) OVER (
                      PARTITION BY dt.stream_ref
                      ORDER BY dt.start_time
                  ) AS next_start
              FROM (
                  SELECT DISTINCT
                      t.stream_ref,
                      t.start_time
                  FROM taxonomies t
                  WHERE t.disabled_at   IS NULL
                    AND t.start_time   <= $effective_active_to
                    AND t.start_time   >= COALESCE((
                          SELECT t2.start_time
                          FROM taxonomies t2
                          WHERE t2.stream_ref = t.stream_ref
                            AND t2.disabled_at   IS NULL
                            AND t2.start_time   <= $effective_active_from
                          ORDER BY t2.start_time DESC, t2.group_sequence DESC
                          LIMIT 1
                        ), 0
                    )
              ) dt
          ) ot
            ON base.stream_ref = ot.stream_ref
           AND base.start_time    = ot.start_time
          WHERE base.group_sequence = base.max_group_sequence
      ) child
        ON child.stream_ref = parent.child_stream_ref
      /* Overlap check: child's interval must intersect parent's */
      WHERE child.start_time         <= parent.group_sequence_end
        AND child.group_sequence_end >= parent.group_sequence_start
    ),

    -- select distinct child union parent
    all_streams as (
    -- merge root stream with substreams
        SELECT DISTINCT child_stream_ref as stream_ref
        FROM substreams
        UNION
        SELECT $stream_ref as stream_ref
    ),

    -- Find substreams that don't exist
    inexisting_substreams as (
        SELECT 
            dp.address as data_provider, 
            s.stream_id 
        FROM all_streams rs
        LEFT JOIN streams s ON s.id = rs.stream_ref
        LEFT JOIN data_providers dp ON s.data_provider_id = dp.id
        WHERE s.id IS NULL
    ),

    -- Find substreams that are private
    private_substreams as (
        SELECT 
            dp.address as data_provider, 
            s.stream_id 
        FROM all_streams rs
        JOIN streams s ON s.id = rs.stream_ref
        JOIN data_providers dp ON s.data_provider_id = dp.id
        WHERE (
            SELECT value_i
            FROM metadata m
            WHERE m.stream_ref = rs.stream_ref
                AND m.metadata_key = 'read_visibility'
                AND m.disabled_at IS NULL
            ORDER BY m.created_at DESC
            LIMIT 1
        ) = 1  -- 1 indicates private visibility
    ),

    -- Find private streams where the wallet doesn't have access
    streams_without_permissions as (
        SELECT p.data_provider, p.stream_id 
        FROM private_substreams p
        -- check if it doesn't have explicit permission
        WHERE NOT EXISTS (
            SELECT 1
            FROM metadata m
            JOIN streams s ON m.stream_ref = s.id
            JOIN data_providers dp ON s.data_provider_id = dp.id
            WHERE dp.address = p.data_provider
                AND s.stream_id = p.stream_id
                AND m.metadata_key = 'allow_read_wallet'
                AND m.value_ref = $wallet_address
                AND m.disabled_at IS NULL
            LIMIT 1
        ) 
        -- check if it's not the owner
        AND NOT EXISTS (
            SELECT 1
            FROM metadata m
            JOIN streams s ON m.stream_ref = s.id
            JOIN data_providers dp ON s.data_provider_id = dp.id
            WHERE dp.address = p.data_provider
                AND s.stream_id = p.stream_id
                AND m.metadata_key = 'stream_owner'
                AND m.disabled_at IS NULL
                AND m.value_ref = $wallet_address
            LIMIT 1
        ) 
    )
    SELECT
        EXISTS (SELECT 1 FROM inexisting_substreams) AS has_missing,
        EXISTS (SELECT 1 FROM streams_without_permissions) AS has_unauthorized {
        -- error out if there's a missing streams
        if $counts.has_missing {
            ERROR('streams missing for stream with id=' || $stream_ref);
        }

        -- Return false if there are any unauthorized streams
        $result := NOT $counts.has_unauthorized;
    }

    return $result;
};

/**
 * is_allowed_to_read_all: Checks if a wallet can read a stream and all its substreams.
 * Uses recursive CTE to traverse taxonomy hierarchy and check permissions.
 */
CREATE OR REPLACE ACTION is_allowed_to_read_all(
    $data_provider TEXT,
    $stream_id TEXT,
    $wallet_address TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns (is_allowed BOOL) {
    $data_provider := LOWER($data_provider);
    $wallet_address := LOWER($wallet_address);

    -- Extension agent has unrestricted read access for caching purposes
    if $wallet_address = 'extension_agent' {
        return true;
    }

    -- Resolve stream ref (get_stream_id returns NULL if stream doesn't exist)
    $stream_ref := get_stream_id($data_provider, $stream_id);
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Delegate to private version (core logic)
    return is_allowed_to_read_all_core($stream_ref, $wallet_address, $active_from, $active_to);
};

/**
 * is_allowed_to_compose_all_core: Core private implementation for hierarchical compose permissions.
 * Checks if a stream can compose another stream and all its substreams using stream reference.
 * This contains the complex recursive CTE logic for checking hierarchical permissions.
 */
CREATE OR REPLACE ACTION is_allowed_to_compose_all_core(
    $stream_ref INT,
    $active_from INT,
    $active_to INT
) PRIVATE view returns (is_allowed BOOL) {
    $max_int8 INT := 9223372036854775000;
    $effective_active_from INT := COALESCE($active_from, 0);
    $effective_active_to INT := COALESCE($active_to, $max_int8);

    $result BOOL := true;
    -- Check for missing or unauthorized substreams using recursive CTE
    for $counts in with recursive
        -- this holds, for every parent-child link that is active,
        -- the rows that are considered effective given the time window.
        substreams AS (
        /*------------------------------------------------------------------
        * (1) Base Case: overshadow logic for ($data_provider, $stream_id).
        *     - For each distinct start_time, pick the row with the max group_sequence.
        *     - next_start is used to define [group_sequence_start, group_sequence_end].
        *------------------------------------------------------------------*/
        SELECT
            base.stream_ref,
            base.child_stream_ref,

            -- The interval during which this row is active:
            base.start_time            AS group_sequence_start,
            COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end

        FROM (
            SELECT
                t.stream_ref,
                t.child_stream_ref,
                t.start_time,
                t.group_sequence,
                MAX(t.group_sequence) OVER (
                    PARTITION BY t.stream_ref, t.start_time
                ) AS max_group_sequence
            FROM taxonomies t
            WHERE t.stream_ref = $stream_ref
                AND t.disabled_at   IS NULL
                AND t.start_time   <= $effective_active_to
                AND t.start_time   >= COALESCE((
                    -- Find the most recent taxonomy at or before effective_active_from
                    SELECT t2.start_time
                    FROM taxonomies t2
                    WHERE t2.stream_ref = t.stream_ref
                        AND t2.disabled_at   IS NULL
                        AND t2.start_time   <= $effective_active_from
                    ORDER BY t2.start_time DESC, t2.group_sequence DESC
                    LIMIT 1
                    ), 0
                )
        ) base
        JOIN (
            /* Distinct start_times for top-level (dp, sid), used for LEAD() */
            SELECT
                dt.stream_ref,
                dt.start_time,
                LEAD(dt.start_time) OVER (
                    PARTITION BY dt.stream_ref
                    ORDER BY dt.start_time
                ) AS next_start
            FROM (
                SELECT DISTINCT
                    t.stream_ref,
                    t.start_time
                FROM taxonomies t
                WHERE t.stream_ref = $stream_ref
                    AND t.disabled_at   IS NULL
                    AND t.start_time   <= $effective_active_to
                    AND t.start_time   >= COALESCE((
                        SELECT t2.start_time
                        FROM taxonomies t2
                        WHERE t2.stream_ref = t.stream_ref
                            AND t2.disabled_at   IS NULL
                            AND t2.start_time   <= $effective_active_from
                        ORDER BY t2.start_time DESC, t2.group_sequence DESC
                        LIMIT 1
                        ), 0
                    )
            ) dt
        ) ot
        ON base.stream_ref = ot.stream_ref
        AND base.start_time    = ot.start_time
        WHERE base.group_sequence = base.max_group_sequence

        UNION

        /*------------------------------------------------------------------
        * (2) Recursive Child-Level Overshadow:
        *     For each discovered child, gather overshadow rows for that child
        *     and produce intervals that overlap the parent's own active interval.
        *------------------------------------------------------------------*/
        SELECT
            -- promote the child to the parent
            parent.child_stream_ref,
            child.child_stream_ref,

            -- Intersection of parent's active interval and child's:
            GREATEST(parent.group_sequence_start, child.start_time)   AS group_sequence_start,
            LEAST   (parent.group_sequence_end,   child.group_sequence_end) AS group_sequence_end

        FROM substreams parent
        JOIN (
            /* Child overshadow logic, same pattern as above but for child dp/sid. */
            SELECT
                base.stream_ref,
                base.child_stream_ref,
                base.start_time,
                COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
            FROM (
                SELECT
                    t.stream_ref,
                    t.child_stream_ref,
                    t.start_time,
                    t.group_sequence,
                    MAX(t.group_sequence) OVER (
                        PARTITION BY t.stream_ref, t.start_time
                    ) AS max_group_sequence
                FROM taxonomies t
                WHERE t.disabled_at IS NULL
                    AND t.start_time <= $effective_active_to
                    AND t.start_time >= COALESCE((
                        -- Most recent taxonomy at or before effective_from
                        SELECT t2.start_time
                        FROM taxonomies t2
                        WHERE t2.stream_ref = t.stream_ref
                            AND t2.disabled_at   IS NULL
                            AND t2.start_time   <= $effective_active_from
                        ORDER BY t2.start_time DESC, t2.group_sequence DESC
                        LIMIT 1
                        ), 0
                    )
            ) base
            JOIN (
                /* Distinct start_times at child level */
                SELECT
                    dt.stream_ref,
                    dt.start_time,
                    LEAD(dt.start_time) OVER (
                        PARTITION BY dt.stream_ref
                        ORDER BY dt.start_time
                    ) AS next_start
                FROM (
                    SELECT DISTINCT
                        t.stream_ref,
                        t.start_time
                    FROM taxonomies t
                    WHERE t.disabled_at   IS NULL
                        AND t.start_time   <= $effective_active_to
                        AND t.start_time   >= COALESCE((
                            SELECT t2.start_time
                            FROM taxonomies t2
                            WHERE t2.stream_ref = t.stream_ref
                                AND t2.disabled_at   IS NULL
                                AND t2.start_time   <= $effective_active_from
                            ORDER BY t2.start_time DESC, t2.group_sequence DESC
                            LIMIT 1
                            ), 0
                        )
                ) dt
            ) ot
            ON base.stream_ref = ot.stream_ref
            AND base.start_time    = ot.start_time
            WHERE base.group_sequence = base.max_group_sequence
        ) child
            ON child.stream_ref = parent.child_stream_ref
        /* Overlap check: child's interval must intersect parent's */
        WHERE child.start_time         <= parent.group_sequence_end
            AND child.group_sequence_end >= parent.group_sequence_start
        ),
    
        parent_child_edges as (
            SELECT DISTINCT p.stream_ref, p.child_stream_ref
            FROM substreams p
        ),
        -- Check that all child streams exist.
        inexisting_substreams AS (
            SELECT DISTINCT p.child_stream_ref
            FROM parent_child_edges p
            LEFT JOIN streams s
              ON p.child_stream_ref = s.id
            WHERE s.id IS NULL
        ),
        -- For each edge, if the child is private, check that the child whitelists its parent.
        unauthorized_edges AS (
            SELECT p.stream_ref, p.child_stream_ref
            FROM parent_child_edges p
            JOIN streams parent_s ON p.stream_ref = parent_s.id
            JOIN data_providers parent_dp ON parent_s.data_provider_id = parent_dp.id
            JOIN streams child_s ON p.child_stream_ref = child_s.id
            JOIN data_providers child_dp ON child_s.data_provider_id = child_dp.id
            WHERE (
                SELECT value_i
                FROM metadata m
                WHERE m.stream_ref = p.child_stream_ref
                  AND m.metadata_key = 'compose_visibility'
                  AND m.disabled_at IS NULL
                ORDER BY m.created_at DESC
                LIMIT 1
            -- check if the child is a private stream (compose visibility is 1)
            ) = 1
            AND NOT EXISTS (
                SELECT 1
                FROM metadata m2
                WHERE m2.stream_ref = p.child_stream_ref
                  AND m2.metadata_key = 'allow_compose_stream'
                  AND m2.disabled_at IS NULL
                  AND m2.value_ref = parent_s.stream_id::text
                LIMIT 1
            )
            -- check if both aren't from the same owner, which could mean that they have permission by default
            AND (
                SELECT value_ref
                FROM metadata m3
                WHERE m3.stream_ref = p.child_stream_ref
                  AND m3.metadata_key = 'stream_owner'
                  AND m3.disabled_at IS NULL
                ORDER BY m3.created_at DESC
                LIMIT 1
            ) IS DISTINCT FROM (
                SELECT value_ref
                FROM metadata m4
                WHERE m4.stream_ref = p.stream_ref
                  AND m4.metadata_key = 'stream_owner'
                  AND m4.disabled_at IS NULL
                ORDER BY m4.created_at DESC
                LIMIT 1
            )
        )
        SELECT
            EXISTS (SELECT 1 FROM inexisting_substreams) AS has_missing,
            EXISTS (SELECT 1 FROM unauthorized_edges) AS has_unauthorized {
            -- error out if there's a missing streams
            if $counts.has_missing {
                ERROR('Missing child streams for stream with id=' || $stream_ref);
            }

            -- only authorized if there are no unauthorized edges
            $result := NOT $counts.has_unauthorized;
        }

        -- return if it's authorized or not
        return $result;
};

/**
 * is_allowed_to_compose_all: Checks if a wallet can compose a stream and all its substreams.
 * Uses recursive CTE to traverse taxonomy hierarchy and check permissions.
 */
CREATE OR REPLACE ACTION is_allowed_to_compose_all(
    $data_provider TEXT,
    $stream_id TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns (is_allowed BOOL) {
    $data_provider := LOWER($data_provider);

    -- Resolve stream ref (get_stream_id returns NULL if stream doesn't exist)
    $stream_ref := get_stream_id($data_provider, $stream_id);
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Delegate to private version (core logic)
    return is_allowed_to_compose_all_core($stream_ref, $active_from, $active_to);
};



/**
 * wallet_write_batch_core: Private batch version that uses stream refs directly.
 * Checks if a wallet can write to multiple streams using their stream references.
 * Returns false if any stream refs are null (indicating non-existent streams).
 * Returns true only if the wallet can write to all existing streams.
 */
CREATE OR REPLACE ACTION wallet_write_batch_core(
    $stream_refs INT[],
    $wallet TEXT
) PRIVATE VIEW RETURNS (result BOOL) {
    $lowercase_wallet TEXT := LOWER($wallet);

    -- Use UNNEST for efficient batch processing
    for $row in SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM UNNEST($stream_refs) AS t(stream_ref) WHERE t.stream_ref IS NULL
        ) THEN false
        ELSE NOT EXISTS (
            SELECT 1
            FROM UNNEST($stream_refs) AS t(stream_ref)
            LEFT JOIN (
                SELECT stream_ref, value_ref AS owner
                FROM (
                    SELECT
                        m.stream_ref,
                        m.value_ref,
                        ROW_NUMBER() OVER (
                            PARTITION BY m.stream_ref
                            ORDER BY m.created_at DESC, m.row_id DESC
                        ) AS rn
                    FROM metadata m
                    WHERE m.metadata_key = 'stream_owner'
                      AND m.disabled_at IS NULL
                ) latest_owner
                WHERE rn = 1
            ) o ON o.stream_ref = t.stream_ref
            LEFT JOIN (
                SELECT DISTINCT m.stream_ref
                FROM metadata m
                WHERE m.metadata_key = 'allow_write_wallet'
                  AND m.disabled_at IS NULL
                  AND m.value_ref = $lowercase_wallet
            ) ap ON ap.stream_ref = t.stream_ref
            WHERE t.stream_ref IS NOT NULL
              AND NOT (o.owner = $lowercase_wallet OR ap.stream_ref IS NOT NULL)
        )
    END AS result
    FROM (SELECT 1) dummy {
        return $row.result;
    }
    return false;
};

/**
 * is_wallet_allowed_to_write: Checks if a wallet can write to a stream.
 * Grants write access if wallet is stream owner or has explicit permission.
 */
CREATE OR REPLACE ACTION is_wallet_allowed_to_write(
    $data_provider TEXT,
    $stream_id TEXT,
    $wallet TEXT
) PUBLIC view returns (result bool) {
    $data_provider := LOWER($data_provider);
    $lower_wallet := LOWER($wallet);

    -- Resolve stream ref once
    $stream_ref := get_stream_id($data_provider, $stream_id);

    -- Check if the wallet is the stream owner
    if is_stream_owner_core($stream_ref, $lower_wallet) {
        return true;
    }

    -- Check if the wallet is explicitly allowed to write via metadata permissions (latest row)
    if get_latest_metadata_ref_core($stream_ref, 'allow_write_wallet', $lower_wallet) IS DISTINCT FROM NULL {
        return true;
    }

    return false;
};

/**
 * is_wallet_allowed_to_write_batch: Checks if a wallet can write to multiple streams.
 * Checks permission for each stream in the provided arrays and returns the valid streams.
 * Uses WITH RECURSIVE for efficient batch processing, avoiding action-loop anti-patterns.
 * Combines ownership and explicit permission checks in a single SQL context.
 */
CREATE OR REPLACE ACTION is_wallet_allowed_to_write_batch(
    $data_providers TEXT[],
    $stream_ids TEXT[],
    $wallet TEXT
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT,
    is_allowed BOOL
) {
    -- Lowercase wallet once
    $wallet := LOWER($wallet);
    if NOT check_ethereum_address($wallet) {
        ERROR('Invalid wallet address. Must be a valid Ethereum address: ' || $wallet);
    }

    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }

     -- Handle edge case of empty arrays gracefully
     if array_length($data_providers) = 0 {
        RETURN; -- Return empty table
     }

    -- Use UNNEST for efficient batch processing
    RETURN SELECT
        t.data_provider,
        t.stream_id,
        COALESCE(o.owner = $wallet, false)
        OR COALESCE(p.has_perm, false) AS is_allowed
    FROM (
        SELECT
            LOWER(dp)    AS data_provider,
            sid           AS stream_id
        FROM UNNEST($data_providers, $stream_ids) AS u(dp, sid)
    ) t
    LEFT JOIN (
        -- Precompute the latest owner per stream_ref, but only for our inputs
        SELECT
            dp.address       AS data_provider,
            s.stream_id      AS stream_id,
            latest.value_ref AS owner
        FROM (
            SELECT
                stream_ref,
                value_ref
            FROM (
                SELECT
                    m.stream_ref,
                    m.value_ref,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.stream_ref
                        ORDER BY m.created_at DESC, m.row_id DESC
                    ) AS rn
                FROM metadata m
                WHERE
                    m.metadata_key = 'stream_owner'
                    AND m.disabled_at IS NULL
                    -- only streams/providers in inputs
                    AND EXISTS (
                        SELECT 1
                        FROM (
                            SELECT LOWER(dp) AS data_provider, sid AS stream_id
                            FROM UNNEST($data_providers, $stream_ids) AS u(dp, sid)
                        ) i
                        JOIN streams si ON si.id = m.stream_ref
                        JOIN data_providers dpi ON dpi.id = si.data_provider_id
                        WHERE
                            dpi.address = i.data_provider
                            AND si.stream_id = i.stream_id
                    )
            ) x
            WHERE rn = 1
        ) latest
        JOIN streams s ON s.id = latest.stream_ref
        JOIN data_providers dp ON dp.id = s.data_provider_id
    ) o
      ON LOWER(t.data_provider) = o.data_provider
     AND t.stream_id          = o.stream_id
    LEFT JOIN (
        -- Precompute whether the wallet has write permission, but only for our inputs
        SELECT DISTINCT
            dp.address  AS data_provider,
            s.stream_id AS stream_id,
            true        AS has_perm
        FROM metadata m
        JOIN streams s ON s.id = m.stream_ref
        JOIN data_providers dp ON dp.id = s.data_provider_id
        WHERE
            m.metadata_key = 'allow_write_wallet'
            AND m.value_ref = $wallet
            AND m.disabled_at IS NULL
            -- only streams/providers in inputs
            AND EXISTS (
                SELECT 1
                FROM (
                    SELECT LOWER(dp) AS data_provider, sid AS stream_id
                    FROM UNNEST($data_providers, $stream_ids) AS u(dp, sid)
                ) i
                WHERE
                    dp.address  = i.data_provider
                    AND s.stream_id = i.stream_id
            )
    ) p
      ON LOWER(t.data_provider) = p.data_provider
     AND t.stream_id          = p.stream_id;
};

/**
 * has_write_permission_batch: Checks if a wallet has explicit write permission for multiple streams.
 * This doesn't check ownership, nor existence, only explicit permissions via allow_write_wallet metadata.
 * Returns a table indicating permission status for each stream.
 */
CREATE OR REPLACE ACTION has_write_permission_batch(
    $data_providers TEXT[],
    $stream_ids TEXT[],
    $wallet TEXT
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT,
    has_permission BOOL
) {
    -- Lowercase wallet once
    $wallet := LOWER($wallet);

    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }

    -- Use UNNEST for optimal performance with direct array processing
    RETURN SELECT
        t.data_provider,
        t.stream_id,
        EXISTS (
            SELECT 1
            FROM metadata m
            JOIN streams s ON m.stream_ref = s.id
            JOIN data_providers dp ON s.data_provider_id = dp.id
            WHERE dp.address = LOWER(t.data_provider)
              AND s.stream_id = t.stream_id
              AND m.metadata_key = 'allow_write_wallet'
              AND m.value_ref = $wallet
              AND m.disabled_at IS NULL
        ) AS has_permission
    FROM UNNEST($data_providers, $stream_ids) AS t(data_provider, stream_id);
};
