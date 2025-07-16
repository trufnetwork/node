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
    if $wallet_address = 'extension_agent' {
        return true;
    }
    
    -- Check if the stream exists
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }
    -- if it's the owner, return true
    if is_stream_owner($data_provider, $stream_id, $wallet_address) {
        return true;
    }

    -- Check if the stream is private
    $read_visibility INT := get_latest_metadata_int($data_provider, $stream_id, 'read_visibility');
    -- public by default
    if $read_visibility IS NULL {
        $read_visibility := 0;
    }

    if $read_visibility = 0 {
        -- short circuit if the stream is not private
        return true;
    }

    -- Check if the wallet is allowed to read the stream
    if get_latest_metadata_ref($data_provider, $stream_id, 'allow_read_wallet', $lowercase_wallet_address) IS DISTINCT FROM NULL {
        -- wallet is allowed to read the stream
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
    -- Check if the stream exists
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }
    if !stream_exists($composing_data_provider, $composing_stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $child_stream_id);
    }
    
    -- check if it's from the same data provider
    $stream_owner := get_latest_metadata_ref($data_provider, $stream_id, 'stream_owner', NULL);
    $composing_stream_owner := get_latest_metadata_ref($composing_data_provider, $composing_stream_id, 'stream_owner', NULL);
    if $stream_owner != $composing_stream_owner {
        ERROR('Composing stream must be from the same data provider: data_provider=' || $data_provider || ' composing_data_provider=' || $composing_data_provider);
    }

    -- Check if the stream is private
    $compose_visibility INT := get_latest_metadata_int($data_provider, $stream_id, 'compose_visibility');
    -- public by default
    if $compose_visibility IS NULL {
        $compose_visibility := 0;
    }

    if $compose_visibility = 0 {
        -- the stream is public for composing
        return true;
    }

    -- Check if the wallet is allowed to compose the stream
    if get_latest_metadata_ref($data_provider, $stream_id, 'allow_compose_stream', $composing_stream_id) IS DISTINCT FROM NULL {
        -- wallet is allowed to compose the stream
        return true;
    }

    -- none of the above authorized, so return false
    return false;
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

    -- Check if the stream exists
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    $max_int8 INT := 9223372036854775000;
    $effective_active_from INT := COALESCE($active_from, 0);
    $effective_active_to INT := COALESCE($active_to, $max_int8);
    $stream_ref := get_stream_id($data_provider, $stream_id);

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
        SELECT (
            SELECT s.id 
            FROM streams s
            JOIN data_providers dp ON s.data_provider_id = dp.id
            WHERE dp.address = $data_provider AND s.stream_id = $stream_id
        ) as stream_ref
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
                AND LOWER(m.value_ref) = LOWER($wallet_address)
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
                AND LOWER(m.value_ref) = LOWER($wallet_address)
            LIMIT 1
        ) 
    )
    SELECT 
        (SELECT COUNT(*) FROM inexisting_substreams) AS missing_count,
        (SELECT COUNT(*) FROM streams_without_permissions) AS unauthorized_count {
        -- error out if there's a missing streams
        if $counts.missing_count > 0 {
            ERROR('streams missing for stream: data_provider=' || $data_provider || ' stream_id=' || $stream_id || ' missing_count=' || $counts.missing_count::TEXT);
        }

        -- Return false if there are any unauthorized streams
        $result := $counts.unauthorized_count = 0;
    }
    
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

    -- Check if the stream exists
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }
    
    $max_int8 INT := 9223372036854775000;
    $effective_active_from INT := COALESCE($active_from, 0);
    $effective_active_to INT := COALESCE($active_to, $max_int8);
    $stream_ref := get_stream_id($data_provider, $stream_id);

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
            (SELECT COUNT(*) FROM inexisting_substreams) AS missing_count,
            (SELECT COUNT(*) FROM unauthorized_edges) AS unauthorized_count {
            -- error out if there's a missing streams
            if $counts.missing_count > 0 {
                ERROR('Missing child streams for stream: data_provider=' || $data_provider ||
                      ' stream_id=' || $stream_id || ' missing_count=' || $counts.missing_count::TEXT);
            }

            -- only authorized if there are no unauthorized edges
            $result := $counts.unauthorized_count = 0;
        }

        -- return if it's authorized or not
        return $result;
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
    $wallet := LOWER($wallet);

    -- Check if the wallet is the stream owner
    if is_stream_owner($data_provider, $stream_id, $wallet) {
        return true;
    }

    -- Check if the wallet is explicitly allowed to write via metadata permissions
    for $row in get_metadata(
        $data_provider,
        $stream_id,
        'allow_write_wallet',
        $wallet,
        1,
        0,
        'created_at DESC'
    ) {
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

    -- BEST PRACTICE: Use RETURN WITH RECURSIVE to perform all logic in one SQL batch operation.
     RETURN WITH RECURSIVE
     -- 1. Generate indexes 1 to N
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($data_providers)
    ),
    arguments AS (
        SELECT
            $data_providers AS data_providers,
            $stream_ids AS stream_ids
    ),
     -- 2. Unnest the input arrays into pairs, keeping original index for order/duplicates
    all_pairs AS (
        SELECT
            idx,
            LOWER(arguments.data_providers[idx]) AS data_provider,
            arguments.stream_ids[idx] AS stream_id
        FROM indexes
        JOIN arguments ON 1=1
    ),
    -- 3. Get unique pairs to check metadata efficiently
    unique_pairs AS (
      SELECT DISTINCT data_provider, stream_id
      FROM all_pairs
    ),
    -- 4. Check both ownership AND permission for each unique pair using subqueries
    unique_check_results AS (
      SELECT
            up.data_provider,
            up.stream_id,
            -- Combine the two checks:
            -- COALESCE handles cases where a subquery might return NULL instead of FALSE
            COALESCE(
                -- Check 1: Is the wallet the LATEST valid owner?
                (SELECT LOWER(m_own.value_ref)
                  FROM metadata m_own
                  JOIN streams s_own ON m_own.stream_ref = s_own.id
                  JOIN data_providers dp_own ON s_own.data_provider_id = dp_own.id
                  WHERE dp_own.address = up.data_provider
                    AND s_own.stream_id = up.stream_id
                    AND m_own.metadata_key = 'stream_owner'
                    AND m_own.disabled_at IS NULL
                  ORDER BY m_own.created_at DESC -- Get the latest owner record
                  LIMIT 1
                ) = $wallet -- This comparison (NULL = $wallet) results in NULL if no owner found
              , FALSE) -- COALESCE NULL to FALSE
            OR -- Logical OR
            COALESCE(
                -- Check 2: Does ANY active permission record exist for the wallet?
                EXISTS (
                    SELECT 1
                    FROM metadata m_perm
                    JOIN streams s_perm ON m_perm.stream_ref = s_perm.id
                    JOIN data_providers dp_perm ON s_perm.data_provider_id = dp_perm.id
                    WHERE dp_perm.address = up.data_provider
                      AND s_perm.stream_id = up.stream_id
                      AND m_perm.metadata_key = 'allow_write_wallet'
                      AND LOWER(m_perm.value_ref) = $wallet
                      AND m_perm.disabled_at IS NULL
                    -- No ORDER BY/LIMIT needed, just checking for existence of any record
                )
            , FALSE) -- COALESCE NULL to FALSE
            as is_allowed
      FROM unique_pairs up
      -- Note: We query metadata table via subqueries, not via LEFT JOIN + GROUP BY
    )
    -- 5. Final SELECT: Map the results from unique checks back to all original pairs
    SELECT
        ap.data_provider,
        ap.stream_id,
        -- JOIN ensures is_allowed is not NULL, but COALESCE is safe
        COALESCE(ucr.is_allowed, FALSE) AS is_allowed
    FROM all_pairs ap
    -- JOIN back to the results calculated for the unique pairs
    -- this will not return any rows for streams that don't exist
    JOIN unique_check_results ucr ON ap.data_provider = ucr.data_provider AND ap.stream_id = ucr.stream_id
    ORDER BY ap.idx; -- Preserve the original input order
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
    -- Use helper function to avoid expensive for-loop roundtrips
    $data_providers := helper_lowercase_array($data_providers);
    $wallet := LOWER($wallet);

    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }

    $lowercase_wallet TEXT := LOWER($wallet);

    -- Use WITH RECURSIVE to unnest all pairs, then find unique pairs to check
    -- This is much more efficient than checking every single pair from input arrays
    RETURN WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($data_providers)
    ),
    stream_arrays AS (
        SELECT 
            $data_providers AS data_providers,
            $stream_ids AS stream_ids
    ),
    all_pairs AS (
        SELECT 
            stream_arrays.data_providers[idx] AS data_provider,
            stream_arrays.stream_ids[idx] AS stream_id
        FROM indexes
        JOIN stream_arrays ON 1=1
    ),
    unique_pairs AS (
        SELECT DISTINCT data_provider, stream_id
        FROM all_pairs
    ),
    -- Check which unique streams have explicit write permission for the wallet
    unique_permission_check AS (
        SELECT 
            up.data_provider,
            up.stream_id,
            CASE WHEN m.value_ref IS NOT NULL THEN true ELSE false END AS has_permission
        FROM unique_pairs up
        LEFT JOIN (
            SELECT dp.address as data_provider, s.stream_id, m.value_ref
            FROM metadata m
            JOIN streams s ON m.stream_ref = s.id
            JOIN data_providers dp ON s.data_provider_id = dp.id
            WHERE m.metadata_key = 'allow_write_wallet'
              AND LOWER(m.value_ref) = $lowercase_wallet
              AND m.disabled_at IS NULL
            ORDER BY m.created_at DESC
        ) m ON up.data_provider = m.data_provider AND up.stream_id = m.stream_id
    )
    -- Map the permission status back to all original pairs
    SELECT 
        ap.data_provider,
        ap.stream_id,
        upc.has_permission
    FROM all_pairs ap
    JOIN unique_permission_check upc ON ap.data_provider = upc.data_provider AND ap.stream_id = upc.stream_id;
};
