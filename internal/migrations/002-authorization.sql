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
    $lowercase_wallet_address TEXT := LOWER($wallet_address);
    -- Check if the stream exists
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }
    -- Check if the stream is private
    $is_private BOOL := false;
    for $row in get_metadata(
        $data_provider,
        $stream_id,
        'read_visibility',
        null,
        1,
        0,
        'created_at DESC'
    ) {
        if $row.value_i = 1 {
            $is_private := true;
        }
    }
    if $is_private = false {
        -- short circuit if the stream is not private
        return true;
    }

    -- Check if the wallet is allowed to read the stream
    $is_allowed BOOL := false;
    for $row in get_metadata(
        $data_provider,
        $stream_id,
        'allow_read_wallet',
        $lowercase_wallet_address,
        1,
        0,
        'created_at DESC'
    ) {
        $is_allowed := true;
    }

    if $is_private = true AND $is_allowed = false {
        return false;
    }

    return true;
};

/**
 * is_allowed_to_compose: Checks if a wallet can compose a stream.
 * Considers compose visibility and explicit compose permissions.
 */
CREATE OR REPLACE ACTION is_allowed_to_compose(
    $data_provider TEXT,
    $stream_id TEXT,
    $composing_stream_id TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns (is_allowed BOOL) {
    -- Check if the stream exists
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }
    if !stream_exists($data_provider, $composing_stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $child_stream_id);
    }
    -- Check if the stream is private
    $is_private BOOL := false;
    for $row in get_metadata(
        $data_provider,
        $stream_id,
        'compose_visibility',
        null,
        1,
        0,
        'created_at DESC'
    ) {
        if $row.value_i = 1 {
            $is_private := true;
        }
    }
    if $is_private = false {
        -- short circuit if the stream is not private
        return true;
    }

    -- Check if the wallet is allowed to read the stream
    $is_allowed BOOL := false;
    for $row in get_metadata(
        $data_provider,
        $stream_id,
        'allow_compose_stream',
        $composing_stream_id,
        1,
        0,
        'created_at DESC'
    ) {
        $is_allowed := true;
    }

    if $is_private = true AND $is_allowed = false {
        return false;
    }

    NOTICE(FORMAT('is_allowed_to_compose: data_provider=%s stream_id=%s wallet_address=%s is_private=%s is_allowed=%s', $data_provider, $stream_id, $lowercase_wallet_address, $is_private, $is_allowed));

    return true;
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
    -- Check if the stream exists
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    $max_int8 INT := 9223372036854775000;
    $effective_active_from INT := COALESCE($active_from, 0);
    $effective_active_to INT := COALESCE($active_to, $max_int8);


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
          base.data_provider         AS parent_data_provider,
          base.stream_id             AS parent_stream_id,
          base.child_data_provider,
          base.child_stream_id,

          -- The interval during which this row is active:
          base.start_time            AS group_sequence_start,
          COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end

      FROM (
          SELECT
              t.data_provider,
              t.stream_id,
              t.child_data_provider,
              t.child_stream_id,
              t.start_time,
              t.group_sequence,
              MAX(t.group_sequence) OVER (
                  PARTITION BY t.data_provider, t.stream_id, t.start_time
              ) AS max_group_sequence
          FROM taxonomies t
          WHERE t.data_provider = $data_provider
            AND t.stream_id     = $stream_id
            AND t.disabled_at   IS NULL
            AND t.start_time   <= $effective_active_to
            AND t.start_time   >= COALESCE((
                  -- Find the most recent taxonomy at or before effective_active_from
                  SELECT t2.start_time
                  FROM taxonomies t2
                  WHERE t2.data_provider = t.data_provider
                    AND t2.stream_id     = t.stream_id
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
              WHERE t.data_provider = $data_provider
                AND t.stream_id     = $stream_id
                AND t.disabled_at   IS NULL
                AND t.start_time   <= $effective_active_to
                AND t.start_time   >= COALESCE((
                      SELECT t2.start_time
                      FROM taxonomies t2
                      WHERE t2.data_provider = t.data_provider
                        AND t2.stream_id     = t.stream_id
                        AND t2.disabled_at   IS NULL
                        AND t2.start_time   <= $effective_active_from
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

      UNION

      /*------------------------------------------------------------------
       * (2) Recursive Child-Level Overshadow:
       *     For each discovered child, gather overshadow rows for that child
       *     and produce intervals that overlap the parent's own active interval.
       *------------------------------------------------------------------*/
      SELECT
          -- promote the child to the parent
          parent.child_data_provider   AS parent_data_provider,
          parent.child_stream_id       AS parent_stream_id,

          child.child_data_provider,
          child.child_stream_id,

          -- Intersection of parent's active interval and child's:
          GREATEST(parent.group_sequence_start, child.start_time)   AS group_sequence_start,
          LEAST   (parent.group_sequence_end,   child.group_sequence_end) AS group_sequence_end

      FROM substreams parent
      JOIN (
          /* Child overshadow logic, same pattern as above but for child dp/sid. */
          SELECT
              base.data_provider,
              base.stream_id,
              base.child_data_provider,
              base.child_stream_id,
              base.start_time,
              COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
          FROM (
              SELECT
                  t.data_provider,
                  t.stream_id,
                  t.child_data_provider,
                  t.child_stream_id,
                  t.start_time,
                  t.group_sequence,
                  MAX(t.group_sequence) OVER (
                      PARTITION BY t.data_provider, t.stream_id, t.start_time
                  ) AS max_group_sequence
              FROM taxonomies t
              WHERE t.disabled_at IS NULL
                AND t.start_time <= $effective_active_to
                AND t.start_time >= COALESCE((
                      -- Most recent taxonomy at or before effective_from
                      SELECT t2.start_time
                      FROM taxonomies t2
                      WHERE t2.data_provider = t.data_provider
                        AND t2.stream_id     = t.stream_id
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
                  WHERE t.disabled_at   IS NULL
                    AND t.start_time   <= $effective_active_to
                    AND t.start_time   >= COALESCE((
                          SELECT t2.start_time
                          FROM taxonomies t2
                          WHERE t2.data_provider = t.data_provider
                            AND t2.stream_id     = t.stream_id
                            AND t2.disabled_at   IS NULL
                            AND t2.start_time   <= $effective_active_from
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
      /* Overlap check: child's interval must intersect parent's */
      WHERE child.start_time         <= parent.group_sequence_end
        AND child.group_sequence_end >= parent.group_sequence_start
    ),

    -- select distinct child union parent
    all_streams as (
        -- merge root stream with substreams
        SELECT DISTINCT child_data_provider AS data_provider, child_stream_id AS stream_id
        FROM substreams
        UNION
        SELECT $data_provider AS data_provider, $stream_id AS stream_id
    ),
    
    -- Find substreams that don't exist
        inexisting_substreams as (
            SELECT rs.data_provider, rs.stream_id 
            FROM all_streams rs
            LEFT JOIN streams s 
                ON rs.data_provider = s.data_provider 
                AND rs.stream_id = s.stream_id
            WHERE s.data_provider IS NULL
        ),
        -- Find substreams that are private
        private_substreams as (
            SELECT rs.data_provider, rs.stream_id 
            FROM all_streams rs
            WHERE (
                SELECT value_i
                FROM metadata m
                WHERE m.data_provider = rs.data_provider
                    AND m.stream_id = rs.stream_id
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
            WHERE NOT EXISTS (
                SELECT 1
                FROM metadata m
                WHERE m.data_provider = p.data_provider
                    AND m.stream_id = p.stream_id
                    AND m.metadata_key = 'allow_read_wallet'
                    AND LOWER(m.value_ref) = LOWER($wallet_address)
                    AND m.disabled_at IS NULL
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
    
    -- If we got here (which we shouldn't), return false as a fallback
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
    -- Check if the stream exists
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }
    
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
            base.data_provider         AS parent_data_provider,
            base.stream_id             AS parent_stream_id,
            base.child_data_provider,
            base.child_stream_id,

            -- The interval during which this row is active:
            base.start_time            AS group_sequence_start,
            COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end

        FROM (
            SELECT
                t.data_provider,
                t.stream_id,
                t.child_data_provider,
                t.child_stream_id,
                t.start_time,
                t.group_sequence,
                MAX(t.group_sequence) OVER (
                    PARTITION BY t.data_provider, t.stream_id, t.start_time
                ) AS max_group_sequence
            FROM taxonomies t
            WHERE t.data_provider = $data_provider
                AND t.stream_id     = $stream_id
                AND t.disabled_at   IS NULL
                AND t.start_time   <= $effective_active_to
                AND t.start_time   >= COALESCE((
                    -- Find the most recent taxonomy at or before effective_active_from
                    SELECT t2.start_time
                    FROM taxonomies t2
                    WHERE t2.data_provider = t.data_provider
                        AND t2.stream_id     = t.stream_id
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
                WHERE t.data_provider = $data_provider
                    AND t.stream_id     = $stream_id
                    AND t.disabled_at   IS NULL
                    AND t.start_time   <= $effective_active_to
                    AND t.start_time   >= COALESCE((
                        SELECT t2.start_time
                        FROM taxonomies t2
                        WHERE t2.data_provider = t.data_provider
                            AND t2.stream_id     = t.stream_id
                            AND t2.disabled_at   IS NULL
                            AND t2.start_time   <= $effective_active_from
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

        UNION

        /*------------------------------------------------------------------
        * (2) Recursive Child-Level Overshadow:
        *     For each discovered child, gather overshadow rows for that child
        *     and produce intervals that overlap the parent's own active interval.
        *------------------------------------------------------------------*/
        SELECT
            -- promote the child to the parent
            parent.child_data_provider   AS parent_data_provider,
            parent.child_stream_id       AS parent_stream_id,

            child.child_data_provider,
            child.child_stream_id,

            -- Intersection of parent's active interval and child's:
            GREATEST(parent.group_sequence_start, child.start_time)   AS group_sequence_start,
            LEAST   (parent.group_sequence_end,   child.group_sequence_end) AS group_sequence_end

        FROM substreams parent
        JOIN (
            /* Child overshadow logic, same pattern as above but for child dp/sid. */
            SELECT
                base.data_provider,
                base.stream_id,
                base.child_data_provider,
                base.child_stream_id,
                base.start_time,
                COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
            FROM (
                SELECT
                    t.data_provider,
                    t.stream_id,
                    t.child_data_provider,
                    t.child_stream_id,
                    t.start_time,
                    t.group_sequence,
                    MAX(t.group_sequence) OVER (
                        PARTITION BY t.data_provider, t.stream_id, t.start_time
                    ) AS max_group_sequence
                FROM taxonomies t
                WHERE t.disabled_at IS NULL
                    AND t.start_time <= $effective_active_to
                    AND t.start_time >= COALESCE((
                        -- Most recent taxonomy at or before effective_from
                        SELECT t2.start_time
                        FROM taxonomies t2
                        WHERE t2.data_provider = t.data_provider
                            AND t2.stream_id     = t.stream_id
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
                    WHERE t.disabled_at   IS NULL
                        AND t.start_time   <= $effective_active_to
                        AND t.start_time   >= COALESCE((
                            SELECT t2.start_time
                            FROM taxonomies t2
                            WHERE t2.data_provider = t.data_provider
                                AND t2.stream_id     = t.stream_id
                                AND t2.disabled_at   IS NULL
                                AND t2.start_time   <= $effective_active_from
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
        /* Overlap check: child's interval must intersect parent's */
        WHERE child.start_time         <= parent.group_sequence_end
            AND child.group_sequence_end >= parent.group_sequence_start
        ),
    
        parent_child_edges as (
            SELECT DISTINCT p.parent_data_provider, p.parent_stream_id, p.child_data_provider, p.child_stream_id
            FROM substreams p
        ),
        -- Check that all child streams exist.
        inexisting_substreams AS (
            SELECT DISTINCT p.child_data_provider, p.child_stream_id
            FROM parent_child_edges p
            LEFT JOIN streams s
              ON p.child_data_provider = s.data_provider
             AND p.child_stream_id = s.stream_id
            WHERE s.data_provider IS NULL
        ),
        -- For each edge, if the child is private, check that the child whitelists its parent.
        unauthorized_edges AS (
            SELECT p.parent_data_provider, p.parent_stream_id, p.child_data_provider, p.child_stream_id
            FROM parent_child_edges p
            WHERE (
                SELECT value_i
                FROM metadata m
                WHERE m.data_provider = p.child_data_provider
                  AND m.stream_id = p.child_stream_id
                  AND m.metadata_key = 'compose_visibility'
                  AND m.disabled_at IS NULL
                ORDER BY m.created_at DESC
                LIMIT 1
            -- check if the child is a private stream (compose visibility is 1)
            ) = 1
            AND NOT EXISTS (
                SELECT 1
                FROM metadata m2
                WHERE m2.data_provider = p.child_data_provider
                  AND m2.stream_id = p.child_stream_id
                  AND m2.metadata_key = 'allow_compose_stream'
                  AND m2.disabled_at IS NULL
                  AND m2.value_ref = p.parent_stream_id::text
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
