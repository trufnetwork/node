/*
    TAXONOMY QUERY ACTIONS
    
    This migration adds SQL actions to query taxonomies by block height range.
    Required for explorer synchronization to detect taxonomy changes efficiently.
    
    Actions Created:
    - list_taxonomies_by_height: Main action for height-based taxonomy querying
    - get_taxonomies_for_streams: Batch processing for specific streams
    
    Use Case:
    Explorer synchronizer can call list_taxonomies_by_height(@height - 1000, @height, ...)
    to detect taxonomy changes incrementally without expensive full-stream scanning.
*/

/**
 * list_taxonomies_by_height: Queries taxonomies within a specific block height range.
 * Supports pagination and latest-only filtering for efficient synchronization.
 * 
 * Parameters:
 *   $from_height: Start height (inclusive). If NULL, uses earliest available.
 *   $to_height: End height (inclusive). If NULL, uses current height.
 *   $limit: Maximum number of results to return.
 *   $offset: Number of results to skip for pagination.
 *   $latest_only: If true, returns only latest group_sequence per stream.
 * 
 * Returns:
 *   Table with taxonomy entries matching the criteria, ordered by created_at ASC.
 *   
 * Logic:
 *   1. If both from_height and to_height are NULL -> fetch latest set of taxonomies
 *   2. Latest set is defined by latest created_at and max group_sequence per stream
 *   3. One stream can have multiple taxonomy sets, only return latest if latest_only=true
 *   4. Filter by height range if provided
 *   5. Only active taxonomies (disabled_at IS NULL)
 *   6. Order by created_at ASC for consistent pagination
 */
CREATE OR REPLACE ACTION list_taxonomies_by_height(
    $from_height INT8,
    $to_height INT8,
    $limit INT,
    $offset INT,
    $latest_only BOOL
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT,
    child_data_provider TEXT,
    child_stream_id TEXT,
    weight NUMERIC(36,18),
    created_at INT8,
    group_sequence INT8,
    start_time INT8
) {
    -- Set defaults for pagination and validate values
    if $limit IS NULL {
        $limit := 1000;
    }
    if $offset IS NULL {
        $offset := 0;
    }
    
    -- Ensure non-negative values for PostgreSQL compatibility
    if $limit < 0 {
        $limit := 0;
    }
    if $offset < 0 {
        $offset := 0;
    }
    if $latest_only IS NULL {
        $latest_only := false;
    }
    
    -- Get current block height for default behavior
    $current_block INT8 := @height;
    
    -- Determine effective height range
    $effective_from INT8;
    $effective_to INT8;
    
    if $from_height IS NULL AND $to_height IS NULL {
        -- Special case: return latest set of taxonomies
        -- Use a reasonable lookback window to find recent taxonomies
        $effective_from := $current_block - 1000;
        $effective_to := $current_block;
    } else {
        $effective_from := COALESCE($from_height, 0);
        $effective_to := COALESCE($to_height, $current_block);
    }
    
    -- Validate height range
    if $effective_from > $effective_to {
        ERROR('Invalid height range: from_height (' || $effective_from::TEXT || ') > to_height (' || $effective_to::TEXT || ')');
    }
    
    if $latest_only {
        -- Return only latest group_sequence per stream within height range using normalized refs
        RETURN WITH stream_latest_info AS (
            SELECT 
                t.stream_ref,
                MAX(t.created_at) AS max_created_at
            FROM taxonomies t
            WHERE t.created_at >= $effective_from
              AND t.created_at <= $effective_to
              AND t.disabled_at IS NULL
            GROUP BY t.stream_ref
        ),
        stream_latest_group AS (
            SELECT 
                sli.stream_ref,
                sli.max_created_at,
                MAX(t.group_sequence) AS max_group_sequence
            FROM stream_latest_info sli
            JOIN taxonomies t 
              ON t.stream_ref = sli.stream_ref 
             AND t.created_at = sli.max_created_at
            WHERE t.disabled_at IS NULL
            GROUP BY sli.stream_ref, sli.max_created_at
        )
        SELECT 
            dp.address AS data_provider,
            s.stream_id,
            dpc.address AS child_data_provider,
            sc.stream_id AS child_stream_id,
            t.weight,
            t.created_at,
            t.group_sequence,
            t.start_time
        FROM stream_latest_group slg
        JOIN taxonomies t 
          ON t.stream_ref = slg.stream_ref 
         AND t.created_at = slg.max_created_at 
         AND t.group_sequence = slg.max_group_sequence
        JOIN streams s ON s.id = t.stream_ref
        JOIN data_providers dp ON dp.id = s.data_provider_id
        JOIN streams sc ON sc.id = t.child_stream_ref
        JOIN data_providers dpc ON dpc.id = sc.data_provider_id
        WHERE t.disabled_at IS NULL
        ORDER BY t.created_at ASC, t.group_sequence ASC, dp.address ASC, s.stream_id ASC, dpc.address ASC, sc.stream_id ASC
        LIMIT $limit OFFSET $offset;
    } else {
        -- Return all taxonomies within height range using normalized refs
        RETURN SELECT 
            dp.address AS data_provider,
            s.stream_id,
            dpc.address AS child_data_provider,
            sc.stream_id AS child_stream_id,
            t.weight,
            t.created_at,
            t.group_sequence,
            t.start_time
        FROM taxonomies t
        JOIN streams s ON s.id = t.stream_ref
        JOIN data_providers dp ON dp.id = s.data_provider_id
        JOIN streams sc ON sc.id = t.child_stream_ref
        JOIN data_providers dpc ON dpc.id = sc.data_provider_id
        WHERE t.created_at >= $effective_from
          AND t.created_at <= $effective_to
          AND t.disabled_at IS NULL
        ORDER BY t.created_at ASC, t.group_sequence ASC
        LIMIT $limit OFFSET $offset;
    }
};

/**
 * get_taxonomies_for_streams: Batch fetch taxonomies for specific streams.
 * Uses efficient WITH RECURSIVE pattern for array processing.
 * 
 * Parameters:
 *   $data_providers: Array of data provider addresses.
 *   $stream_ids: Array of stream IDs (must match data_providers length).
 *   $latest_only: If true, returns only latest group_sequence per stream.
 * 
 * Returns:
 *   Table with taxonomy entries for the specified streams.
 *   
 * Use Case:
 *   When sync process needs to verify taxonomy for specific streams.
 */
CREATE OR REPLACE ACTION get_taxonomies_for_streams(
    $data_providers TEXT[],
    $stream_ids TEXT[],
    $latest_only BOOL
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT,
    child_data_provider TEXT,
    child_stream_id TEXT,
    weight NUMERIC(36,18),
    created_at INT8,
    group_sequence INT8,
    start_time INT8
) {
    -- Data providers will be lowercased directly in queries using LOWER()

    if $latest_only IS NULL {
        $latest_only := false;
    }

    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }

    if $latest_only {
        RETURN WITH target_streams AS (
            SELECT DISTINCT
                s.id AS stream_ref
            FROM UNNEST($data_providers, $stream_ids) AS t(data_provider, stream_id)
            JOIN data_providers dp ON dp.address = LOWER(t.data_provider)
            JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id
        ),
        stream_latest_info AS (
            SELECT
                t.stream_ref,
                MAX(t.created_at) as max_created_at
            FROM taxonomies t
            JOIN target_streams ts ON ts.stream_ref = t.stream_ref
            WHERE t.disabled_at IS NULL
            GROUP BY t.stream_ref
        ),
        stream_latest_group AS (
            SELECT
                sli.stream_ref,
                sli.max_created_at,
                MAX(t.group_sequence) as max_group_sequence
            FROM stream_latest_info sli
            JOIN taxonomies t ON t.stream_ref = sli.stream_ref
                              AND t.created_at = sli.max_created_at
            WHERE t.disabled_at IS NULL
            GROUP BY sli.stream_ref, sli.max_created_at
        )
        SELECT
            dp.address AS data_provider,
            s.stream_id,
            dpc.address AS child_data_provider,
            sc.stream_id AS child_stream_id,
            t.weight,
            t.created_at,
            t.group_sequence,
            t.start_time
        FROM stream_latest_group slg
        JOIN taxonomies t
          ON t.stream_ref = slg.stream_ref
         AND t.created_at = slg.max_created_at
         AND t.group_sequence = slg.max_group_sequence
        JOIN streams s ON s.id = t.stream_ref
        JOIN data_providers dp ON dp.id = s.data_provider_id
        JOIN streams sc ON sc.id = t.child_stream_ref
        JOIN data_providers dpc ON dpc.id = sc.data_provider_id
        WHERE t.disabled_at IS NULL;
    } else {
        RETURN SELECT DISTINCT
            dp.address AS data_provider,
            s.stream_id,
            dpc.address AS child_data_provider,
            sc.stream_id AS child_stream_id,
            tax.weight,
            tax.created_at,
            tax.group_sequence,
            tax.start_time
        FROM UNNEST($data_providers, $stream_ids) AS t(data_provider, stream_id)
        JOIN data_providers dp ON dp.address = LOWER(t.data_provider)
        JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id
        JOIN taxonomies tax ON tax.stream_ref = s.id
        JOIN streams sc ON sc.id = tax.child_stream_ref
        JOIN data_providers dpc ON dpc.id = sc.data_provider_id
        WHERE tax.disabled_at IS NULL
        ORDER BY tax.created_at ASC, tax.group_sequence ASC;
    }
};