/**
 * list_metadata_by_height: Queries metadata within a specific block height range.
 * Supports pagination.
 * Supports filtering by key and ref.
 * 
 * Parameters:
 *   $key: filter by metadata key.
 *.  $ref: filter by value reference.
 *   $from_height: Start height (inclusive). If NULL, uses earliest available.
 *   $to_height: End height (inclusive). If NULL, uses current height.
 *   $limit: Maximum number of results to return.
 *   $offset: Number of results to skip for pagination.
 * 
 * Returns:
 *   Table with metadata entries matching the criteria.
 */
CREATE OR REPLACE ACTION list_metadata_by_height(
  $key TEXT,
  $ref TEXT,
  $from_height INT8,
  $to_height INT8,
  $limit INT,
  $offset INT
) PUBLIC view returns table(
    stream_id TEXT,
    data_provider TEXT,
    row_id uuid,
    value_i INT,
    value_f NUMERIC(36,18),
    value_b bool,
    value_s TEXT,
    value_ref TEXT,
    created_at INT8
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
    
    -- Get current block height for default behavior
    $current_block INT8 := @height;
    
    -- Determine effective height range, if none given, get all metadata
    $effective_from INT8 := COALESCE($from_height, 0);
    $effective_to INT8 := COALESCE($to_height, $current_block);
    
    -- Validate height range
    if $effective_from > $effective_to {
        ERROR('Invalid height range: from_height (' || $effective_from::TEXT || ') > to_height (' || $effective_to::TEXT || ')');
    }

     RETURN SELECT
              s.stream_id,
              dp.address AS data_provider,
              row_id,
              value_i,
              value_f,
              value_b,
              value_s,
              value_ref,
              m.created_at
            FROM metadata m
            JOIN streams s ON s.id = m.stream_ref
            JOIN data_providers dp ON dp.id = s.data_provider_id
            WHERE metadata_key = $key
              AND m.disabled_at IS NULL
              -- do not use LOWER on value_ref, or it will break the index lookup
              AND ($ref IS NULL OR m.value_ref = LOWER($ref))
              AND m.created_at >= $effective_from
              AND m.created_at <= $effective_to
            ORDER BY m.created_at ASC
            LIMIT $limit OFFSET $offset;
};