CREATE OR REPLACE ACTION get_stream_ids(
    $data_providers TEXT[],
    $stream_ids TEXT[]
) PRIVATE VIEW RETURNS (stream_ids INT[]) {
    -- Use WITH RECURSIVE to process stream ids lookup in single SQL operation
    -- This avoids the expensive for-loop roundtrips
    for $row in WITH RECURSIVE
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($data_providers)
    ),
    input_arrays AS (
        SELECT
            $data_providers AS data_providers,
            $stream_ids AS stream_ids
    ),
    -- Unnest to all pairs with original index for order reconstruction
    all_pairs AS (
        SELECT
            idx,
            LOWER(input_arrays.data_providers[idx]) AS data_provider,
            input_arrays.stream_ids[idx] AS stream_id
        FROM indexes
        JOIN input_arrays ON 1=1
    ),
    -- Deduplicate lookups to avoid repeated joins per identical pair
    unique_pairs AS (
        SELECT DISTINCT data_provider, stream_id
        FROM all_pairs
    ),
    -- Perform a single lookup per unique pair
    unique_lookup AS (
        SELECT
            up.data_provider,
            up.stream_id,
            s.id AS stream_ref
        FROM unique_pairs up
        JOIN data_providers dp ON dp.address = up.data_provider
        JOIN streams s ON s.data_provider_id = dp.id
                      AND s.stream_id = up.stream_id
    ),
    -- Map back to original order using idx
    mapped AS (
        SELECT ap.idx, ul.stream_ref
        FROM all_pairs ap
        JOIN unique_lookup ul
          ON ul.data_provider = ap.data_provider
         AND ul.stream_id = ap.stream_id
    ),
    build_array AS (
        SELECT 1 AS current_idx, ARRAY[]::INT[] AS result
        UNION ALL
        SELECT
            ba.current_idx + 1,
            array_append(ba.result, m.stream_ref)
        FROM build_array ba
        JOIN mapped m ON m.idx = ba.current_idx
        WHERE ba.current_idx <= array_length($data_providers)
    )
    SELECT result AS stream_refs
    FROM build_array
    WHERE current_idx = array_length($data_providers) + 1 {
      return $row.stream_refs;
    }
};
