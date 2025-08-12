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
    stream_lookups AS (
        SELECT
            idx,
            s.id AS stream_ref
        FROM indexes
        JOIN input_arrays ON 1=1
        JOIN data_providers dp ON dp.address = input_arrays.data_providers[idx]
        JOIN streams s ON s.data_provider_id = dp.id
                      AND s.stream_id = input_arrays.stream_ids[idx]
    ),
    build_array AS (
        SELECT 1 AS current_idx, ARRAY[]::INT[] AS result
        UNION ALL
        SELECT
            ba.current_idx + 1,
            array_append(ba.result, sl.stream_ref)
        FROM build_array ba
        JOIN stream_lookups sl ON sl.idx = ba.current_idx
        WHERE ba.current_idx <= array_length($data_providers)
    )
    SELECT result AS stream_refs
    FROM build_array
    WHERE current_idx = array_length($data_providers) + 1 {
      return $row.stream_refs;
    }
};
