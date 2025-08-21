CREATE OR REPLACE ACTION get_stream_ids(
    $data_providers TEXT[],
    $stream_ids TEXT[]
) PUBLIC VIEW RETURNS (stream_ids INT[]) {
    $stream_refs int[];
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
    all_pairs AS (
        SELECT
            idx,
            LOWER(input_arrays.data_providers[idx]) AS data_provider,
            input_arrays.stream_ids[idx] AS stream_id
        FROM indexes
        JOIN input_arrays ON 1=1
    ),
    direct_lookup AS (
        SELECT ap.idx, s.id AS stream_ref
        FROM all_pairs ap
        LEFT JOIN data_providers dp ON dp.address = ap.data_provider
        LEFT JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = ap.stream_id
    )
    SELECT stream_ref
    FROM direct_lookup
    ORDER BY idx {
      $stream_refs = array_append($stream_refs, $row.stream_ref);
    }
    return $stream_refs;
};

