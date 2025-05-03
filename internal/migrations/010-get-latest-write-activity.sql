CREATE OR REPLACE ACTION get_last_transactions(
    $data_provider TEXT,
    $limit_size   INT8
) PUBLIC VIEW RETURNS TABLE(
    created_at INT8,
    method     TEXT
) {
    IF $limit_size IS NULL OR $limit_size <= 0 {
        $limit_size := 6;
    }

    IF $limit_size > 100 {
        ERROR('Limit size cannot exceed 100');
    }

    RETURN SELECT created_at, method FROM (
        SELECT created_at, method, ROW_NUMBER() OVER (PARTITION BY created_at ORDER BY priority ASC) AS rn FROM (
             SELECT created_at, 'deployStream' AS method, 1 AS priority
             FROM streams
             WHERE COALESCE($data_provider, '') = '' OR data_provider = $data_provider
             UNION ALL
             SELECT created_at, 'insertRecords', 2
             FROM primitive_events
             WHERE COALESCE($data_provider, '') = '' OR data_provider = $data_provider
             UNION ALL
             SELECT created_at, 'setTaxonomies', 3
             FROM taxonomies
             WHERE COALESCE($data_provider, '') = '' OR data_provider = $data_provider
             UNION ALL
             SELECT created_at, 'setMetadata', 4
             FROM metadata
             WHERE COALESCE($data_provider, '') = '' OR data_provider = $data_provider
         ) AS combined
    ) AS ranked
    WHERE rn = 1
    ORDER BY created_at DESC
    LIMIT $limit_size;
}
