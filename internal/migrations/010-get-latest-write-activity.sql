CREATE OR REPLACE ACTION get_last_transactions(
    $data_provider TEXT,
    $limit_size   INT8
) PUBLIC VIEW RETURNS TABLE(
    created_at INT8,
    method     TEXT
) {
    $data_provider := LOWER($data_provider);
    IF $limit_size IS NULL {
        $limit_size := 6;
    }
    IF $limit_size <= 0 {
        $limit_size := 6;
    }

    IF $limit_size > 100 {
        ERROR('Limit size cannot exceed 100');
    }

    RETURN SELECT created_at, method FROM (
      SELECT created_at, method, ROW_NUMBER() OVER (PARTITION BY created_at ORDER BY priority ASC) AS rn FROM (
          SELECT s.created_at, 'deployStream' AS method, 1 AS priority
          FROM streams s
          JOIN data_providers dp ON s.data_provider_id = dp.id
          WHERE COALESCE($data_provider, '') = '' OR dp.address = $data_provider
          UNION ALL
          SELECT pe.created_at, 'insertRecords', 2
          FROM primitive_events pe
          JOIN streams s ON pe.stream_ref = s.id
          JOIN data_providers dp ON s.data_provider_id = dp.id
          WHERE COALESCE($data_provider, '') = '' OR dp.address = $data_provider
          UNION ALL
          SELECT t.created_at, 'setTaxonomies', 3
          FROM taxonomies t
          JOIN streams s ON t.stream_ref = s.id
          JOIN data_providers dp ON s.data_provider_id = dp.id
          WHERE COALESCE($data_provider, '') = '' OR dp.address = $data_provider
          UNION ALL
          SELECT m.created_at, 'setMetadata', 4
          FROM metadata m
          JOIN streams s ON m.stream_ref = s.id
          JOIN data_providers dp ON s.data_provider_id = dp.id
          WHERE COALESCE($data_provider, '') = '' OR dp.address = $data_provider
      ) AS combined
  ) AS ranked
  WHERE rn = 1
  ORDER BY created_at DESC
  LIMIT $limit_size;
}
