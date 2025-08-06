-- purpose: populate data_provider_id column in streams table
-- this file should be run as many times as needed
WITH rows_to_update AS (
    SELECT
        s.data_provider as target_data_provider,
        s.stream_id as target_stream_id,
        dp.id AS data_provider_id
    FROM
        streams s
        JOIN data_providers dp ON dp.address = s.data_provider
    WHERE
        s.data_provider_id IS NULL
    ORDER BY
        s.created_at
    LIMIT
        100000
)
UPDATE
    streams
SET
    data_provider_id = r.data_provider_id
FROM
    rows_to_update r
WHERE
    data_provider = target_data_provider
    AND stream_id = target_stream_id;