-- purpose: populate id column in streams table
-- this file should be run as many times as needed
WITH rows_to_update AS (
    SELECT
        data_provider,
        stream_id,
        id
    FROM
        streams
    WHERE
        id IS NULL
    LIMIT
        100000
), latest_id AS (
    SELECT
        COALESCE(MAX(id), 0) + 1 as next_id
    FROM
        streams
),
-- add the new id
rows_with_id AS (
    SELECT
        data_provider,
        stream_id,
        next_id + ROW_NUMBER() OVER (
            ORDER BY
                data_provider,
                stream_id
        ) as id
    FROM
        rows_to_update
        JOIN latest_id ON 1 = 1
)
UPDATE
    streams
SET
    id = rows_with_id.id
FROM
    rows_with_id
WHERE
    streams.data_provider = rows_with_id.data_provider
    AND streams.stream_id = rows_with_id.stream_id;