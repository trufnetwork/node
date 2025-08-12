WITH rows_to_update AS (
    SELECT
        pe.data_provider as row_data_provider,
        pe.stream_id as row_stream_id,
        pe.event_time as row_event_time,
        pe.created_at as row_created_at,
        s.id AS stream_ref_to_set
    FROM primitive_events pe
             JOIN streams s
                  ON s.data_provider = pe.data_provider
                      AND s.stream_id = pe.stream_id
    WHERE pe.stream_ref IS NULL
    ORDER BY pe.created_at
    LIMIT 10000
)
UPDATE primitive_events
SET stream_ref = stream_ref_to_set
FROM rows_to_update
WHERE data_provider = row_data_provider
  AND stream_id = row_stream_id
  AND event_time = row_event_time
  AND created_at = row_created_at;
