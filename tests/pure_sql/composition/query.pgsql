-- Simple query with parameters
CREATE OR REPLACE FUNCTION get_substreams(
    v_stream_id     TEXT,
    v_data_provider TEXT,
    v_active_from   INTEGER,
    v_active_to     INTEGER
)
RETURNS TABLE(data_provider TEXT, stream_id TEXT) 
LANGUAGE SQL
AS $$
WITH RECURSIVE 
    -- effective_taxonomies holds, for every parent-child link that is active,
    -- the rows that are considered effective given the time window.
    effective_taxonomies AS (
      SELECT 
         t.data_provider,
         t.stream_id,
         t.child_data_provider,
         t.child_stream_id,
         t.start_time
      FROM taxonomies t
      WHERE t.disabled_at IS NULL
        AND (v_active_to IS NULL OR t.start_time <= v_active_to)
        AND (
          -- (A) For rows before (or at) v_active_from: only include the one with the maximum start_time.
          (v_active_from IS NOT NULL 
             AND t.start_time <= v_active_from 
             AND t.start_time = (
                   SELECT max(t2.start_time)
                   FROM taxonomies t2
                   WHERE t2.data_provider = t.data_provider
                     AND t2.stream_id = t.stream_id
                     AND t2.disabled_at IS NULL
                     AND (v_active_to IS NULL OR t2.start_time <= v_active_to)
                     AND t2.start_time <= v_active_from
             )
          )
          -- (B) Also include any rows with start_time greater than v_active_from.
          OR (v_active_from IS NULL OR t.start_time > v_active_from)
        )
    ),
    -- Now recursively gather substreams using the effective taxonomy links.
    recursive_substreams AS (
      -- Base: effective children of the given root stream.
      -- Start with the root stream itself
      SELECT v_data_provider AS data_provider, 
             v_stream_id AS stream_id
      UNION
      -- Then add all child streams
      SELECT et.child_data_provider,
                       et.child_stream_id
                FROM effective_taxonomies et
                JOIN recursive_substreams rs
                    ON et.data_provider = rs.data_provider
                    AND et.stream_id = rs.stream_id
    )
SELECT DISTINCT data_provider, stream_id
FROM recursive_substreams;
$$;





-- Example usage:
SELECT * FROM get_substreams('1c', 'provider1', NULL, NULL) ORDER BY stream_id;
-- this should return all substreams, including that 1.5p and 1.6c

SELECT * FROM get_substreams('1c', 'provider1', 0, 0) ORDER BY stream_id;
-- this should return all except 1.5p and 1.6c

SELECT * FROM get_substreams('1c', 'provider1', 5, 5) ORDER BY stream_id;
-- this should return 1.1c, 1.1.1p

SELECT * FROM get_substreams('1c', 'provider1', 6, 6) ORDER BY stream_id;
-- this should return 1.1c, 1.1.1p too

SELECT * FROM get_substreams('1c', 'provider1', 10, 10) ORDER BY stream_id;
-- this should return 1.5p and 1.6c

SELECT * FROM get_substreams('1c', 'provider1', 6, 10) ORDER BY stream_id;
-- this should return 1.1c, 1.1.1p, 1.5p, 1.6c

