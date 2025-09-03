-- Read-only: compute all (stream_ref, day_index) candidates to backfill pending_prune_days  
-- Safe to re-run. Safety window enforced by auto_digest action during processing.  
-- Alternative version for when pending_prune_days table doesn't exist yet.  

WITH params AS (
  SELECT
    (extract(epoch FROM now())::bigint) AS now_utc,
    86400::bigint                          AS day_secs
),
primitive_streams AS (
  SELECT id AS stream_ref
  FROM main.streams
  WHERE stream_type = 'primitive'
),
raw_days AS (
  SELECT DISTINCT
    pe.stream_ref,
    (pe.event_time / p.day_secs)::bigint AS day_index
  FROM main.primitive_events pe
  JOIN primitive_streams ps ON ps.stream_ref = pe.stream_ref
  JOIN params p ON TRUE
)
SELECT rd.stream_ref, rd.day_index
FROM raw_days rd
WHERE 1=1
ORDER BY rd.day_index ASC, rd.stream_ref ASC;
