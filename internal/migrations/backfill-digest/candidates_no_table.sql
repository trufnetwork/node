-- Read-only: compute all (stream_ref, day_index) candidates to backfill pending_prune_days
-- Safe to re-run. Excludes current partial day via 24h safety window.
-- Alternative version for when pending_prune_days table doesn't exist yet.

WITH params AS (
  SELECT
    (extract(epoch FROM now())::bigint) AS now_utc,
    86400::bigint                          AS day_secs
),
cutoff AS (
  SELECT ((now_utc - day_secs) / day_secs)::bigint AS cutoff_day
  FROM params
),
primitive_streams AS (
  SELECT id AS stream_ref
  FROM main.streams
  WHERE stream_type = 'primitive'
),
raw_days AS (
  SELECT DISTINCT
    pe.stream_ref,
    (pe.event_time / 86400)::bigint AS day_index
  FROM main.primitive_events pe
  JOIN primitive_streams ps ON ps.stream_ref = pe.stream_ref
  JOIN cutoff c ON TRUE
  WHERE pe.event_time < (c.cutoff_day + 1) * 86400
)
SELECT rd.stream_ref, rd.day_index
FROM raw_days rd
WHERE 1=1
ORDER BY rd.day_index ASC, rd.stream_ref ASC;
