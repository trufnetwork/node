CREATE OR REPLACE ACTION get_stream_ids(
  $data_providers TEXT[],
  $stream_ids     TEXT[]
) PUBLIC VIEW RETURNS (stream_ids INT[]) {
  IF array_length($data_providers) != array_length($stream_ids) {
    ERROR('array lengths mismatch');
  }

  RETURN
  WITH idx AS (
    SELECT
      ord AS idx,
      LOWER(dp) AS dp,   -- compute once
      sid       AS sid
    FROM unnest($data_providers, $stream_ids) WITH ORDINALITY AS u(dp, sid, ord)
  ),
  joined AS (
    SELECT i.idx, s.id AS stream_id_resolved
    FROM idx i
    LEFT JOIN data_providers d
      ON d.address = i.dp
    LEFT JOIN streams s
      ON s.data_provider_id = d.id
     AND s.stream_id       = i.sid
  )
  SELECT COALESCE(array_agg(stream_id_resolved ORDER BY idx), ARRAY[]::INT[])
  FROM joined;
};


CREATE OR REPLACE ACTION get_stream_id(
  $data_provider_address TEXT,
  $stream_id TEXT
) PUBLIC returns (id INT) {
  $ids := get_stream_ids(ARRAY[$data_provider_address], ARRAY[$stream_id]);
  if array_length($ids) = 0 {
    return NULL;
  }
  return $ids[1];
};
