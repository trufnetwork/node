CREATE OR REPLACE ACTION get_stream_ids(
  $data_providers TEXT[],
  $stream_ids TEXT[]
) PUBLIC VIEW RETURNS (stream_ids INT[]) {
  IF array_length($data_providers) != array_length($stream_ids) {
    ERROR('array lengths mismatch');
  }

  RETURN
  WITH joined AS (
    SELECT gs.idx, s.id AS stream_id_resolved
    FROM generate_subscripts($data_providers) AS gs(idx)
    LEFT JOIN data_providers d ON d.address = LOWER($data_providers[gs.idx])
    LEFT JOIN streams s ON s.data_provider_id = d.id AND s.stream_id = $stream_ids[gs.idx]
  )
  SELECT array_agg(stream_id_resolved ORDER BY idx)
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
