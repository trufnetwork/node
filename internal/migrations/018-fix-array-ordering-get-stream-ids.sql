CREATE OR REPLACE ACTION get_stream_ids(
  $data_providers TEXT[],
  $stream_ids TEXT[]
) PUBLIC VIEW RETURNS (stream_ids INT[]) {
  IF array_length($data_providers) != array_length($stream_ids) {
    ERROR('array lengths mismatch');
  }

  RETURN
  SELECT array_agg(s.id)
  FROM UNNEST($data_providers, $stream_ids) AS t(dp, sid)
  LEFT JOIN data_providers d ON d.address = LOWER(dp)
  LEFT JOIN streams s ON s.data_provider_id = d.id AND s.stream_id = sid;
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
