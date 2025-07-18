CREATE OR REPLACE ACTION get_database_size() PUBLIC VIEW RETURNS TABLE(
  database_size BIGINT
) {
  RETURN WITH RECURSIVE
  table_stats AS (
    SELECT 
      'data_providers' AS table_name,
      (SELECT COUNT(*) FROM data_providers) AS row_count,
      58 AS avg_row_bytes,  -- UUID(16) + address(42) = 58 bytes
      20 AS avg_pk_index_bytes,  -- UUID PK index
      50 AS avg_other_index_bytes  -- Unique index on address
    UNION ALL
    SELECT 
      'streams',
      (SELECT COUNT(*) FROM streams),
      81 AS avg_row_bytes,  -- UUID(16) + UUID(16) + stream_id(32) + stream_type(9) + created_at(8) = 81
      20 AS avg_pk_index_bytes,  -- UUID PK index
      50 AS avg_other_index_bytes  -- FK index on data_provider_id + composite index
    UNION ALL
    SELECT 
      'taxonomies',
      (SELECT COUNT(*) FROM taxonomies),
      100 AS avg_row_bytes,  -- UUID(16) + UUID(16) + UUID(16) + weight(18) + created_at(8) + disabled_at(8) + group_sequence(8) + start_time(8) = 98, rounded to 100
      20 AS avg_pk_index_bytes,  -- UUID PK index
      60 AS avg_other_index_bytes  -- FK indexes on parent_stream_ref and child_stream_ref
    UNION ALL
    SELECT 
      'primitive_events',
      (SELECT COUNT(*) FROM primitive_events),
      50 AS avg_row_bytes,  -- UUID(16) + event_time(8) + value(18) + created_at(8) = 50 bytes
      60 AS avg_pk_index_bytes,  -- Composite PK index (stream_ref + event_time + created_at)
      20 AS avg_other_index_bytes  -- FK index on stream_ref
    UNION ALL
    SELECT 
      'metadata',
      (SELECT COUNT(*) FROM metadata),
      90 AS avg_row_bytes,  -- UUID(16) + UUID(16) + metadata_key(~20) + value_text(~20) + created_at(8) + disabled_at(8) = ~88, rounded to 90
      20 AS avg_pk_index_bytes,  -- UUID PK index
      40 AS avg_other_index_bytes  -- FK index on stream_ref + other indexes
  )

  SELECT
    SUM(row_count * (avg_row_bytes + avg_pk_index_bytes + avg_other_index_bytes))::BIGINT
  FROM table_stats;
}