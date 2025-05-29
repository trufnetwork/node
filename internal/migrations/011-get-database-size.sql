CREATE OR REPLACE ACTION get_database_size() PUBLIC VIEW RETURNS TABLE(
  database_size BIGINT
) {
  RETURN WITH RECURSIVE
  table_stats AS (
    SELECT 
      'streams' AS table_name,
      (SELECT COUNT(*) FROM streams) AS row_count,
      128 AS avg_row_bytes,
      100 AS avg_pk_index_bytes,
      0 AS avg_other_index_bytes  -- Only PK exists
    UNION ALL
    SELECT 
      'taxonomies',
      (SELECT COUNT(*) FROM taxonomies),
      258,
      48,  -- PK index (UUID)
      100  -- FK index to streams
    UNION ALL
    SELECT 
      'primitive_events',
      (SELECT COUNT(*) FROM primitive_events),
      179,
      120,  -- Composite PK index
      100   -- FK index to streams
    UNION ALL
    SELECT 
      'metadata',
      (SELECT COUNT(*) FROM metadata),
      230,  -- Average of your estimate
      48,   -- PK index (UUID)
      100   -- FK index to streams
  )


  SELECT
    SUM(row_count * (avg_row_bytes + avg_pk_index_bytes + avg_other_index_bytes))::BIGINT
  FROM table_stats;
}