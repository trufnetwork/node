-- 1. Drop all foreign key constraints first
ALTER TABLE metadata DROP CONSTRAINT IF EXISTS fk_metadata_stream_ref;
ALTER TABLE taxonomies DROP CONSTRAINT IF EXISTS fk_taxonomies_stream_ref;
ALTER TABLE taxonomies DROP CONSTRAINT IF EXISTS fk_taxonomies_child_stream_ref;
ALTER TABLE primitive_events DROP CONSTRAINT IF EXISTS fk_primitive_stream_ref;
ALTER TABLE streams DROP CONSTRAINT IF EXISTS fk_streams_data_provider;

-- 2. Drop primary key constraints
DROP TABLE IF EXISTS data_providers;

-- 3. Drop all indexes on these columns
DROP INDEX IF EXISTS streams_provider_stream_idx;
DROP INDEX IF EXISTS streams_id_idx;
DROP INDEX IF EXISTS meta_stream_to_key_idx;
DROP INDEX IF EXISTS meta_stream_to_ref_idx;
DROP INDEX IF EXISTS meta_key_ref_to_stream_idx;
DROP INDEX IF EXISTS tax_child_unique_idx;
DROP INDEX IF EXISTS tax_stream_start_idx;
DROP INDEX IF EXISTS tax_child_stream_idx;
DROP INDEX IF EXISTS tax_latest_sequence_idx;
DROP INDEX IF EXISTS pe_gap_fill_idx;
DROP INDEX IF EXISTS pe_stream_created_idx;

-- 4. Drop UUID columns
ALTER TABLE streams DROP COLUMN IF EXISTS id;
ALTER TABLE streams DROP COLUMN IF EXISTS data_provider_id;
ALTER TABLE metadata DROP COLUMN IF EXISTS stream_ref;
ALTER TABLE taxonomies DROP COLUMN IF EXISTS stream_ref;
ALTER TABLE taxonomies DROP COLUMN IF EXISTS child_stream_ref;
ALTER TABLE primitive_events DROP COLUMN IF EXISTS stream_ref;