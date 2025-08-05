-- Purpose: before the migration incident, part of the migration was already done. Here we're preparing for the migration
-- and also making the IDs type int instead of current uuid.

-- this index is necessary for the migration
CREATE UNIQUE INDEX IF NOT EXISTS streams_provider_stream_idx ON streams(data_provider_id, stream_id);

DROP INDEX IF EXISTS pe_gap_filling_idx; -- this is redundant and should not exist
-- this should only be necessary after all migrations
DROP INDEX IF EXISTS pe_gap_fill_idx; -- stream_ref, event_time
DROP INDEX IF EXISTS pe_stream_created_idx; -- stream_ref, created_at

-- Drop existing constraints first
ALTER TABLE streams DROP CONSTRAINT IF EXISTS fk_streams_data_provider;
ALTER TABLE metadata DROP CONSTRAINT IF EXISTS fk_metadata_stream_ref;
ALTER TABLE taxonomies DROP CONSTRAINT IF EXISTS fk_taxonomies_stream_ref;
ALTER TABLE taxonomies DROP CONSTRAINT IF EXISTS fk_taxonomies_child_stream_ref;
ALTER TABLE primitive_events DROP CONSTRAINT IF EXISTS fk_primitive_stream_ref;


-- remake data_providers table
-- First drop the primary key constraint before renaming the column
ALTER TABLE data_providers DROP CONSTRAINT data_providers_pkey;

-- rename data_providers.id column to old_id
ALTER TABLE data_providers RENAME COLUMN id TO old_id;

-- create a new column for data_providers.id that is an int
ALTER TABLE data_providers ADD COLUMN id INT;

-- populate the new column with the increasing ids
WITH with_ids AS (
    SELECT old_id, ROW_NUMBER() OVER (ORDER BY old_id) as id
    FROM data_providers
)
UPDATE data_providers
SET id = with_ids.id
FROM with_ids
WHERE data_providers.old_id = with_ids.old_id;

-- drop old_id column
ALTER TABLE data_providers DROP COLUMN IF EXISTS old_id;

-- make this column the primary key
ALTER TABLE data_providers ADD CONSTRAINT data_providers_pkey PRIMARY KEY (id);

-- drop old id columns
ALTER TABLE streams DROP COLUMN IF EXISTS id;
ALTER TABLE streams DROP COLUMN IF EXISTS data_provider_id;
ALTER TABLE metadata DROP COLUMN IF EXISTS stream_ref;
ALTER TABLE taxonomies DROP COLUMN IF EXISTS stream_ref;
ALTER TABLE taxonomies DROP COLUMN IF EXISTS child_stream_ref;
ALTER TABLE primitive_events DROP COLUMN IF EXISTS stream_ref;

-- Create new INT columns 
ALTER TABLE streams ADD COLUMN id INT;
ALTER TABLE streams ADD COLUMN data_provider_id INT;
ALTER TABLE metadata ADD COLUMN stream_ref INT;
ALTER TABLE taxonomies ADD COLUMN stream_ref INT;
ALTER TABLE taxonomies ADD COLUMN child_stream_ref INT;
ALTER TABLE primitive_events ADD COLUMN stream_ref INT;

-- add new references to the new id column
ALTER TABLE streams ADD CONSTRAINT fk_streams_data_provider
FOREIGN KEY (data_provider_id) REFERENCES data_providers(id) ON DELETE CASCADE;


-- add indexes for improved performance
CREATE INDEX IF NOT EXISTS meta_stream_to_key_idx ON metadata(stream_ref, metadata_key, created_at);
CREATE INDEX IF NOT EXISTS meta_stream_to_ref_idx ON metadata(stream_ref, metadata_key, value_ref);
CREATE INDEX IF NOT EXISTS meta_key_ref_to_stream_idx ON metadata(metadata_key, value_ref, stream_ref);

CREATE UNIQUE INDEX IF NOT EXISTS tax_child_unique_idx ON taxonomies (stream_ref, start_time, group_sequence, child_stream_ref);
CREATE INDEX IF NOT EXISTS tax_stream_start_idx ON taxonomies (stream_ref, start_time, disabled_at);
CREATE INDEX IF NOT EXISTS tax_child_stream_idx ON taxonomies (child_stream_ref);
CREATE INDEX IF NOT EXISTS tax_latest_sequence_idx ON taxonomies (stream_ref, start_time, group_sequence);

