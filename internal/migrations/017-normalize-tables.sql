/*----------------------------------------------------------------------
 * Streams
 *---------------------------------------------------------------------*/

ALTER TABLE streams
ADD COLUMN id INT,
ADD COLUMN data_provider_id INT;

-- reference data_provider_id to data_providers table
ALTER TABLE streams
ADD CONSTRAINT fk_streams_data_provider 
FOREIGN KEY (data_provider_id) REFERENCES data_providers(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS streams_provider_stream_idx ON streams(data_provider_id, stream_id);
CREATE UNIQUE INDEX IF NOT EXISTS streams_id_idx ON streams(id);

/*----------------------------------------------------------------------
 * Metadata
 *---------------------------------------------------------------------*/

ALTER TABLE metadata
ADD COLUMN stream_ref INT;

ALTER TABLE metadata
ADD CONSTRAINT fk_metadata_stream_ref
FOREIGN KEY (stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS meta_stream_to_key_idx ON metadata(stream_ref, metadata_key, created_at);
CREATE INDEX IF NOT EXISTS meta_stream_to_ref_idx ON metadata(stream_ref, metadata_key, value_ref);
CREATE INDEX IF NOT EXISTS meta_key_ref_to_stream_idx ON metadata(metadata_key, value_ref, stream_ref);

/*----------------------------------------------------------------------
 * Taxonomies
 *---------------------------------------------------------------------*/

ALTER TABLE taxonomies
ADD COLUMN stream_ref INT,
ADD COLUMN child_stream_ref INT;

ALTER TABLE taxonomies
ADD CONSTRAINT fk_taxonomies_stream_ref
FOREIGN KEY (stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

ALTER TABLE taxonomies
ADD CONSTRAINT fk_taxonomies_child_stream_ref
FOREIGN KEY (child_stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS tax_child_unique_idx ON taxonomies (stream_ref, start_time, group_sequence, child_stream_ref);
CREATE INDEX IF NOT EXISTS tax_stream_start_idx ON taxonomies (stream_ref, start_time, disabled_at);
CREATE INDEX IF NOT EXISTS tax_child_stream_idx ON taxonomies (child_stream_ref);
CREATE INDEX IF NOT EXISTS tax_latest_sequence_idx ON taxonomies (stream_ref, start_time, group_sequence);

/*----------------------------------------------------------------------
 * Primitive Events
 *---------------------------------------------------------------------*/

ALTER TABLE primitive_events
ADD COLUMN stream_ref INT;

ALTER TABLE primitive_events
ADD CONSTRAINT fk_primitive_stream_ref
FOREIGN KEY (stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

/*----------------------------------------------------------------------
 * Cleanup legacy columns and adjust primary keys
 *---------------------------------------------------------------------*/

-- Metadata: drop denormalized identifiers now that stream_ref is established
ALTER TABLE metadata DROP COLUMN IF EXISTS data_provider;
ALTER TABLE metadata DROP COLUMN IF EXISTS stream_id;

-- Taxonomies: drop denormalized identifiers now that refs exist
ALTER TABLE taxonomies DROP COLUMN IF EXISTS data_provider;
ALTER TABLE taxonomies DROP COLUMN IF EXISTS stream_id;
ALTER TABLE taxonomies DROP COLUMN IF EXISTS child_data_provider;
ALTER TABLE taxonomies DROP COLUMN IF EXISTS child_stream_id;

-- Primitive events: drop legacy composite references and primary key
ALTER TABLE primitive_events DROP CONSTRAINT IF EXISTS primitive_events_pkey;
DROP INDEX IF EXISTS pe_prov_stream_created_idx;
ALTER TABLE primitive_events DROP COLUMN IF EXISTS data_provider;
ALTER TABLE primitive_events DROP COLUMN IF EXISTS stream_id;

-- Streams: switch primary key to id and drop old composite identifiers
DROP INDEX IF EXISTS streams_type_idx;
DROP INDEX IF EXISTS streams_latest_idx;
ALTER TABLE streams DROP CONSTRAINT IF EXISTS streams_pkey;
ALTER TABLE streams ADD CONSTRAINT streams_pkey PRIMARY KEY (id);

-- add composite index on data_provider_id and stream_id
-- it will still be necessary for lookups
CREATE INDEX IF NOT EXISTS streams_dp_stream_id_idx ON streams (data_provider_id, stream_id);

/*----------------------------------------------------------------------
 * Final integrity and performance indexes
 *---------------------------------------------------------------------*/
CREATE INDEX IF NOT EXISTS streams_data_provider_id_idx ON streams (data_provider_id);

-- Recreate equivalent indexes under the new column layout
CREATE INDEX IF NOT EXISTS streams_type_idx ON streams (stream_type, data_provider_id, id);
CREATE INDEX IF NOT EXISTS streams_latest_idx ON streams (created_at, data_provider_id, id);

-- Performance indexes for common query patterns
-- Optimize standard time-series queries (get_record_primitive, get_record_composed)
CREATE INDEX IF NOT EXISTS primitive_events_query_idx ON primitive_events (stream_ref, event_time, created_at);

-- Optimize Truflation-specific frozen queries (truflation_get_record_primitive)
CREATE INDEX IF NOT EXISTS primitive_events_truflation_idx ON primitive_events (stream_ref, event_time, truflation_created_at);

-- Optimize global stream listing ordered by stream_id (list_streams)
CREATE INDEX IF NOT EXISTS streams_stream_id_idx ON streams (stream_id);

-- Optimize listing role members ordered by grant time (list_role_members)
CREATE INDEX IF NOT EXISTS role_members_granted_at_idx ON role_members (owner, role_name, granted_at);