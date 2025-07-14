/*----------------------------------------------------------------------
 * Streams
 *---------------------------------------------------------------------*/

ALTER TABLE streams
ADD COLUMN id UUID,
ADD COLUMN data_provider_id UUID;

-- reference data_provider_id to data_providers table
ALTER TABLE streams
ADD CONSTRAINT fk_streams_data_provider 
FOREIGN KEY (data_provider_id) REFERENCES data_providers(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS streams_provider_stream_idx ON streams(data_provider_id, stream_id);
CREATE UNIQUE INDEX IF NOT EXISTS streams_id_idx ON streams(id);

UPDATE streams 
SET data_provider_id = dp.id
FROM data_providers dp 
WHERE streams.data_provider = dp.address;

/*----------------------------------------------------------------------
 * Metadata
 *---------------------------------------------------------------------*/

ALTER TABLE metadata
ADD COLUMN stream_ref UUID;

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
ADD COLUMN stream_ref UUID,
ADD COLUMN child_stream_ref UUID;

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