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