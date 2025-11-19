-- Add data_provider column to attestations table
ALTER TABLE attestations
ADD COLUMN IF NOT EXISTS data_provider TEXT NOT NULL DEFAULT '';

-- Add stream_id column to attestations table
ALTER TABLE attestations
ADD COLUMN IF NOT EXISTS stream_id TEXT NOT NULL DEFAULT '';

-- Create index on data_provider for efficient querying
CREATE INDEX IF NOT EXISTS ix_att_data_provider
    ON attestations(data_provider);

-- Create index on stream_id for efficient querying
CREATE INDEX IF NOT EXISTS ix_att_stream_id
    ON attestations(stream_id);

-- Create composite index for data_provider + stream_id queries
CREATE INDEX IF NOT EXISTS ix_att_provider_stream
    ON attestations(data_provider, stream_id);
