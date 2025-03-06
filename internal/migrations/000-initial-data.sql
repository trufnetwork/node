/* 
    INITIAL MIGRATION FILE

    The intention of this file is to store only tables and constraints that will be used on TN bootstrap.
    Actions should be added in separate files for better readability.
 */
CREATE TABLE IF NOT EXISTS streams (
    stream_id TEXT NOT NULL,
    data_provider TEXT NOT NULL,
    stream_type TEXT NOT NULL,

    -- Primary key must be defined inline
    PRIMARY KEY (data_provider, stream_id),

    -- Constraints
    CHECK (stream_type IN ('primitive', 'composed')),
    -- as close as we can get from ethereum addresses
    CHECK (data_provider LIKE '0x%' AND LENGTH(data_provider) = 42),
    -- valid stream ids - must start with 'st' followed by 30 characters
    CHECK (LENGTH(stream_id) = 32 AND substring(stream_id, 1, 2) = 'st')
);

-- Create indexes separately
CREATE INDEX IF NOT EXISTS stream_type_idx ON streams (stream_type);

CREATE TABLE IF NOT EXISTS taxonomies (
    data_provider TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    taxonomy_id UUID NOT NULL,
    child_stream_id TEXT NOT NULL,
    child_data_provider TEXT NOT NULL,
    weight NUMERIC(36, 18) NOT NULL,
    created_at INT8 NOT NULL,
    disabled_at INT8,
    version INT8 NOT NULL,
    start_time INT8 NOT NULL,

    PRIMARY KEY (taxonomy_id),
    FOREIGN KEY (data_provider, stream_id)
        REFERENCES streams(data_provider, stream_id)
        ON DELETE CASCADE,
    FOREIGN KEY (child_data_provider, child_stream_id)
        REFERENCES streams(data_provider, stream_id)
        -- we don't want to taxonomies to change if a stream is deleted
        ON DELETE NO ACTION,

    CHECK (weight >= 0),
    CHECK (version >= 0),
    CHECK (start_time >= 0)
);

-- Create indexes separately
CREATE INDEX IF NOT EXISTS child_stream_idx ON taxonomies (data_provider, stream_id, start_time, version, child_data_provider, child_stream_id);

CREATE TABLE IF NOT EXISTS primitive_events (
    stream_id TEXT NOT NULL,
    data_provider TEXT NOT NULL,
    event_time INT8 NOT NULL, -- unix timestamp
    value NUMERIC(36, 18) NOT NULL,
    created_at INT8 NOT NULL, -- based on blockheight

    PRIMARY KEY (data_provider, stream_id, event_time, created_at),
    FOREIGN KEY (data_provider, stream_id)
        REFERENCES streams(data_provider, stream_id)
        ON DELETE CASCADE
);

-- Create indexes separately
CREATE INDEX IF NOT EXISTS event_time_idx ON primitive_events (event_time);
CREATE INDEX IF NOT EXISTS created_at_idx ON primitive_events (created_at);

CREATE TABLE IF NOT EXISTS metadata (
    row_id UUID NOT NULL,
    data_provider TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    metadata_key TEXT NOT NULL,
    value_i INT8,
    value_f NUMERIC(36, 18),
    value_b BOOLEAN,
    value_s TEXT,
    value_ref TEXT,
    created_at INT8 NOT NULL, -- block height
    disabled_at INT8, -- block height

    PRIMARY KEY (row_id),
    FOREIGN KEY (data_provider, stream_id)
        REFERENCES streams(data_provider, stream_id)
        ON DELETE CASCADE
);

-- Create indexes separately
-- for fetching a specific stream's key-value pairs, or just the latest
CREATE INDEX IF NOT EXISTS stream_key_created_idx ON metadata (data_provider, stream_id, metadata_key, created_at);
-- for fetching a specific stream's key-value pairs by reference
CREATE INDEX IF NOT EXISTS stream_ref_idx ON metadata (data_provider, stream_id, metadata_key, value_ref);
-- for fetching only by reference
CREATE INDEX IF NOT EXISTS ref_idx ON metadata (metadata_key, value_ref, data_provider, stream_id);