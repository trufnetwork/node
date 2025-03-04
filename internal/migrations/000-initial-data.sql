/* 
    INITIAL MIGRATION FILE

    The intention of this file is to store only tables and constraints that will be used on TN bootstrap.
    Actions should be added in separate files for better readability.
 */
CREATE TABLE streams (
    stream_id TEXT NOT NULL,
    data_provider TEXT NOT NULL,
    stream_type TEXT NOT NULL,

    -- indexes and constraints
    PRIMARY KEY (data_provider, stream_id),
    INDEX stream_type_idx (stream_type)

    CONSTRAINT check_stream_type CHECK (stream_type IN ('primitive', 'composed')),
    -- valid ethereum addresses
    CONSTRAINT check_data_provider CHECK (data_provider ~ '^0x[a-fA-F0-9]{40}$'),
    -- valid stream ids like "st123...321"
    CONSTRAINT check_stream_id CHECK (stream_id ~ '^st[a-z0-9]{30}$')
);

CREATE TABLE taxonomies (
    data_provider TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    taxonomy_id UUID NOT NULL,
    child_stream_id TEXT NOT NULL,
    child_data_provider TEXT NOT NULL,
    weight DECIMAL(36, 18) NOT NULL,
    created_at INT NOT NULL,
    disabled_at INT,
    version INT NOT NULL,
    start_ts INT NOT NULL,

    PRIMARY KEY (taxonomy_id),
    FOREIGN KEY fk_stream (data_provider, stream_id)
        REFERENCES streams(data_provider, stream_id)
        ON DELETE CASCADE,
    FOREIGN KEY fk_child_stream (child_data_provider, child_stream_id)
        REFERENCES streams(data_provider, stream_id)
        -- this means that if some stream is deleted, the taxonomies with reference will persist
        -- it will be up to the composer to decide what to do with them, instead of changing everyone's taxonomies
        ON DELETE DO NOTHING,
    INDEX child_stream_idx (data_provider, stream_id, start_ts, version, child_data_provider, child_stream_id)

    CONSTRAINT check_weight CHECK (weight >= 0),
    CONSTRAINT check_version CHECK (version >= 0),
    CONSTRAINT check_start_ts CHECK (start_ts >= 0)
);


CREATE TABLE primitive_events (
    stream_id TEXT NOT NULL,
    data_provider TEXT NOT NULL,
    ts INT NOT NULL,     -- unix timestamp
    value DECIMAL(36, 18) NOT NULL,
    created_at INT NOT NULL, -- based on blockheight

    -- indexes and constraints
    PRIMARY KEY (data_provider, stream_id, ts, created_at),
    FOREIGN KEY fk_stream (data_provider, stream_id)
        REFERENCES streams(data_provider, stream_id)
        ON DELETE CASCADE,
    INDEX ts_idx (ts),
    INDEX created_at_idx (created_at)
);

CREATE TABLE metadata (
    row_id UUID NOT NULL,
    data_provider TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    metadata_key TEXT NOT NULL,
    value_i INT,
    value_f DECIMAL(36, 18),
    value_b BOOL,
    value_s TEXT,
    value_ref TEXT,
    created_at INT NOT NULL, -- block height
    disabled_at INT, -- block height

    -- indexes and constraints
    -- for fetching a specific metadata row or delete it
    PRIMARY KEY (row_id),
    FOREIGN KEY fk_stream (data_provider, stream_id)
        REFERENCES streams(data_provider, stream_id)
        ON DELETE CASCADE,
    -- for fetching a specific stream's key-value pairs, or just the latest
    INDEX stream_key_created_idx (data_provider, stream_id, metadata_key, created_at DESC),
    -- for fetching a specific stream's key-value pairs by reference
    -- e.g. which users can read this stream?
    INDEX stream_ref_idx (data_provider, stream_id, metadata_key, value_ref),
    -- for fetching only by reference
    -- e.g. which streams can this wallet read from?
    INDEX ref_idx (metadata_key, value_ref, data_provider, stream_id)
);