/* 
    INITIAL MIGRATION FILE

    The intention of this file is to store only tables and constraints that will be used on TN bootstrap.
    Actions should be added in separate files for better readability.
    
    Tables:
    - streams: Core table storing stream metadata with immutable data provider references
    - taxonomies: Defines parent-child relationships between streams with versioning
    - primitive_events: Stores time-series data points for primitive streams
    - metadata: Flexible key-value store for stream configuration and properties
 */
CREATE TABLE IF NOT EXISTS streams (
    stream_id TEXT NOT NULL,
    -- data_provider != stream_owner
    -- data_provider == creator of the stream
    -- important because we want immutable reference, while ownership can be transferred
    data_provider TEXT NOT NULL,
    stream_type TEXT NOT NULL,
    created_at INT8 NOT NULL,

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
CREATE INDEX IF NOT EXISTS streams_type_idx ON streams (stream_type, data_provider, stream_id);

-- create index to check latest streams
CREATE INDEX IF NOT EXISTS streams_latest_idx ON streams (created_at, data_provider, stream_id);

CREATE TABLE IF NOT EXISTS taxonomies (
    data_provider TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    taxonomy_id UUID NOT NULL,
    child_data_provider TEXT NOT NULL,
    child_stream_id TEXT NOT NULL,
    weight NUMERIC(36, 18) NOT NULL,
    created_at INT8 NOT NULL,
    disabled_at INT8,
    -- group_sequence is the number of taxonomies that have been created for this parent stream
    group_sequence INT8 NOT NULL,
    start_time INT8 NOT NULL,

    PRIMARY KEY (taxonomy_id),
    FOREIGN KEY (data_provider, stream_id)
        REFERENCES streams(data_provider, stream_id)
        ON DELETE CASCADE,
    -- don't add child data providers as foreign keys, because we want
    -- to allow taxonomies to be adjusted independently of streams existing or being deleted
    -- we create them as unique index instead

    CHECK (weight >= 0),
    CHECK (group_sequence >= 0),
    CHECK (start_time >= 0)
);

-- Create indexes separately
CREATE UNIQUE INDEX IF NOT EXISTS tax_child_uniq_idx ON taxonomies (data_provider, stream_id, start_time, group_sequence, child_data_provider, child_stream_id);
-- TODO: Add this back in when we support where clause
-- CREATE INDEX IF NOT EXISTS active_child_stream_idx ON taxonomies (data_provider, stream_id)
-- WHERE disabled_at IS NULL;
-- for now, we just index disabled_at
CREATE INDEX IF NOT EXISTS tax_disabled_idx ON taxonomies (disabled_at);

-- For faster taxonomy queries filtering by provider/stream/start_time and disabled status
CREATE INDEX IF NOT EXISTS tax_dp_stream_start_idx 
ON taxonomies (data_provider, stream_id, start_time, disabled_at);

-- For efficient recursive lookups
CREATE INDEX IF NOT EXISTS tax_child_lookup_idx
ON taxonomies (child_data_provider, child_stream_id);

-- optimizes "get latest taxonomy group_sequence"
CREATE INDEX IF NOT EXISTS tax_latest_group_sequence_idx ON taxonomies (data_provider, stream_id, start_time, group_sequence);

CREATE TABLE IF NOT EXISTS primitive_events (
    stream_id TEXT NOT NULL,
    data_provider TEXT NOT NULL,
    event_time INT8 NOT NULL, -- unix timestamp
    value NUMERIC(36, 18) NOT NULL,
    created_at INT8 NOT NULL, -- based on blockheight
    truflation_created_at TEXT, -- RFC3339 formatted timestamp, i.e. 2023-10-01T00:00:00Z

    PRIMARY KEY (data_provider, stream_id, event_time, created_at),
    FOREIGN KEY (data_provider, stream_id)
        REFERENCES streams(data_provider, stream_id)
        ON DELETE CASCADE
);

/* Create indexes separately for primitive_events */

-- Add optimized index for gap-filling queries
CREATE INDEX IF NOT EXISTS pe_gap_filling_idx ON primitive_events 
(data_provider, stream_id, event_time);

-- For queries filtering by provider/stream and created_at (for frozen_at queries)
CREATE INDEX IF NOT EXISTS pe_prov_stream_created_idx ON primitive_events 
(data_provider, stream_id, created_at);

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

/* Create indexes separately for metadata */

-- For fetching a specific stream's key-value pairs, or just the latest
CREATE INDEX IF NOT EXISTS meta_stream_key_idx ON metadata (data_provider, stream_id, metadata_key, created_at);

-- For fetching a specific stream's key-value pairs by reference
CREATE INDEX IF NOT EXISTS meta_stream_ref_idx ON metadata (data_provider, stream_id, metadata_key, value_ref);

-- For fetching only by reference when metadata_key is the primary filter
CREATE INDEX IF NOT EXISTS meta_key_ref_idx ON metadata (metadata_key, value_ref, data_provider, stream_id);


-- TODO: Add this back in when we support where clause
-- For efficiently querying only active (non-disabled) metadata records
-- Reduces scan size when disabled records are excluded from results
-- CREATE INDEX IF NOT EXISTS active_metadata_idx ON metadata 
-- (data_provider, stream_id, metadata_key)
-- WHERE disabled_at IS NULL;
-- for now, we just index disabled_at
CREATE INDEX IF NOT EXISTS meta_disabled_idx ON metadata (disabled_at);