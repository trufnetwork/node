-- Create unique constraint on streams.id before creating foreign keys that reference it
-- Note: We use a unique index instead of a constraint since that's what the schema expects
-- CREATE UNIQUE INDEX IF NOT EXISTS streams_id_idx ON streams(id);

-- ALTER TABLE
--     metadata
-- ADD
--     CONSTRAINT fk_metadata_stream_ref FOREIGN KEY (stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

-- ALTER TABLE
--     taxonomies
-- ADD
--     CONSTRAINT fk_taxonomies_stream_ref FOREIGN KEY (stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

-- ALTER TABLE
--     taxonomies
-- ADD
--     CONSTRAINT fk_taxonomies_child_stream_ref FOREIGN KEY (child_stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

-- create partial index on primitive_events.stream_ref
-- ONLY if kwil starts supporting this
-- CREATE INDEX IF NOT EXISTS
--     idx_primitive_events_missing_ref
-- ON  primitive_events (data_provider, stream_id, event_time, created_at)
-- WHERE stream_ref IS NULL;
-- considering 20K updates (without vacuum analyze streams and primitive_events)
-- with this, we can get between 6~10 seconds.
-- without this, 11~14 seconds (Risk: time increases with more rows that are not null)

create index pe_missing_ref_by_created_idx
    on primitive_events (created_at)
    where (stream_ref IS NULL);


-- drop this index as it confuses the query planner
DROP INDEX IF EXISTS pe_prov_stream_created_idx;
-- recreate it with all expected columns
CREATE INDEX IF NOT EXISTS pe_prov_stream_created_idx ON primitive_events 
(data_provider, stream_id, created_at, event_time);
