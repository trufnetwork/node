-- Create unique constraint on streams.id before creating foreign keys that reference it
-- Note: We use a unique index instead of a constraint since that's what the schema expects
CREATE UNIQUE INDEX IF NOT EXISTS streams_id_idx ON streams(id);

ALTER TABLE
    metadata
ADD
    CONSTRAINT fk_metadata_stream_ref FOREIGN KEY (stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

ALTER TABLE
    taxonomies
ADD
    CONSTRAINT fk_taxonomies_stream_ref FOREIGN KEY (stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

ALTER TABLE
    taxonomies
ADD
    CONSTRAINT fk_taxonomies_child_stream_ref FOREIGN KEY (child_stream_ref) REFERENCES streams(id) ON DELETE CASCADE;

-- note: this won't work until kwil supports partial indexes
create index pe_missing_ref_by_created_idx
    on primitive_events (created_at)
    where (stream_ref IS NULL);


-- drop this index as it confuses the query planner
DROP INDEX IF EXISTS pe_prov_stream_created_idx;
-- recreate it with all expected columns
CREATE INDEX IF NOT EXISTS pe_prov_stream_created_idx ON primitive_events 
(data_provider, stream_id, created_at, event_time);
