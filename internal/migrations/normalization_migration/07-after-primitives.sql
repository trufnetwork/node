-- include final indexes and constraints for real usage
CREATE INDEX IF NOT EXISTS pe_gap_fill_idx ON primitive_events (stream_ref, event_time, created_at);

CREATE INDEX IF NOT EXISTS pe_stream_created_idx ON primitive_events (stream_ref, created_at);

ALTER TABLE
    primitive_events
ADD
    CONSTRAINT fk_primitive_stream_ref FOREIGN KEY (stream_ref) REFERENCES streams(id) ON DELETE CASCADE;