/*
 * DIGEST SCHEMA MIGRATION
 * 
 * Creates the two essential tables needed for the digest system:
 * - primitive_event_type: Marks which records are CLOSE/HIGH/LOW or combination of them after digest
 * - pending_prune_days: Simple queue of days needing digest processing
 *   (row presence = pending, row deletion = complete)
 */

CREATE TABLE IF NOT EXISTS primitive_event_type (
    stream_ref INT NOT NULL,
    event_time INT8 NOT NULL,
    type INT4 NOT NULL,
    
    CONSTRAINT pk_primitive_event_type PRIMARY KEY (stream_ref, event_time),
    CONSTRAINT fk_pet_stream_ref FOREIGN KEY (stream_ref) 
        REFERENCES streams(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pet_stream_time 
    ON primitive_event_type(stream_ref, event_time);

CREATE TABLE IF NOT EXISTS pending_prune_days (
    stream_ref INT NOT NULL,
    day_index INT NOT NULL,
    
    CONSTRAINT pk_pending_prune_days PRIMARY KEY (stream_ref, day_index),
    CONSTRAINT fk_ppd_stream_ref FOREIGN KEY (stream_ref) 
        REFERENCES streams(id) ON DELETE CASCADE,
    CONSTRAINT chk_ppd_day_index_valid CHECK (day_index >= 0)
);

CREATE INDEX IF NOT EXISTS idx_ppd_processing_order 
    ON pending_prune_days(day_index, stream_ref);