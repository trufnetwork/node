/*
 * DIGEST SCHEMA MIGRATION
 * 
 * Creates the two essential tables needed for the digest system:
 * - primitive_event_type: Marks which records are CLOSE/HIGH/LOW or combination of them after digest
 * - pending_prune_days: Simple queue of days needing digest processing
 *   (row presence = pending, row deletion = complete)
 * - digest_config: Configuration for the digest system
 */

CREATE TABLE IF NOT EXISTS primitive_event_type (
    stream_ref INT NOT NULL,
    event_time INT8 NOT NULL,
    type INT4 NOT NULL,
    
    CONSTRAINT pk_primitive_event_type PRIMARY KEY (stream_ref, event_time),
    CONSTRAINT fk_pet_stream_ref FOREIGN KEY (stream_ref) 
        REFERENCES streams(id) ON DELETE CASCADE
);

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

-- Create single-row config table
CREATE TABLE IF NOT EXISTS digest_config (
    id INT8 PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    enabled BOOL NOT NULL DEFAULT false,
    digest_schedule TEXT NOT NULL,
    updated_at_height INT8 NOT NULL DEFAULT 0
);

-- Seed the row rather than leaving it to whoever brings the network up. Leaving
-- it out is how a network reaches the state testnet was in: no row, so the
-- scheduler reads "disabled" and stops, which is indistinguishable in the logs
-- from digest being deliberately off. Nobody noticed until the queue had been
-- filling since 2010.
--
-- Shipped disabled, so this creates the row without starting digest anywhere,
-- and ON CONFLICT leaves a network that already has one exactly as it is --
-- including its own schedule. Turning digest on stays an operator decision made
-- through a signed exec-sql.
--
-- digest_schedule is NOT NULL with no default, which is the reason a row could
-- not simply be conjured by the DEFAULTs the way duplicate_prune_config's is in
-- 056. The value below matches DefaultDigestSchedule in
-- extensions/tn_digest/constants.go, which is what the extension already falls
-- back to when the schedule comes back empty.
INSERT INTO digest_config (id, enabled, digest_schedule)
VALUES (1, false, '0 */6 * * *')
ON CONFLICT (id) DO NOTHING;


