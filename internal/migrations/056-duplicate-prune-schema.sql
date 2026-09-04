/*
 * DUPLICATE PRUNE SCHEMA MIGRATION
 *
 * A stream that publishes the same value every day stores one row a day forever.
 * Reads carry the last observation forward, so every row after the first in such
 * a run answers exactly what the row before it already answered. This migration
 * creates the configuration the pruner in 057 reads; the rule itself lives there.
 *
 * Nothing is deleted until an operator turns `enabled` on, which ships false.
 */

CREATE TABLE IF NOT EXISTS duplicate_prune_config (
    id INT8 PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    -- Read by the scheduler, not by the actions. The actions are leader-gated and
    -- do what they are told, so an operator can still drain by hand through a
    -- signed exec-sql while the scheduled sweep stays off.
    enabled BOOL NOT NULL DEFAULT false,
    -- A record is only ever a candidate once it is this far in the past. 30 days
    -- covers 98.8% of the duplicates on mainnet; a tighter window buys about 1%.
    retention_days INT NOT NULL DEFAULT 30,
    prune_schedule TEXT NOT NULL DEFAULT '0 */6 * * *',
    -- Where the sweep is. Duplicate-ness is a property of a whole stream rather
    -- than of one day, so there is no queue to drain the way digest has: the
    -- pruner walks streams.id in order and wraps at the end. A stream with
    -- nothing to prune costs one bounded query.
    last_stream_ref INT NOT NULL DEFAULT 0,
    updated_at_height INT8 NOT NULL DEFAULT 0,

    CONSTRAINT chk_dpc_retention_days_positive CHECK (retention_days > 0),
    CONSTRAINT chk_dpc_cursor_not_negative CHECK (last_stream_ref >= 0)
);

-- Seeded here on purpose. digest_config went unseeded for years, and the
-- consequence was a network where digest had simply never run because nobody
-- noticed the row was missing. Creating it disabled costs nothing and removes
-- that failure mode; 019 now seeds its own row the same way.
INSERT INTO duplicate_prune_config (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
