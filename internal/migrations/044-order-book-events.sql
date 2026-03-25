/*
 * ORDER BOOK EVENT HISTORY
 *
 * Creates the ob_order_events table to record individual order events
 * (placement, cancellation, fill, settlement) for wallet history queries.
 *
 * This table is EPHEMERAL on-chain — the indexer syncs events to permanent
 * storage, then trim_order_events() deletes old rows to bound node storage.
 * This follows the same pattern as tn_digest (write → index → trim).
 */

-- =============================================================================
-- ob_order_events: Individual order event log
-- =============================================================================
CREATE TABLE IF NOT EXISTS ob_order_events (
    id INT PRIMARY KEY,
    tx_hash BYTEA NOT NULL,
    query_id INT NOT NULL,
    participant_id INT NOT NULL,
    event_type TEXT NOT NULL,
    outcome BOOLEAN NOT NULL,
    price INT NOT NULL,
    amount INT8 NOT NULL,
    counterparty_id INT,
    block_height INT8 NOT NULL,
    block_timestamp INT8 NOT NULL,

    FOREIGN KEY (query_id) REFERENCES ob_queries(id),
    FOREIGN KEY (participant_id) REFERENCES ob_participants(id)
);

-- Index for indexer sequential sync (same pattern as ob_net_impacts)
CREATE INDEX IF NOT EXISTS idx_ob_order_events_id ON ob_order_events(id);

-- Index for trim operations (delete by block_height)
CREATE INDEX IF NOT EXISTS idx_ob_order_events_height ON ob_order_events(block_height);

-- =============================================================================
-- ob_record_order_event: Helper to insert a single order event
-- =============================================================================
CREATE OR REPLACE ACTION ob_record_order_event(
    $query_id INT,
    $participant_id INT,
    $event_type TEXT,
    $outcome BOOLEAN,
    $price INT,
    $amount INT8,
    $counterparty_id INT
) PRIVATE {
    $next_id INT;
    for $row in SELECT COALESCE(MAX(id), 0::INT) + 1 as val FROM ob_order_events {
        $next_id := $row.val;
    }

    INSERT INTO ob_order_events (
        id, tx_hash, query_id, participant_id, event_type,
        outcome, price, amount, counterparty_id,
        block_height, block_timestamp
    ) VALUES (
        $next_id, decode(@txid, 'hex'), $query_id, $participant_id, $event_type,
        $outcome, $price, $amount, $counterparty_id,
        @height, @block_timestamp
    );
};

-- =============================================================================
-- trim_order_events: Delete old events after indexer has synced them
-- =============================================================================
/**
 * Called by the tn_digest scheduler (leader-only) to trim old order events.
 * Uses block_height cutoff with a configurable buffer to ensure the indexer
 * has had time to sync before deletion.
 *
 * Parameters:
 * - $preserve_blocks: Number of recent blocks to keep (e.g., 172800 = ~2 days)
 * - $delete_cap: Maximum rows to delete per invocation (prevents large txs)
 *
 * Returns via NOTICE: deleted count, remaining count, has_more flag
 */
CREATE OR REPLACE ACTION trim_order_events(
    $preserve_blocks INT8,
    $delete_cap INT
) PUBLIC owner {
    $cutoff INT8 := @height - $preserve_blocks;

    -- Don't trim if cutoff is negative (chain is younger than preserve window)
    if $cutoff <= 0 {
        NOTICE('trim_order_events: cutoff<=0, nothing to trim');
        RETURN;
    }

    $count INT;
    for $row in SELECT count(*)::INT as cnt FROM ob_order_events WHERE block_height < $cutoff {
        $count := $row.cnt;
    }

    if $count = 0 {
        NOTICE('trim_order_events: deleted=0 remaining=0 has_more=false');
        RETURN;
    }

    $to_delete INT;
    if $count > $delete_cap {
        $to_delete := $delete_cap;
    } else {
        $to_delete := $count;
    }

    DELETE FROM ob_order_events
    WHERE id IN (
        SELECT id FROM ob_order_events
        WHERE block_height < $cutoff
        ORDER BY id ASC
        LIMIT $to_delete
    );

    $remaining INT := $count - $to_delete;
    $has_more BOOL := $count > $delete_cap;
    NOTICE('trim_order_events: deleted=' || $to_delete::TEXT
        || ' remaining=' || $remaining::TEXT
        || ' has_more=' || $has_more::TEXT);
};
