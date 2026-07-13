/**
 * =============================================================================
 * Transaction Ledger Retention
 * =============================================================================
 *
 * Bounds growth of main.transaction_events (and its CASCADE child
 * transaction_event_distributions) — a consensus-state table that otherwise
 * accumulates a row per fee event forever, replicated to every validator and
 * captured in snapshots/statesync.
 *
 * Only the high-volume write-fee rows are trimmed: method_id = 2 (insertRecords,
 * written by both insert_records and the truflation insert path). These are a
 * flat 1 TRUF -> leader per write, so the per-tx ledger row is uniform and the
 * transaction itself remains permanently resolvable off-chain via the Trufscan
 * chain indexer. Low-volume, semantically-rich classes (deployStream 1,
 * setTaxonomies 3, requestAttestation 6, setMetadata 7, createMarket 8) are left
 * untouched so their fee/distribution history is preserved.
 *
 * Follows the same write -> index -> trim pattern as trim_order_events (044),
 * driven by the leader-only tn_digest scheduler. Deletion is deterministic
 * (block-height cutoff, never wall-clock) and delete-capped, so every validator
 * removes exactly the same rows when replaying the block.
 *
 * Sequencing: the Trufscan indexer fallback (trufscan #183) must be live before
 * this is activated, so that a pruned tx still resolves on the explorer /tx page.
 */

-- =============================================================================
-- trim_transaction_events: delete old high-volume write-fee ledger rows
-- =============================================================================
/**
 * Called by the tn_digest scheduler (leader-only) to trim old transaction
 * events. Uses a block_height cutoff with a configurable buffer so the indexer
 * has had time to sync before deletion. The CASCADE on
 * transaction_event_distributions removes each trimmed row's fee breakdown.
 *
 * Parameters:
 * - $preserve_blocks: Number of recent blocks to keep (e.g., 172800 = ~2 days)
 * - $delete_cap: Maximum rows to delete per invocation (prevents large txs)
 *
 * Returns via NOTICE: deleted count, remaining count, has_more flag
 */
CREATE OR REPLACE ACTION trim_transaction_events(
    $preserve_blocks INT8,
    $delete_cap INT
) PUBLIC owner {
    -- Only the high-volume write-fee ledger rows are trimmed; other methods
    -- carry irreplaceable fee/distribution history and are kept.
    $write_method_id INT := 2; -- insertRecords (insert_records + truflation)

    $cutoff INT8 := @height - $preserve_blocks;

    -- Don't trim if cutoff is negative (chain is younger than preserve window)
    if $cutoff <= 0 {
        NOTICE('trim_transaction_events: cutoff<=0, nothing to trim');
        RETURN;
    }

    $count INT;
    for $row in SELECT count(*)::INT as cnt FROM transaction_events
        WHERE method_id = $write_method_id AND block_height < $cutoff {
        $count := $row.cnt;
    }

    if $count = 0 {
        NOTICE('trim_transaction_events: deleted=0 remaining=0 has_more=false');
        RETURN;
    }

    $to_delete INT;
    if $count > $delete_cap {
        $to_delete := $delete_cap;
    } else {
        $to_delete := $count;
    }

    DELETE FROM transaction_events
    WHERE tx_id IN (
        SELECT tx_id FROM transaction_events
        WHERE method_id = $write_method_id
          AND block_height < $cutoff
        ORDER BY block_height ASC, tx_id ASC
        LIMIT $to_delete
    );

    $remaining INT := $count - $to_delete;
    $has_more BOOL := $count > $delete_cap;
    NOTICE('trim_transaction_events: deleted=' || $to_delete::TEXT
        || ' remaining=' || $remaining::TEXT
        || ' has_more=' || $has_more::TEXT);
};
