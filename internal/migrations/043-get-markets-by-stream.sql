-- Migration: Order Book Discovery Action
-- Adds get_markets_by_stream discovery view for high-performance indexed lookups.

-- =============================================================================
-- get_markets_by_stream: Discovery view for asset pages
-- =============================================================================
/**
 * Returns all markets associated with a specific stream ID.
 * This lookup is high-performance (indexed).
 *
 * Parameters:
 * - $stream_id: The 32-byte stream identifier
 * - $limit_val: Maximum number of results (default 100, max 100)
 * - $offset_val: Number of results to skip (default 0)
 *
 * Returns table of market summaries.
 */
CREATE OR REPLACE ACTION get_markets_by_stream(
    $stream_id BYTEA,
    $limit_val INT,
    $offset_val INT
)
PUBLIC VIEW RETURNS TABLE (
    id INT,
    hash BYTEA,
    data_provider BYTEA,
    action_id TEXT,
    settle_time INT8,
    settled BOOLEAN,
    winning_outcome BOOLEAN,
    max_spread INT,
    min_order_size INT8,
    created_at INT8
) {
    if $stream_id IS NULL {
        ERROR('stream_id is required');
    }

    -- Apply default and max limits 
    -- Note: This logic is intentionally duplicated from list_markets (032-order-book-actions.sql)
    -- to maintain consistency in pagination behavior.
    $effective_limit INT := 100;
    $effective_offset INT := 0;

    if $limit_val IS NOT NULL AND $limit_val > 0 AND $limit_val <= 100 {
        $effective_limit := $limit_val;
    }
    if $offset_val IS NOT NULL AND $offset_val >= 0 {
        $effective_offset := $offset_val;
    }

    RETURN SELECT id, hash, data_provider, action_id, settle_time, settled, 
                  winning_outcome, max_spread, min_order_size, created_at
           FROM ob_queries
           WHERE stream_id = $stream_id
           ORDER BY created_at DESC
           LIMIT $effective_limit OFFSET $effective_offset;
};
