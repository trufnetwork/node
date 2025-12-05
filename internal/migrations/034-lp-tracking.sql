/**
 * MIGRATION 034: LIQUIDITY PROVIDER TRACKING (ISSUE 9A)
 *
 * Purpose:
 * Track eligible liquidity providers for future fee distribution (Issue 9B).
 * LPs qualify by placing split limit orders with tight spreads and sufficient size.
 *
 * LP Eligibility Criteria:
 * 1. Spread requirement: BOTH buy and sell prices must be within max_spread of midpoint (50¢)
 *    Example: max_spread=5 → 52¢/48¢ qualifies (distance=2¢), 56¢/44¢ does not (distance=6¢)
 * 2. Size requirement: Order amount >= min_order_size from ob_queries table
 * 3. Qualification method: Based on place_split_limit_order() volume only
 *
 * Implementation Note:
 * LP eligibility is checked and recorded at order placement time (snapshot approach).
 * Later changes to market parameters (max_spread, min_order_size) do not retroactively
 * affect LP qualification status.
 *
 * Related Issues:
 * - Issue 9A: LP Tracking Infrastructure (THIS MIGRATION)
 * - Issue 9B: Fee Distribution Engine (will use this table)
 * - Migration 032: Modified to track LPs during place_split_limit_order()
 */

-- ============================================================================
-- SCHEMA: LP Tracking Table
-- ============================================================================

/**
 * ob_liquidity_providers
 *
 * Tracks users who qualify as liquidity providers for each market.
 * A user qualifies by placing split limit orders with:
 * - Spread: Both prices within max_spread of midpoint (50 cents)
 * - Size: Amount >= min_order_size
 *
 * Primary Key: (query_id, participant_id)
 * - One record per LP per market
 * - Multiple qualifying orders accumulate split_order_amount
 */
CREATE TABLE IF NOT EXISTS ob_liquidity_providers (
    query_id INT NOT NULL,
    participant_id INT NOT NULL,
    split_order_amount INT8 NOT NULL,   -- Total volume from qualified split orders
    last_order_true_price INT NOT NULL, -- Most recent qualifying order prices (for debugging)
    last_order_false_price INT NOT NULL,
    first_qualified_at INT8 NOT NULL,   -- Unix timestamp of first qualifying order
    last_qualified_at INT8 NOT NULL,    -- Unix timestamp of most recent qualifying order
    total_qualified_orders INT NOT NULL DEFAULT 1, -- Count of qualifying orders
    PRIMARY KEY (query_id, participant_id),
    FOREIGN KEY (query_id) REFERENCES ob_queries(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES ob_participants(id)
);

/**
 * Index for efficient LP queries during fee distribution.
 * Used by distribute_fees() to find all LPs for a settled market.
 */
CREATE INDEX idx_ob_lp_by_query ON ob_liquidity_providers(query_id);

-- ============================================================================
-- ACTIONS: LP Queries
-- ============================================================================

-- =============================================================================
-- get_lp_stats: Query liquidity providers for a market
-- =============================================================================
/**
 * View action to query all liquidity providers for a specific market.
 * Returns LP details including volume, order count, and timestamps.
 *
 * Use cases:
 * - Check who's providing liquidity before settlement
 * - Verify LP eligibility for a market
 * - Audit LP tracking logic
 *
 * Returns:
 * - participant_id: Internal user ID
 * - wallet_address: User's wallet address
 * - split_order_amount: Total volume of qualifying split orders
 * - total_qualified_orders: Number of qualifying orders placed
 * - first_qualified_at: When LP first qualified (unix timestamp)
 * - last_qualified_at: When LP last placed a qualifying order
 * - last_order_true_price: Most recent order's true outcome price
 * - last_order_false_price: Most recent order's false outcome price
 *
 * Parameters:
 * - $query_id: Market ID from ob_queries
 *
 * Example:
 * - Call: get_lp_stats(1)
 * - Returns all LPs for market 1, sorted by volume (highest first)
 */
CREATE OR REPLACE ACTION get_lp_stats($query_id INT) PUBLIC VIEW
RETURNS TABLE(
    participant_id INT,
    wallet_address BYTEA,
    split_order_amount INT8,
    total_qualified_orders INT,
    first_qualified_at INT8,
    last_qualified_at INT8,
    last_order_true_price INT,
    last_order_false_price INT
) {
    for $row in
        SELECT
            lp.participant_id,
            p.wallet_address,
            lp.split_order_amount,
            lp.total_qualified_orders,
            lp.first_qualified_at,
            lp.last_qualified_at,
            lp.last_order_true_price,
            lp.last_order_false_price
        FROM ob_liquidity_providers lp
        INNER JOIN ob_participants p ON lp.participant_id = p.id
        WHERE lp.query_id = $query_id
        ORDER BY lp.split_order_amount DESC
    {
        RETURN NEXT
            $row.participant_id,
            $row.wallet_address,
            $row.split_order_amount,
            $row.total_qualified_orders,
            $row.first_qualified_at,
            $row.last_qualified_at,
            $row.last_order_true_price,
            $row.last_order_false_price;
    }
}
