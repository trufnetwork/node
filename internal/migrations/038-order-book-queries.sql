/**
 * MIGRATION 038: ORDER BOOK QUERY ACTIONS
 *
 * Read-only PUBLIC VIEW actions for querying order book data:
 * - get_order_book($query_id, $outcome) - View market orders
 * - get_user_positions() - View caller's portfolio
 * - get_market_depth($query_id, $outcome) - Aggregated volume (optional)
 * - get_best_prices($query_id, $outcome) - Market spread (optional)
 * - get_user_collateral() - User's locked value (optional)
 *
 * Dependencies: Migration 030 (schema), 031 (vault), 032 (actions)
 */

-- ============================================================================
-- CORE QUERY ACTIONS
-- ============================================================================

/**
 * get_order_book($query_id, $outcome)
 *
 * View all buy and sell orders for a specific market and outcome.
 * Excludes holdings (price=0) - only shows active orders.
 *
 * Parameters:
 * - $query_id: Market ID
 * - $outcome: TRUE for YES, FALSE for NO
 *
 * Returns TABLE of:
 * - participant_id: Internal participant ID
 * - price: Order price (-99 to -1 for buys, 1 to 99 for sells)
 * - amount: Number of shares
 * - last_updated: FIFO timestamp for order priority
 * - wallet_address: Hex-encoded wallet address (0x-prefixed)
 *
 * Sorting: Best prices first (closest to 0), then FIFO within price level
 *
 * Usage:
 *   kwil-cli database call --action get_order_book \
 *     --inputs '[{"$query_id": 1, "$outcome": true}]'
 *
 * Example Output:
 *   participant_id | price | amount | last_updated | wallet_address
 *   42            | -55   | 1000   | 1701234567   | 0xabcd...1234
 *   17            | 55    | 500    | 1701234600   | 0xef12...5678
 *   42            | -56   | 2000   | 1701234580   | 0xabcd...1234
 */
CREATE OR REPLACE ACTION get_order_book(
    $query_id INT,
    $outcome BOOL
) PUBLIC VIEW RETURNS TABLE(
    participant_id INT,
    price INT,
    amount INT8,
    last_updated INT8,
    wallet_address TEXT
) {
    -- Validate inputs
    if $query_id IS NULL {
        ERROR('query_id is required');
    }
    if $outcome IS NULL {
        ERROR('outcome is required (TRUE for YES, FALSE for NO)');
    }

    -- Return buy/sell orders, sorted by price and FIFO
    -- Exclude holdings (price = 0)
    for $order in
        SELECT
            p.participant_id,
            p.price,
            p.amount,
            p.last_updated,
            '0x' || encode(part.wallet_address, 'hex') as wallet_hex
        FROM ob_positions p
        JOIN ob_participants part ON p.participant_id = part.id
        WHERE p.query_id = $query_id
          AND p.outcome = $outcome
          AND p.price != 0  -- Exclude holdings (only show orders)
        ORDER BY
            abs(p.price) ASC,      -- Best prices first (1, -1, 2, -2, ...)
            p.last_updated ASC     -- FIFO within price level
    {
        RETURN NEXT $order.participant_id, $order.price, $order.amount, $order.last_updated, $order.wallet_hex;
    }
};

/**
 * get_user_positions()
 *
 * View caller's portfolio across all markets (holdings + open orders).
 * Uses @caller to identify the user automatically.
 *
 * Returns empty result if user hasn't participated in any markets yet.
 *
 * Returns TABLE of:
 * - query_id: Market ID
 * - outcome: TRUE for YES, FALSE for NO
 * - price: Position type indicator (0=holding, <0=buy, >0=sell)
 * - amount: Number of shares
 * - position_type: Human-readable label ('holding', 'buy_order', 'sell_order')
 *
 * Usage:
 *   kwil-cli database call --action get_user_positions
 *
 * Example Output:
 *   query_id | outcome | price | amount | position_type
 *   1        | TRUE    | 0     | 500    | holding
 *   1        | FALSE   | 60    | 300    | sell_order
 *   2        | TRUE    | -55   | 1000   | buy_order
 *   2        | FALSE   | 0     | 200    | holding
 */
CREATE OR REPLACE ACTION get_user_positions()
PUBLIC VIEW RETURNS TABLE(
    query_id INT,
    outcome BOOL,
    price INT,
    amount INT8,
    position_type TEXT
) {
    -- Get caller's wallet address (convert @caller TEXT to 20-byte BYTEA)
    -- @caller format: "0xABCD..." (42 chars: 0x + 40 hex chars)
    $caller_bytes BYTEA := decode(substring(LOWER(@caller), 3, 40), 'hex');

    -- Lookup participant ID
    $participant_id INT;
    for $row in SELECT id FROM ob_participants WHERE wallet_address = $caller_bytes {
        $participant_id := $row.id;
    }

    -- Return empty if participant doesn't exist (user hasn't traded yet)
    if $participant_id IS NULL {
        RETURN;
    }

    -- Return all positions for this user
    for $pos in
        SELECT
            query_id,
            outcome,
            price,
            amount,
            CASE
                WHEN price = 0 THEN 'holding'
                WHEN price < 0 THEN 'buy_order'
                ELSE 'sell_order'
            END as position_type
        FROM ob_positions
        WHERE participant_id = $participant_id
        ORDER BY query_id, outcome, price DESC
    {
        RETURN NEXT $pos.query_id, $pos.outcome, $pos.price, $pos.amount, $pos.position_type;
    }
};