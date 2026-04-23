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

/**
 * get_market_depth($query_id, $outcome)
 *
 * Aggregate order volume at each price level (for depth chart visualization).
 * Useful for UIs showing market liquidity distribution.
 *
 * Parameters:
 * - $query_id: Market ID
 * - $outcome: TRUE for YES, FALSE for NO
 *
 * Returns TABLE of:
 * - price: Absolute price level (1-99)
 * - buy_volume: Total shares in buy orders at this price
 * - sell_volume: Total shares in sell orders at this price
 *
 * Sorting: By price ascending (best prices first)
 *
 * Usage:
 *   kwil-cli database call --action get_market_depth \
 *     --inputs '[{"$query_id": 1, "$outcome": true}]'
 *
 * Example Output:
 *   price | buy_volume | sell_volume
 *   50    | 5000       | 3000
 *   51    | 2000       | 1500
 *   52    | 1000       | 0
 */
CREATE OR REPLACE ACTION get_market_depth(
    $query_id INT,
    $outcome BOOL
) PUBLIC VIEW RETURNS TABLE(
    price INT,
    buy_volume INT8,
    sell_volume INT8
) {
    if $query_id IS NULL {
        ERROR('query_id is required');
    }
    if $outcome IS NULL {
        ERROR('outcome is required');
    }

    -- Aggregate volume at each price level
    for $depth in
        SELECT
            abs(price) as abs_price,
            COALESCE(SUM(CASE WHEN price < 0 THEN amount ELSE 0::INT8 END)::INT8, 0::INT8) as buy_vol,
            COALESCE(SUM(CASE WHEN price > 0 THEN amount ELSE 0::INT8 END)::INT8, 0::INT8) as sell_vol
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = $outcome
          AND price != 0  -- Exclude holdings
        GROUP BY abs(price)
        ORDER BY abs(price) ASC
    {
        RETURN NEXT $depth.abs_price, $depth.buy_vol, $depth.sell_vol;
    }
};

/**
 * get_best_prices($query_id, $outcome)
 *
 * Get current best bid (highest buy) and best ask (lowest sell) for a market.
 * Shows the current market spread.
 *
 * Parameters:
 * - $query_id: Market ID
 * - $outcome: TRUE for YES, FALSE for NO
 *
 * Returns:
 * - best_bid: Highest buy order price (NULL if no buy orders)
 * - best_ask: Lowest sell order price (NULL if no sell orders)
 * - spread: Difference between ask and bid (NULL if either side missing)
 *
 * Usage:
 *   kwil-cli database call --action get_best_prices \
 *     --inputs '[{"$query_id": 1, "$outcome": true}]'
 *
 * Example Output:
 *   best_bid | best_ask | spread
 *   55       | 58       | 3
 */
CREATE OR REPLACE ACTION get_best_prices(
    $query_id INT,
    $outcome BOOL
) PUBLIC VIEW RETURNS (
    best_bid INT,
    best_ask INT,
    spread INT
) {
    if $query_id IS NULL {
        ERROR('query_id is required');
    }
    if $outcome IS NULL {
        ERROR('outcome is required');
    }

    -- Get best bid (highest buy order)
    -- Buy orders have negative price, so MAX(ABS(price)) gives the highest bid
    $best_bid INT;
    for $row in
        SELECT COALESCE(MAX(abs(price)), NULL)::INT as max_bid
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = $outcome
          AND price < 0
    {
        $best_bid := $row.max_bid;
    }

    -- Get best ask (lowest sell order)
    $best_ask INT;
    for $row in
        SELECT COALESCE(MIN(price), NULL)::INT as min_ask
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = $outcome
          AND price > 0
    {
        $best_ask := $row.min_ask;
    }

    -- Calculate spread
    $spread INT;
    if $best_bid IS NOT NULL AND $best_ask IS NOT NULL {
        $spread := $best_ask - $best_bid;
    }

    RETURN $best_bid, $best_ask, $spread;
};

/**
 * get_user_collateral()
 *
 * Show caller's total collateral locked across all markets.
 * Useful for user dashboards showing "total value locked in prediction markets".
 *
 * Returns:
 * - total_locked: Total collateral (shares value + buy orders locked)
 * - buy_orders_locked: Collateral locked in open buy orders
 * - shares_value: Value of shares held (holdings + open sells, at $1.00 per share)
 *
 * All values in wei (18 decimals).
 *
 * Usage:
 *   kwil-cli database call --action get_user_collateral
 *
 * Example Output:
 *   total_locked               | buy_orders_locked          | shares_value
 *   2500000000000000000000     | 500000000000000000000      | 2000000000000000000000
 *   (2500 tokens)              | (500 tokens)               | (2000 tokens)
 */
CREATE OR REPLACE ACTION get_user_collateral()
PUBLIC VIEW RETURNS (
    total_locked NUMERIC(78, 0),
    buy_orders_locked NUMERIC(78, 0),
    shares_value NUMERIC(78, 0)
) {
    -- Get caller's wallet address
    $caller_bytes BYTEA := decode(substring(LOWER(@caller), 3, 40), 'hex');

    -- Lookup participant ID
    $participant_id INT;
    for $row in SELECT id FROM ob_participants WHERE wallet_address = $caller_bytes {
        $participant_id := $row.id;
    }

    -- Return zeros if participant doesn't exist
    if $participant_id IS NULL {
        RETURN 0::NUMERIC(78, 0), 0::NUMERIC(78, 0), 0::NUMERIC(78, 0);
    }

    -- Calculate locked collateral from buy orders
    -- Buy order collateral = |price| * amount * 0.01 (price in cents)
    -- Convert to wei: multiply by 10^18, divide by 100
    $buy_locked NUMERIC(78, 0);
    for $row in
        SELECT COALESCE(SUM(abs(price)::NUMERIC(78, 0) * amount::NUMERIC(78, 0) * '10000000000000000'::NUMERIC(78, 0))::NUMERIC(78, 0), 0::NUMERIC(78, 0)) as total
        FROM ob_positions
        WHERE participant_id = $participant_id
          AND price < 0
    {
        $buy_locked := $row.total;
    }

    -- Calculate value of shares held (holdings + open sells)
    -- Each share = $1.00 = 10^18 wei
    $shares_value NUMERIC(78, 0);
    for $row in
        SELECT COALESCE(SUM(amount::NUMERIC(78, 0) * '1000000000000000000'::NUMERIC(78, 0))::NUMERIC(78, 0), 0::NUMERIC(78, 0)) as total
        FROM ob_positions
        WHERE participant_id = $participant_id
          AND price >= 0
    {
        $shares_value := $row.total;
    }

    -- Total locked collateral
    $total_locked NUMERIC(78, 0) := $buy_locked + $shares_value;

    RETURN $total_locked, $buy_locked, $shares_value;
};
