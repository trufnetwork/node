/*
 * ORDER BOOK ACTIONS
 *
 * User-facing actions for the prediction market:
 * - create_market: Create a new prediction market
 * - get_market_info: Get market details by ID
 * - list_markets: List markets with optional filtering
 *
 * Bridge Support:
 * - All collateral-locking actions accept a $bridge parameter
 * - Supported bridges: hoodi_tt2, sepolia_bridge, ethereum_bridge
 */

-- =============================================================================
-- validate_bridge: Helper to validate bridge parameter
-- =============================================================================
/**
 * Validates that the bridge parameter is one of the supported bridges.
 * Throws an error if invalid.
 *
 * Parameters:
 * - $bridge: The bridge namespace to validate
 */
CREATE OR REPLACE ACTION validate_bridge($bridge TEXT) PRIVATE {
    if $bridge IS NULL {
        ERROR('bridge parameter is required');
    }

    if $bridge != 'hoodi_tt2' AND
       $bridge != 'sepolia_bridge' AND
       $bridge != 'ethereum_bridge' {
        ERROR('Invalid bridge. Supported: hoodi_tt2, sepolia_bridge, ethereum_bridge');
    }

    RETURN;
};

-- =============================================================================
-- get_bridge_units_per_dollar: Bridge token base units per $1.00 of collateral
-- =============================================================================
/**
 * Returns the number of bridge token base units that represent $1.00 of
 * collateral. This is 10^decimals where decimals is the bridge token's
 * on-chain decimals.
 *
 * Embedded test bridges (hoodi_tt2, sepolia_bridge, ethereum_bridge) are all
 * 18-decimal TRUF-likes and share the same value. The generated mainnet
 * override (.prod.sql) replaces the body with eth_truf=10^18 and
 * eth_usdc=10^6, driven by BRIDGE_DECIMALS in
 * scripts/generate_prod_migrations.py — the only place real per-bridge
 * decimals live.
 *
 * IMPORTANT: $price in OB actions is INT in [1, 99] — an integer-encoded
 * probability/price-point, NOT a value with units. To compute collateral
 * in base units:
 *
 *     collateral_base_units = ($amount * $price * units_per_dollar) / 100
 *
 * Parameters:
 * - $bridge: Bridge namespace
 *
 * Returns:
 * - units_per_dollar: Base units representing $1.00
 */
CREATE OR REPLACE ACTION get_bridge_units_per_dollar($bridge TEXT)
    PUBLIC VIEW RETURNS (units_per_dollar NUMERIC(78, 0)) {
    if $bridge = 'hoodi_tt2' {
        RETURN '1000000000000000000'::NUMERIC(78, 0);
    } else if $bridge = 'sepolia_bridge' {
        RETURN '1000000000000000000'::NUMERIC(78, 0);
    } else if $bridge = 'ethereum_bridge' {
        RETURN '1000000000000000000'::NUMERIC(78, 0);
    }
    ERROR('Unknown bridge: ' || $bridge);
};

-- =============================================================================
-- get_market_bridge: Get the bridge for a market
-- =============================================================================
/**
 * Retrieves the bridge namespace for a given market.
 * Throws an error if market doesn't exist.
 *
 * Parameters:
 * - $query_id: The market ID
 *
 * Returns:
 * - The bridge namespace (hoodi_tt2, sepolia_bridge, or ethereum_bridge)
 */
CREATE OR REPLACE ACTION get_market_bridge($query_id INT) PRIVATE RETURNS (bridge TEXT) {
    $found_bridge TEXT;

    for $row in SELECT bridge FROM ob_queries WHERE id = $query_id {
        $found_bridge := $row.bridge;
    }

    if $found_bridge IS NULL {
        ERROR('Market does not exist (query_id: ' || $query_id::TEXT || ')');
    }

    RETURN $found_bridge;
};

-- =============================================================================
-- create_market: Create a new prediction market
-- =============================================================================
/**
 * Creates a market with specified settlement parameters. The market is
 * identified by a unique hash derived from the query components.
 *
 * Parameters:
 * - $bridge: Bridge namespace for collateral (hoodi_tt2, sepolia_bridge, ethereum_bridge)
 * - $query_components: ABI-encoded (address data_provider, bytes32 stream_id, string action_id, bytes args)
 * - $settle_time: Unix timestamp when market can be settled (must be in future)
 * - $max_spread: Maximum spread for LP rewards (1-50 price-points)
 * - $min_order_size: Minimum order size for LP rewards (must be positive)
 *
 * Returns:
 * - query_id: The integer ID of the created market
 *
 * Fees:
 * - 2 TRUF market creation fee (TODO: adjust based on spam prevention needs)
 */
CREATE OR REPLACE ACTION create_market(
    $bridge TEXT,
    $query_components BYTEA,
    $settle_time INT8,
    $max_spread INT,
    $min_order_size INT8
) PUBLIC RETURNS (query_id INT) {
    -- ==========================================================================
    -- VALIDATION
    -- ==========================================================================

    -- Validate bridge parameter
    validate_bridge($bridge);

    -- Validate query components (must be ABI-encoded)
    if $query_components IS NULL OR length(encode($query_components, 'hex')) = 0 {
        ERROR('query_components is required (ABI-encoded (address,bytes32,string,bytes))');
    }

    -- Compute hash from query components using attestation format
    -- This ensures market hash matches attestation hash for automatic settlement
    $query_hash BYTEA;
    for $row in tn_utils.compute_attestation_hash($query_components) {
        $query_hash := $row.hash;
    }

    -- Validate hash is exactly 32 bytes
    if length(encode($query_hash, 'hex')) != 64 {  -- 32 bytes = 64 hex chars
        ERROR('Invalid query_components: computed hash must be 32 bytes');
    }

    -- Check for duplicate market (hash must be unique)
    $existing_id INT;
    for $row in SELECT id FROM ob_queries WHERE hash = $query_hash {
        $existing_id := $row.id;
    }
    if $existing_id IS NOT NULL {
        ERROR('Market already exists with this query hash (query_id: ' || $existing_id::TEXT || ')');
    }

    -- Validate settlement time (must be in the future)
    -- Use @block_timestamp (unix epoch seconds of current block)
    if $settle_time IS NULL OR $settle_time <= @block_timestamp {
        ERROR('Settlement time must be in the future');
    }

    -- Validate max_spread (1-50 price-points)
    if $max_spread IS NULL OR $max_spread < 1 OR $max_spread > 50 {
        ERROR('Max spread must be between 1 and 50 (price-points)');
    }

    -- Validate min_order_size (must be positive)
    if $min_order_size IS NULL OR $min_order_size < 1 {
        ERROR('Minimum order size must be positive');
    }

    -- ==========================================================================
    -- FEE COLLECTION
    -- ==========================================================================
    -- Fee: 2 TRUF (2 * 10^18 base units of eth_truf)
    -- Market creation fee is ALWAYS paid in TRUF (hoodi_tt on testnet)
    -- regardless of which bridge the market uses for collateral
    $market_creation_fee NUMERIC(78, 0) := '2000000000000000000'::NUMERIC(78, 0);

    -- Check caller has sufficient TRUF balance
    -- IMPORTANT: Fee is collected from hoodi_tt (TRUF), not from market's bridge
    $caller_balance NUMERIC(78, 0) := COALESCE(hoodi_tt.balance(@caller), 0::NUMERIC(78, 0));

    if $caller_balance < $market_creation_fee {
        ERROR('Insufficient TRUF balance for market creation fee. Required: 2 TRUF (hoodi_tt balance)');
    }

    -- Verify leader address is available for fee transfer
    if @leader_sender IS NULL {
        ERROR('Leader address not available for fee transfer');
    }

    -- Safe leader address conversion (handles both TEXT and BYTEA leader_sender)
    $leader_hex TEXT := tn_utils.get_leader_hex();
    if $leader_hex = '' {
        ERROR('Leader address not available for fee transfer');
    }

    -- Transfer fee to leader from TRUF bridge (hoodi_tt)
    hoodi_tt.transfer($leader_hex, $market_creation_fee);

    -- ==========================================================================
    -- CREATE MARKET
    -- ==========================================================================

    -- Safe caller normalization (handles both TEXT and BYTEA @caller)
    $caller_bytes BYTEA := tn_utils.get_caller_bytes();

    -- Insert market record with MAX(id) + 1 pattern
    -- Note: This is safe in Kwil because transactions within a block are processed
    -- sequentially by the consensus engine, not concurrently.
    INSERT INTO ob_queries (
        id,
        hash,
        query_components,
        settle_time,
        max_spread,
        min_order_size,
        created_at,
        creator,
        bridge
    )
    SELECT
        COALESCE(MAX(id), 0) + 1,
        $query_hash,
        $query_components,
        $settle_time,
        $max_spread,
        $min_order_size,
        @height,
        $caller_bytes,
        $bridge
    FROM ob_queries;

    -- Get the ID we just inserted
    $query_id INT;
    for $row in SELECT id FROM ob_queries WHERE hash = $query_hash {
        $query_id := $row.id;
    }

    -- ==========================================================================
    -- RECORD TRANSACTION EVENT
    -- ==========================================================================
    record_transaction_event(
        8,
        $market_creation_fee,
        $leader_hex,
        NULL
    );

    RETURN $query_id;
};

-- =============================================================================
-- get_market_info: Get market details by ID
-- =============================================================================
/**
 * Returns all details for a specific market.
 *
 * Parameters:
 * - $query_id: The market ID to look up
 *
 * Returns market details or raises an error if not found.
 */
CREATE OR REPLACE ACTION get_market_info($query_id INT)
PUBLIC VIEW RETURNS (
    hash BYTEA,
    query_components BYTEA,
    bridge TEXT,
    settle_time INT8,
    settled BOOLEAN,
    winning_outcome BOOLEAN,
    settled_at INT8,
    max_spread INT,
    min_order_size INT8,
    created_at INT8,
    creator BYTEA
) {
    if $query_id IS NULL {
        ERROR('query_id is required');
    }

    for $market in
        SELECT hash, query_components, bridge, settle_time, settled, winning_outcome, settled_at,
               max_spread, min_order_size, created_at, creator
        FROM ob_queries
        WHERE id = $query_id
    {
        RETURN $market.hash, $market.query_components, $market.bridge, $market.settle_time, $market.settled,
               $market.winning_outcome, $market.settled_at, $market.max_spread,
               $market.min_order_size, $market.created_at, $market.creator;
    }

    ERROR('Market not found: ' || $query_id::TEXT);
};

-- =============================================================================
-- get_market_by_hash: Get market details by query hash
-- =============================================================================
/**
 * Returns market details by its unique query hash.
 * Useful for checking if a market already exists before creation.
 *
 * Parameters:
 * - $query_hash: The SHA256 hash of the market's attestation query
 *
 * Returns market details or raises an error if not found.
 */
CREATE OR REPLACE ACTION get_market_by_hash($query_hash BYTEA)
PUBLIC VIEW RETURNS (
    id INT,
    settle_time INT8,
    settled BOOLEAN,
    winning_outcome BOOLEAN,
    settled_at INT8,
    max_spread INT,
    min_order_size INT8,
    created_at INT8,
    creator BYTEA
) {
    if $query_hash IS NULL {
        ERROR('query_hash is required');
    }

    for $market in
        SELECT id, settle_time, settled, winning_outcome, settled_at,
               max_spread, min_order_size, created_at, creator
        FROM ob_queries
        WHERE hash = $query_hash
    {
        RETURN $market.id, $market.settle_time, $market.settled,
               $market.winning_outcome, $market.settled_at, $market.max_spread,
               $market.min_order_size, $market.created_at, $market.creator;
    }

    ERROR('Market not found for given hash');
};

-- =============================================================================
-- list_markets: List markets with optional filtering
-- =============================================================================
/**
 * Returns a paginated list of markets with optional filtering.
 *
 * Parameters:
 * - $settled_filter: Filter by settled status (NULL = all, TRUE = settled only, FALSE = active only)
 * - $limit: Maximum number of results (default 100, max 100)
 * - $offset: Number of results to skip (default 0)
 *
 * Returns table of market summaries ordered by created_at descending.
 */
CREATE OR REPLACE ACTION list_markets(
    $settled_filter BOOLEAN,
    $limit_val INT,
    $offset_val INT
) PUBLIC VIEW RETURNS TABLE(
    id INT,
    hash BYTEA,
    settle_time INT8,
    settled BOOLEAN,
    winning_outcome BOOLEAN,
    max_spread INT,
    min_order_size INT8,
    created_at INT8
) {
    -- Apply default and max limits
    $effective_limit INT := 100;
    $effective_offset INT := 0;

    if $limit_val IS NOT NULL AND $limit_val > 0 AND $limit_val <= 100 {
        $effective_limit := $limit_val;
    }
    if $offset_val IS NOT NULL AND $offset_val >= 0 {
        $effective_offset := $offset_val;
    }

    -- Query with optional filter
    if $settled_filter IS NULL {
        -- Return all markets
        RETURN SELECT id, hash, settle_time, settled, winning_outcome,
                      max_spread, min_order_size, created_at
               FROM ob_queries
               ORDER BY created_at DESC
               LIMIT $effective_limit OFFSET $effective_offset;
    } else {
        -- Filter by settled status
        RETURN SELECT id, hash, settle_time, settled, winning_outcome,
                      max_spread, min_order_size, created_at
               FROM ob_queries
               WHERE settled = $settled_filter
               ORDER BY created_at DESC
               LIMIT $effective_limit OFFSET $effective_offset;
    }
};

-- =============================================================================
-- market_exists: Check if a market exists by hash (lightweight check)
-- =============================================================================
/**
 * Quick check if a market with the given hash already exists.
 * Returns TRUE if exists, FALSE otherwise.
 *
 * Parameters:
 * - $query_hash: The SHA256 hash to check
 */
CREATE OR REPLACE ACTION market_exists($query_hash BYTEA)
PUBLIC VIEW RETURNS (market_exists BOOLEAN) {
    if $query_hash IS NULL {
        RETURN false;
    }

    for $row in SELECT 1 FROM ob_queries WHERE hash = $query_hash LIMIT 1 {
        RETURN true;
    }

    RETURN false;
};

-- =============================================================================
-- Section 3: Trading Actions
-- =============================================================================

-- =============================================================================
-- MATCHING ENGINE
-- =============================================================================
/**
 * The matching engine automatically executes trades by matching compatible orders.
 * Three match types are supported:
 *
 * 1. DIRECT MATCH - Buy order meets sell order at same price
 *    Example: Buy 100 YES @ $0.56 matches Sell 80 YES @ $0.56
 *    Result: 80 shares transfer from seller to buyer
 *
 * 2. MINT MATCH - Opposite buy orders at complementary prices create share pairs
 *    Example: Buy 100 YES @ $0.56 + Buy 80 NO @ $0.44 → Mint 80 pairs
 *    Result: System creates 80 YES shares + 80 NO shares (backed by locked collateral)
 *
 * 3. BURN MATCH - Opposite sell orders at complementary prices destroy shares
 *    Example: Sell 100 YES @ $0.60 + Sell 80 NO @ $0.40 → Burn 80 pairs
 *    Result: System destroys shares, returns $1.00 collateral per pair to sellers
 *
 * All matches support:
 * - FIFO ordering (earlier orders matched first via last_updated timestamp)
 * - Partial fills (order remains open if not fully matched)
 * - Atomic execution (all updates succeed or all fail)
 */

-- =============================================================================
-- match_direct: Direct match implementation with price-crossing
-- =============================================================================
/**
 * Matches overlapping buy and sell orders for the same outcome.
 * Supports price-crossing: a buy@52 can match a sell@51 (standard order book behavior).
 *
 * Price-Crossing Semantics:
 * - A match occurs when the best buy price >= the best sell price
 * - The match executes at the SELL price (buyer gets price improvement)
 * - If buy_price > sell_price, the buyer is refunded the difference
 * - Example: Buy YES @ $0.52 matches Sell YES @ $0.51
 *   → Seller receives $0.51/share, buyer is refunded $0.01/share
 *
 * Collateral Flow (all amounts in bridge token base units):
 * - Seller receives: (match_amount × sell_price × units_per_dollar) / 100
 * - Buyer refund (if price improvement): (match_amount × (buy_price - sell_price) × units_per_dollar) / 100
 * - Shares transfer from seller's holdings to buyer's holdings
 *
 * Recursion Behavior:
 * - Uses tail recursion to process multiple matches sequentially
 * - Each iteration matches one order pair and removes/reduces them from ob_positions
 * - Natural termination when no more overlapping orders exist
 * - Maximum depth is bounded by the number of orders in the order book
 *
 * Parameters:
 * - $query_id: Market identifier
 * - $outcome: TRUE (YES) or FALSE (NO)
 * - $price: Unused (kept for call-site compatibility). Sweep finds best overlap.
 * - $bridge: Bridge identifier for collateral operations
 */
CREATE OR REPLACE ACTION match_direct(
    $query_id INT,
    $outcome BOOL,
    $price INT,
    $bridge TEXT
) PRIVATE {
    -- Find the cheapest sell order (best ask)
    $sell_price INT;
    $sell_participant_id INT;
    $sell_amount INT8;
    $sell_found BOOL := false;

    for $sell_order in
        SELECT price, participant_id, amount
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = $outcome
          AND price > 0          -- Sell orders have positive price
        ORDER BY price ASC, last_updated ASC  -- Cheapest first, then FIFO
        LIMIT 1
    {
        $sell_price := $sell_order.price;
        $sell_participant_id := $sell_order.participant_id;
        $sell_amount := $sell_order.amount;
        $sell_found := true;
    }

    -- No sell order, exit
    if NOT $sell_found {
        RETURN;
    }

    -- Find the most expensive buy order (best bid)
    $buy_price_neg INT;
    $buy_participant_id INT;
    $buy_amount INT8;
    $buy_found BOOL := false;

    for $buy_order in
        SELECT price, participant_id, amount
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = $outcome
          AND price < 0          -- Buy orders have negative price
        ORDER BY price ASC, last_updated ASC  -- Most negative first = highest buy price, then FIFO
        LIMIT 1
    {
        $buy_price_neg := $buy_order.price;
        $buy_participant_id := $buy_order.participant_id;
        $buy_amount := $buy_order.amount;
        $buy_found := true;
    }

    -- No buy order, exit
    if NOT $buy_found {
        RETURN;
    }

    -- Convert negative stored price to positive buy price
    $buy_price INT := 0 - $buy_price_neg;

    -- Check price crossing: buy price must be >= sell price for a match
    if $buy_price < $sell_price {
        RETURN;
    }

    -- Defensive: Skip if either order has zero or negative amount (shouldn't happen)
    if $buy_amount <= 0 OR $sell_amount <= 0 {
        RETURN;
    }

    -- Calculate match amount (minimum of buy and sell)
    $match_amount INT8;
    if $buy_amount < $sell_amount {
        $match_amount := $buy_amount;
    } else {
        $match_amount := $sell_amount;
    }

    -- Get seller's wallet address for payment
    $seller_wallet_bytes BYTEA;
    for $row in SELECT wallet_address FROM ob_participants WHERE id = $sell_participant_id {
        $seller_wallet_bytes := $row.wallet_address;
    }
    $seller_wallet_address TEXT := '0x' || encode($seller_wallet_bytes, 'hex');

    -- Calculate payment to seller (at sell price), in bridge token base units.
    -- $price is INT in [1, 99]; divide by 100 to convert to dollars.
    $units_per_dollar NUMERIC(78, 0) := get_bridge_units_per_dollar($bridge);
    $seller_payment NUMERIC(78, 0) := ($match_amount::NUMERIC(78, 0) *
                                        $sell_price::NUMERIC(78, 0) *
                                        $units_per_dollar) / 100::NUMERIC(78, 0);

    -- Transfer payment from vault to seller
    ob_unlock_collateral($bridge, $seller_wallet_address, $seller_payment);

    -- Record fill events (execution price = sell_price for direct matches)
    ob_record_order_event($query_id, $buy_participant_id, 'direct_buy_fill', $outcome, $sell_price, $match_amount, $sell_participant_id);
    ob_record_order_event($query_id, $sell_participant_id, 'direct_sell_fill', $outcome, $sell_price, $match_amount, $buy_participant_id);

    -- Record sell impact for P&L
    ob_record_tx_impact($sell_participant_id, $outcome, -$match_amount, $seller_payment, FALSE);

    -- Handle buyer price improvement refund
    $price_diff INT := $buy_price - $sell_price;

    -- Get buyer's wallet address (needed for potential refund)
    $buyer_wallet_bytes BYTEA;
    for $row in SELECT wallet_address FROM ob_participants WHERE id = $buy_participant_id {
        $buyer_wallet_bytes := $row.wallet_address;
    }
    $buyer_wallet_address TEXT := '0x' || encode($buyer_wallet_bytes, 'hex');

    if $price_diff > 0 {
        -- Buyer locked collateral at buy_price but match executes at sell_price.
        -- Refund the difference: (match_amount × (buy_price - sell_price) × units_per_dollar) / 100.
        $buyer_refund NUMERIC(78, 0) := ($match_amount::NUMERIC(78, 0) *
                                          $price_diff::NUMERIC(78, 0) *
                                          $units_per_dollar) / 100::NUMERIC(78, 0);
        ob_unlock_collateral($bridge, $buyer_wallet_address, $buyer_refund);

        -- Record buy impact with refund
        ob_record_tx_impact($buy_participant_id, $outcome, $match_amount, $buyer_refund, FALSE);
    } else {
        -- No price improvement (exact price match)
        ob_record_tx_impact($buy_participant_id, $outcome, $match_amount, 0::NUMERIC(78,0), FALSE);
    }

    -- Transfer shares from seller to buyer
    -- Step 1: Delete fully matched orders FIRST (prevents amount=0 constraint violation)
    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND ((participant_id = $sell_participant_id AND outcome = $outcome
            AND price = $sell_price AND amount = $match_amount)
        OR (participant_id = $buy_participant_id AND outcome = $outcome
            AND price = $buy_price_neg AND amount = $match_amount));

    -- Step 2: Reduce seller's sell order (only if partial fill)
    UPDATE ob_positions
    SET amount = amount - $match_amount
    WHERE query_id = $query_id
      AND participant_id = $sell_participant_id
      AND outcome = $outcome
      AND price = $sell_price
      AND amount > $match_amount;

    -- Step 3: Add shares to buyer's holdings (price = 0)
    INSERT INTO ob_positions
    (query_id, participant_id, outcome, price, amount, last_updated)
    VALUES ($query_id, $buy_participant_id, $outcome, 0, $match_amount, @block_timestamp)
    ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
    SET amount = ob_positions.amount + EXCLUDED.amount;

    -- Step 4: Reduce buyer's buy order (only if partial fill)
    UPDATE ob_positions
    SET amount = amount - $match_amount
    WHERE query_id = $query_id
      AND participant_id = $buy_participant_id
      AND outcome = $outcome
      AND price = $buy_price_neg
      AND amount > $match_amount;

    -- Recursively call to match next overlapping orders
    match_direct($query_id, $outcome, $price, $bridge);
};

-- =============================================================================
-- match_mint: Mint match implementation
-- =============================================================================
/**
 * Creates new share pairs when opposite buy orders at complementary prices exist.
 *
 * Example: Buy 100 YES @ $0.56 + Buy 80 NO @ $0.44 → Mint 80 pairs
 *
 * Collateral Flow:
 * - Both buyers already have collateral locked (sum = $1.00 per pair)
 * - No new collateral needed
 * - System creates new YES and NO shares backed by existing locked collateral
 *
 * Recursion Behavior:
 * - Uses tail recursion to process multiple mint matches sequentially
 * - Each iteration mints one share pair and removes/reduces buy orders from ob_positions
 * - Recursion depth = number of share pairs minted at these complementary price levels
 * - Natural termination when no more complementary buy orders exist (LIMIT 1 returns nothing)
 * - Maximum depth is bounded by the number of buy orders in the order book
 * - In practice, depth rarely exceeds 10-20 due to:
 *   * Economic constraints (gas costs for creating many small orders)
 *   * Market maker behavior (traders prefer larger consolidated orders)
 *   * Natural order book dynamics
 * - Worst case: One large YES buy matching 100+ tiny NO buys requires 100+ separate
 *   transactions to create those orders, making it economically impractical
 *
 * Parameters:
 * - $query_id: Market identifier
 * - $yes_price: YES buy order price (e.g., 56)
 * - $no_price: NO buy order price (must sum to 100, e.g., 44)
 */
CREATE OR REPLACE ACTION match_mint(
    $query_id INT,
    $yes_price INT,
    $no_price INT,
    $bridge TEXT
) PRIVATE {
    -- Validate complementary prices (must sum to 100)
    if ($yes_price + $no_price) != 100 {
        RETURN;
    }

    -- Get first YES buy order (FIFO)
    $yes_participant_id INT;
    $yes_amount INT8;
    $yes_found BOOL := false;

    for $yes_buy in
        SELECT participant_id, amount
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = true
          AND price = -$yes_price
        ORDER BY last_updated ASC
        LIMIT 1
    {
        $yes_participant_id := $yes_buy.participant_id;
        $yes_amount := $yes_buy.amount;
        $yes_found := true;
    }

    if NOT $yes_found {
        RETURN;
    }

    -- Get first NO buy order (FIFO)
    $no_participant_id INT;
    $no_amount INT8;
    $no_found BOOL := false;

    for $no_buy in
        SELECT participant_id, amount
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = false
          AND price = -$no_price
        ORDER BY last_updated ASC
        LIMIT 1
    {
        $no_participant_id := $no_buy.participant_id;
        $no_amount := $no_buy.amount;
        $no_found := true;
    }

    if NOT $no_found {
        RETURN;
    }

    -- Prevent self-matching: Don't match orders from the same participant
    -- This allows LPs to place complementary buy orders (e.g., YES @ 48 + NO @ 52)
    -- that stay open in the order book for liquidity provision and rewards eligibility
    if $yes_participant_id = $no_participant_id {
        RETURN;
    }

    -- Defensive: Skip if either order has zero or negative amount (shouldn't happen)
    if $yes_amount <= 0 OR $no_amount <= 0 {
        RETURN;
    }

    -- Calculate mint amount
    $mint_amount INT8;
    if $yes_amount < $no_amount {
        $mint_amount := $yes_amount;
    } else {
        $mint_amount := $no_amount;
    }

    -- Record mint fill events
    ob_record_order_event($query_id, $yes_participant_id, 'mint_fill', TRUE, $yes_price, $mint_amount, $no_participant_id);
    ob_record_order_event($query_id, $no_participant_id, 'mint_fill', FALSE, $no_price, $mint_amount, $yes_participant_id);

    -- Delete fully matched buy orders FIRST
    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND ((participant_id = $yes_participant_id AND outcome = true
            AND price = -$yes_price AND amount = $mint_amount)
        OR (participant_id = $no_participant_id AND outcome = false
            AND price = -$no_price AND amount = $mint_amount));

    -- Mint shares
    INSERT INTO ob_positions
    (query_id, participant_id, outcome, price, amount, last_updated)
    VALUES ($query_id, $yes_participant_id, true, 0, $mint_amount, @block_timestamp)
    ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
    SET amount = ob_positions.amount + EXCLUDED.amount;

    INSERT INTO ob_positions
    (query_id, participant_id, outcome, price, amount, last_updated)
    VALUES ($query_id, $no_participant_id, false, 0, $mint_amount, @block_timestamp)
    ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
    SET amount = ob_positions.amount + EXCLUDED.amount;

    -- Record impacts for P&L
    ob_record_tx_impact($yes_participant_id, true, $mint_amount, 0::NUMERIC(78,0), FALSE);
    ob_record_tx_impact($no_participant_id, false, $mint_amount, 0::NUMERIC(78,0), FALSE);

    -- Reduce buy orders (only if partial fill)
    UPDATE ob_positions
    SET amount = amount - $mint_amount
    WHERE query_id = $query_id
      AND participant_id = $yes_participant_id
      AND outcome = true
      AND price = -$yes_price
      AND amount > $mint_amount;

    UPDATE ob_positions
    SET amount = amount - $mint_amount
    WHERE query_id = $query_id
      AND participant_id = $no_participant_id
      AND outcome = false
      AND price = -$no_price
      AND amount > $mint_amount;

    -- Recursively match next orders
    match_mint($query_id, $yes_price, $no_price, $bridge);
};

-- =============================================================================
-- match_burn: Burn match implementation
-- =============================================================================
/**
 * Destroys share pairs when opposite sell orders at complementary prices exist,
 * returning collateral to sellers.
 *
 * Example: Sell 100 YES @ $0.60 + Sell 80 NO @ $0.40 → Burn 80 pairs
 *
 * Collateral Flow:
 * - Total collateral backing shares: $1.00 per pair
 * - YES seller receives: burn_amount × yes_price
 * - NO seller receives: burn_amount × no_price
 * - Total returned: burn_amount × $1.00
 *
 * Recursion Behavior:
 * - Uses tail recursion to process multiple burn matches sequentially
 * - Each iteration burns one share pair and removes/reduces sell orders from ob_positions
 * - Recursion depth = number of share pairs burned at these complementary price levels
 * - Natural termination when no more complementary sell orders exist (LIMIT 1 returns nothing)
 * - Maximum depth is bounded by the number of sell orders in the order book
 * - In practice, depth rarely exceeds 10-20 due to:
 *   * Economic constraints (gas costs for creating many small orders)
 *   * Market maker behavior (traders prefer larger consolidated orders)
 *   * Natural order book dynamics
 * - Worst case: One large YES sell matching 100+ tiny NO sells requires 100+ separate
 *   transactions to create those orders, making it economically impractical
 *
 * Parameters:
 * - $query_id: Market identifier
 * - $yes_price: YES sell order price (e.g., 60)
 * - $no_price: NO sell order price (must sum to 100, e.g., 40)
 */
CREATE OR REPLACE ACTION match_burn(
    $query_id INT,
    $yes_price INT,
    $no_price INT,
    $bridge TEXT
) PRIVATE {
    -- Bridge token base units per $1.00 of collateral (single source of truth).
    $units_per_dollar NUMERIC(78, 0) := get_bridge_units_per_dollar($bridge);

    -- Validate complementary prices (must sum to 100)
    if ($yes_price + $no_price) != 100 {
        RETURN;
    }

    -- Get first YES sell order (FIFO)
    $yes_participant_id INT;
    $yes_amount INT8;
    $yes_found BOOL := false;

    for $yes_sell in
        SELECT participant_id, amount
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = true
          AND price = $yes_price
        ORDER BY last_updated ASC
        LIMIT 1
    {
        $yes_participant_id := $yes_sell.participant_id;
        $yes_amount := $yes_sell.amount;
        $yes_found := true;
    }

    if NOT $yes_found {
        RETURN;
    }

    -- Get YES seller's wallet address
    $yes_wallet_bytes BYTEA;
    for $row in SELECT wallet_address FROM ob_participants WHERE id = $yes_participant_id {
        $yes_wallet_bytes := $row.wallet_address;
    }
    $yes_wallet_address TEXT := '0x' || encode($yes_wallet_bytes, 'hex');

    -- Get first NO sell order (FIFO)
    $no_participant_id INT;
    $no_amount INT8;
    $no_found BOOL := false;

    for $no_sell in
        SELECT participant_id, amount
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = false
          AND price = $no_price
        ORDER BY last_updated ASC
        LIMIT 1
    {
        $no_participant_id := $no_sell.participant_id;
        $no_amount := $no_sell.amount;
        $no_found := true;
    }

    if NOT $no_found {
        RETURN;
    }

    -- Prevent self-matching: Don't match orders from the same participant
    -- This allows LPs to place complementary sell orders that stay open
    if $yes_participant_id = $no_participant_id {
        RETURN;
    }

    -- Defensive: Skip if either order has zero or negative amount (shouldn't happen)
    if $yes_amount <= 0 OR $no_amount <= 0 {
        RETURN;
    }

    -- Get NO seller's wallet address
    $no_wallet_bytes BYTEA;
    for $row in SELECT wallet_address FROM ob_participants WHERE id = $no_participant_id {
        $no_wallet_bytes := $row.wallet_address;
    }
    $no_wallet_address TEXT := '0x' || encode($no_wallet_bytes, 'hex');

    -- Calculate burn amount
    $burn_amount INT8;
    if $yes_amount < $no_amount {
        $burn_amount := $yes_amount;
    } else {
        $burn_amount := $no_amount;
    }

    -- Record burn fill events
    ob_record_order_event($query_id, $yes_participant_id, 'burn_fill', TRUE, $yes_price, $burn_amount, $no_participant_id);
    ob_record_order_event($query_id, $no_participant_id, 'burn_fill', FALSE, $no_price, $burn_amount, $yes_participant_id);

    -- Calculate payouts in bridge token base units.
    -- $price is INT in [1, 99]; divide by 100 to convert to dollars.
    $yes_payout NUMERIC(78, 0) := ($burn_amount::NUMERIC(78, 0) *
                                    $yes_price::NUMERIC(78, 0) *
                                    $units_per_dollar) / 100::NUMERIC(78, 0);

    $no_payout NUMERIC(78, 0) := ($burn_amount::NUMERIC(78, 0) *
                                   $no_price::NUMERIC(78, 0) *
                                   $units_per_dollar) / 100::NUMERIC(78, 0);

    -- Unlock collateral
    ob_unlock_collateral($bridge, $yes_wallet_address, $yes_payout);
    ob_unlock_collateral($bridge, $no_wallet_address, $no_payout);

    -- Record impacts for P&L
    ob_record_tx_impact($yes_participant_id, TRUE, -$burn_amount, $yes_payout, FALSE);
    ob_record_tx_impact($no_participant_id, FALSE, -$burn_amount, $no_payout, FALSE);

    -- Delete fully matched sell orders FIRST
    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND ((participant_id = $yes_participant_id AND outcome = true
            AND price = $yes_price AND amount = $burn_amount)
        OR (participant_id = $no_participant_id AND outcome = false
            AND price = $no_price AND amount = $burn_amount));

    -- Reduce sell orders (only if partial fill)
    UPDATE ob_positions
    SET amount = amount - $burn_amount
    WHERE query_id = $query_id
      AND participant_id = $yes_participant_id
      AND outcome = true
      AND price = $yes_price
      AND amount > $burn_amount;

    UPDATE ob_positions
    SET amount = amount - $burn_amount
    WHERE query_id = $query_id
      AND participant_id = $no_participant_id
      AND outcome = false
      AND price = $no_price
      AND amount > $burn_amount;

    -- Recursively match next orders
    match_burn($query_id, $yes_price, $no_price, $bridge);
};

-- =============================================================================
-- match_orders: Main matching orchestrator
-- =============================================================================
/**
 * Coordinates all three matching types for a given order.
 *
 * Called automatically after every order placement to attempt matching.
 * Tries all three match types in sequence:
 * 1. Direct match with price-crossing (buy_price >= sell_price)
 * 2. Mint match (creates liquidity, exact complementary prices)
 * 3. Burn match (removes liquidity, exact complementary prices)
 *
 * Parameters:
 * - $query_id: Market identifier
 * - $outcome: TRUE (YES) or FALSE (NO)
 * - $price: Price of the triggering order (used for mint/burn complementary calc)
 */
CREATE OR REPLACE ACTION match_orders(
    $query_id INT,
    $outcome BOOL,
    $price INT,
    $bridge TEXT
) PRIVATE {
    -- ==========================================================================
    -- SECTION 1: VALIDATE INPUTS
    -- ==========================================================================

    if $query_id IS NULL {
        ERROR('query_id is required for matching');
    }

    if $outcome IS NULL {
        ERROR('outcome is required for matching');
    }

    if $price IS NULL OR $price < 1 OR $price > 99 {
        ERROR('price must be between 1 and 99 for matching');
    }

    -- ==========================================================================
    -- SECTION 2: TRY DIRECT MATCH (with price-crossing)
    -- ==========================================================================
    -- Sweeps to find the best overlapping buy/sell pair (buy_price >= sell_price)
    -- Match executes at sell price; buyer is refunded price difference

    match_direct($query_id, $outcome, $price, $bridge);

    -- ==========================================================================
    -- SECTION 3: TRY MINT MATCH
    -- ==========================================================================
    -- Create share pairs when opposite buy orders at complementary prices exist
    -- Example: Buy YES @ 56 + Buy NO @ 44 = Mint pair

    -- Calculate complementary price (must sum to 100)
    $complementary_price INT := 100 - $price;

    -- Only attempt mint match if complementary price is valid (1-99)
    if $complementary_price >= 1 AND $complementary_price <= 99 {
        -- Match based on which outcome we're matching for
        if $outcome = true {
            -- Matching YES order: look for NO buy at complementary price
            match_mint($query_id, $price, $complementary_price, $bridge);
        } else {
            -- Matching NO order: look for YES buy at complementary price
            match_mint($query_id, $complementary_price, $price, $bridge);
        }
    }

    -- ==========================================================================
    -- SECTION 4: TRY BURN MATCH
    -- ==========================================================================
    -- Destroy share pairs when opposite sell orders at complementary prices exist
    -- Example: Sell YES @ 60 + Sell NO @ 40 = Burn pair

    if $complementary_price >= 1 AND $complementary_price <= 99 {
        -- Match based on which outcome we're matching for
        if $outcome = true {
            -- Matching YES order: look for NO sell at complementary price
            match_burn($query_id, $price, $complementary_price, $bridge);
        } else {
            -- Matching NO order: look for YES sell at complementary price
            match_burn($query_id, $complementary_price, $price, $bridge);
        }
    }

    -- Success: All possible matches attempted
    -- Note: Some, all, or none of the matches may have executed
};

-- =============================================================================
-- place_buy_order: Place a buy order for YES or NO shares
-- =============================================================================
/**
 * Places a buy order for YES or NO shares in a prediction market.
 *
 * Users lock collateral (amount × price) to place a buy order. The order is added
 * to the order book with a NEGATIVE price to distinguish it from sell orders.
 * The matching engine is triggered to check for immediate matches.
 *
 * The bridge is determined by the market's configuration (set during create_market).
 *
 * Parameters:
 * - $query_id: Market identifier (from ob_queries.id)
 * - $outcome: TRUE for YES shares, FALSE for NO shares
 * - $price: INT in [1, 99] — implied probability × 100 ($0.01 to $0.99)
 * - $amount: Number of shares to buy
 *
 * Collateral locked (in bridge token base units):
 *   ($amount * $price * units_per_dollar) / 100
 * Example (eth_truf, units_per_dollar = 10^18):
 *   10 shares at price 56 → (10 × 56 × 10^18) / 100 = 5.6 × 10^18 base units (5.6 TRUF)
 *
 * Price Convention:
 * - Buy orders stored with NEGATIVE price (e.g., -56 for a buy at probability 56)
 * - Sell orders stored with POSITIVE price (e.g., 56 for a sell at probability 56)
 * - Holdings stored with price = 0 (not listed for sale)
 *
 * Examples:
 *   place_buy_order(1, TRUE, 56, 10)   -- Buy 10 YES at $0.56 (locks 5.6 TRUF)
 *   place_buy_order(1, FALSE, 44, 20)  -- Buy 20 NO at $0.44 (locks 8.8 TRUF)
 */
CREATE OR REPLACE ACTION place_buy_order(
    $query_id INT,
    $outcome BOOL,
    $price INT,
    $amount INT8
) PUBLIC {
    -- ==========================================================================
    -- SECTION 1: VALIDATION
    -- ==========================================================================

    -- 1.1 Get market bridge (will ERROR if market doesn't exist)
    $bridge TEXT := get_market_bridge($query_id);

    -- 1.2 Validate @caller format and normalize to bytes
    -- Safe caller normalization (handles both TEXT and BYTEA @caller)
    $caller_bytes BYTEA := tn_utils.get_caller_bytes();

    -- 1.3 Validate parameters
    if $query_id IS NULL {
        ERROR('query_id is required');
    }

    if $outcome IS NULL {
        ERROR('outcome is required (TRUE for YES, FALSE for NO)');
    }

    if $price IS NULL OR $price < 1 OR $price > 99 {
        ERROR('price must be between 1 and 99 ($0.01 to $0.99)');
    }

    if $amount IS NULL OR $amount <= 0 {
        ERROR('amount must be positive');
    }

    if $amount > 1000000000 {
        ERROR('amount exceeds maximum allowed of 1,000,000,000');
    }

    -- 1.4 Validate market exists and is not settled
    $settled BOOL;
    $settle_time INT8;
    $market_found BOOL := false;

    for $row in SELECT settled, settle_time FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
        $settle_time := $row.settle_time;
        $market_found := true;
    }

    if NOT $market_found {
        ERROR('Market does not exist (query_id: ' || $query_id::TEXT || ')');
    }

    -- Note: Markets remain tradable until settlement time is reached or explicitly settled (settled=true).
    -- The settle_time is metadata indicating when settlement CAN occur, and now serves as a hard cutoff for trading.
    -- Users cannot continue trading past settle_time.
    -- This two-phase design allows flexibility in settlement timing while ensuring a fixed trading window.
    if $settled {
        ERROR('Market has already settled (no trading allowed)');
    }

    -- Trading Cutoff: Prevent new orders after settlement time
    if @block_timestamp >= $settle_time {
        ERROR('Trading is closed. Market has passed its settlement time.');
    }

    -- ==========================================================================
    -- SECTION 2: CALCULATE COLLATERAL NEEDED
    -- ==========================================================================

    -- All collateral in bridge token base units. units_per_dollar is sourced
    -- from get_bridge_units_per_dollar (the single point where per-bridge
    -- decimals are defined). $price is INT in [1, 99], so we divide by 100.
    --
    --   collateral = ($amount * $price * units_per_dollar) / 100
    $units_per_dollar NUMERIC(78, 0) := get_bridge_units_per_dollar($bridge);
    $collateral_needed NUMERIC(78, 0) := ($amount::NUMERIC(78, 0) *
                                           $price::NUMERIC(78, 0) *
                                           $units_per_dollar) / 100::NUMERIC(78, 0);

    -- ==========================================================================
    -- SECTION 3: CHECK BALANCE (bridge-specific)
    -- ==========================================================================

    $caller_balance NUMERIC(78, 0);
    if $bridge = 'hoodi_tt2' {
        $caller_balance := COALESCE(hoodi_tt2.balance(@caller), 0::NUMERIC(78, 0));
    } else if $bridge = 'sepolia_bridge' {
        $caller_balance := COALESCE(sepolia_bridge.balance(@caller), 0::NUMERIC(78, 0));
    } else if $bridge = 'ethereum_bridge' {
        $caller_balance := COALESCE(ethereum_bridge.balance(@caller), 0::NUMERIC(78, 0));
    }

    if $caller_balance < $collateral_needed {
        -- Display in token base units AND in dollar form (collateral / units_per_dollar).
        ERROR('Insufficient balance. Required: ' || $collateral_needed::TEXT || ' base units ($' ||
              ($collateral_needed / $units_per_dollar)::TEXT || ')');
    }

    -- ==========================================================================
    -- SECTION 4: GET OR CREATE PARTICIPANT
    -- ==========================================================================

    -- Safe caller normalization (already done in Section 1.2)
    -- $caller_bytes is already available

    -- Try to get existing participant
    $participant_id INT;
    for $row in SELECT id FROM ob_participants WHERE wallet_address = $caller_bytes {
        $participant_id := $row.id;
    }

    -- Create if not found (MAX(id) + 1 pattern)
    -- Note: This is safe in Kwil because transactions within a block are processed
    -- sequentially by the consensus engine, not concurrently.
    if $participant_id IS NULL {
        INSERT INTO ob_participants (id, wallet_address)
        SELECT COALESCE(MAX(id), 0) + 1, $caller_bytes
        FROM ob_participants;

        -- Retrieve the newly created ID
        for $row in SELECT id FROM ob_participants WHERE wallet_address = $caller_bytes {
            $participant_id := $row.id;
        }
    }



    -- ==========================================================================
    -- SECTION 5: LOCK COLLATERAL (bridge-specific)
    -- ==========================================================================

    -- Lock tokens from user to vault (network-owned balance)
    -- Note: Bridge lock() throws ERROR on failure (insufficient balance, etc.)
    if $bridge = 'hoodi_tt2' {
        hoodi_tt2.lock($collateral_needed);
    } else if $bridge = 'sepolia_bridge' {
        sepolia_bridge.lock($collateral_needed);
    } else if $bridge = 'ethereum_bridge' {
        ethereum_bridge.lock($collateral_needed);
    }

    -- Record initial impact (collateral spent)
    ob_record_tx_impact($participant_id, $outcome, 0::INT8, $collateral_needed, TRUE);

    -- ==========================================================================
    -- SECTION 6: INSERT BUY ORDER (UPSERT)
    -- ==========================================================================

    -- Buy orders use NEGATIVE price to distinguish from sell orders
    -- Example: Buy at $0.56 stored as price = -56
    --
    -- If multiple orders at same (query_id, participant_id, outcome, price):
    -- - Accumulate amounts
    -- - Update timestamp to latest (FIFO within price level)
    INSERT INTO ob_positions
    (query_id, participant_id, outcome, price, amount, last_updated)
    VALUES ($query_id, $participant_id, $outcome, -$price, $amount, @block_timestamp)
    ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
    SET amount = ob_positions.amount + EXCLUDED.amount,
        last_updated = EXCLUDED.last_updated;

    -- Record order event
    ob_record_order_event($query_id, $participant_id, 'buy_placed', $outcome, $price, $amount, NULL);

    -- ==========================================================================
    -- SECTION 7: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Attempt to match this buy order with existing sell orders
    match_orders($query_id, $outcome, $price, $bridge);

    -- ==========================================================================
    -- SECTION 8: CLEANUP & MATERIALIZE IMPACTS
    -- ==========================================================================

    ob_cleanup_tx_payouts($query_id);

    -- Success: Order placed (may be partially or fully matched by future matching engine)
};

-- =============================================================================
-- place_sell_order: Place a sell order for YES or NO shares
-- =============================================================================
/**
 * Places a sell order for YES or NO shares that the user already owns.
 *
 * Users must own shares (held in positions table with price = 0) before selling.
 * Shares are moved from holdings to the sell order book with positive price.
 * No new collateral is needed since shares already exist.
 *
 * Parameters:
 * - $query_id: Market identifier (from ob_queries.id)
 * - $outcome: TRUE for YES shares, FALSE for NO shares
 * - $price: INT in [1, 99] — implied probability × 100 ($0.01 to $0.99)
 * - $amount: Number of shares to sell
 *
 * Prerequisites: User must own at least $amount shares of $outcome
 *
 * Price Convention:
 * - Holdings stored with price = 0 (shares owned, not listed for sale)
 * - Sell orders stored with POSITIVE price (e.g., 56 for $0.56 sell)
 * - Buy orders stored with NEGATIVE price (e.g., -56 for $0.56 buy)
 *
 * Examples:
 *   place_sell_order(1, TRUE, 56, 10)   -- Sell 10 YES at $0.56
 *   place_sell_order(1, FALSE, 44, 20)  -- Sell 20 NO at $0.44
 */
CREATE OR REPLACE ACTION place_sell_order(
    $query_id INT,
    $outcome BOOL,
    $price INT,
    $amount INT8
) PUBLIC {
    -- ==========================================================================
    -- SECTION 1: VALIDATION
    -- ==========================================================================

    -- 1.0 Get market bridge (will ERROR if market doesn't exist)
    $bridge TEXT := get_market_bridge($query_id);

    -- Safe caller normalization using precompiles
    $caller_bytes BYTEA := tn_utils.get_caller_bytes();

    -- 1.2 Validate parameters
    if $query_id IS NULL {
        ERROR('query_id is required');
    }

    if $outcome IS NULL {
        ERROR('outcome is required (TRUE for YES, FALSE for NO)');
    }

    if $price IS NULL OR $price < 1 OR $price > 99 {
        ERROR('price must be between 1 and 99 ($0.01 to $0.99)');
    }

    if $amount IS NULL OR $amount <= 0 {
        ERROR('amount must be positive');
    }

    if $amount > 1000000000 {
        ERROR('amount exceeds maximum allowed of 1,000,000,000');
    }

    -- 1.3 Validate market exists and is not settled
    $settled BOOL;
    $settle_time INT8;
    $market_found BOOL := false;

    for $row in SELECT settled, settle_time FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
        $settle_time := $row.settle_time;
        $market_found := true;
    }

    if NOT $market_found {
        ERROR('Market does not exist (query_id: ' || $query_id::TEXT || ')');
    }

    -- Note: Markets remain tradable until settlement time is reached or explicitly settled (settled=true).
    -- The settle_time is metadata indicating when settlement CAN occur, and now serves as a hard cutoff for trading.
    -- Users cannot continue trading past settle_time.
    -- This two-phase design allows flexibility in settlement timing while ensuring a fixed trading window.
    if $settled {
        ERROR('Market has already settled (no trading allowed)');
    }

    -- Trading Cutoff: Prevent new orders after settlement time
    if @block_timestamp >= $settle_time {
        ERROR('Trading is closed. Market has passed its settlement time.');
    }

    -- ==========================================================================
    -- SECTION 2: GET PARTICIPANT (NO AUTO-CREATE FOR SELLS)
    -- ==========================================================================

    -- Safe caller normalization (already done in Section 1.2)
    -- $caller_bytes is already available

    -- Look up participant (DON'T auto-create for sells)
    -- If user has shares, they must already be a participant from previous buy/mint
    $participant_id INT;
    for $row in SELECT id FROM ob_participants WHERE wallet_address = $caller_bytes {
        $participant_id := $row.id;
    }

    if $participant_id IS NULL {
        ERROR('No shares found. You must own shares before selling.');
    }

    -- ==========================================================================
    -- SECTION 3: VERIFY SHARE OWNERSHIP
    -- ==========================================================================

    -- Check holding position (price = 0 means holding, not listed for sale)
    $held_amount INT8;
    for $row in SELECT amount FROM ob_positions
        WHERE query_id = $query_id
          AND participant_id = $participant_id
          AND outcome = $outcome
          AND price = 0  -- Holdings have price = 0
    {
        $held_amount := $row.amount;
    }

    -- Validate sufficient shares
    if $held_amount IS NULL {
        ERROR('No shares found. You must own shares of the specified outcome before selling.');
    }

    if $held_amount < $amount {
        ERROR('Insufficient shares. You own: ' || $held_amount::TEXT ||
              ' shares, trying to sell: ' || $amount::TEXT);
    }



    -- ==========================================================================
    -- SECTION 4: MOVE SHARES FROM HOLDING TO SELL ORDER
    -- ==========================================================================

    -- Step 1: Delete holdings if selling all shares (prevents amount=0 constraint violation)
    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND participant_id = $participant_id
      AND outcome = $outcome
      AND price = 0
      AND amount = $amount;

    -- Step 2: Reduce holdings if selling partial shares
    UPDATE ob_positions
    SET amount = amount - $amount
    WHERE query_id = $query_id
      AND participant_id = $participant_id
      AND outcome = $outcome
      AND price = 0
      AND amount > $amount;

    -- Step 3: Insert sell order (UPSERT)
    -- Sell orders use POSITIVE price to distinguish from buy orders
    -- Example: Sell at $0.56 stored as price = 56
    --
    -- If multiple orders at same (query_id, participant_id, outcome, price):
    -- - Accumulate amounts
    -- - Update timestamp to latest (FIFO within price level)
    INSERT INTO ob_positions
    (query_id, participant_id, outcome, price, amount, last_updated)
    VALUES ($query_id, $participant_id, $outcome, $price, $amount, @block_timestamp)
    ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
    SET amount = ob_positions.amount + EXCLUDED.amount,
        last_updated = EXCLUDED.last_updated;

    -- Record order event
    ob_record_order_event($query_id, $participant_id, 'sell_placed', $outcome, $price, $amount, NULL);

    -- ==========================================================================
    -- SECTION 5: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Attempt to match this sell order with existing buy orders
    match_orders($query_id, $outcome, $price, $bridge);

    -- ==========================================================================
    -- SECTION 6: CLEANUP & MATERIALIZE IMPACTS
    -- ==========================================================================

    ob_cleanup_tx_payouts($query_id);

    -- Success: Order placed (may be partially or fully matched by future matching engine)
};

-- =============================================================================
-- place_split_limit_order: Mint binary token pairs and list unwanted side for sale
-- =============================================================================
/**
 * Places a split limit order - the primary liquidity provision mechanism.
 * Mints equal amounts of YES and NO shares, holds the desired outcome,
 * and automatically lists the unwanted outcome for sale.
 *
 * This feature enables users to become liquidity providers and earn a share
 * of redemption fees by providing tight two-sided markets.
 *
 * The bridge is determined by the market's configuration (set during create_market).
 *
 * Parameters:
 * - $query_id: Market identifier (from ob_queries.id)
 * - $true_price: INT in [1, 99] — implied probability for YES × 100 ($0.01 to $0.99)
 * - $amount: Number of share PAIRS to mint
 *
 * Collateral locked (in bridge token base units):
 *   $amount * units_per_dollar    -- one full $1.00 per minted pair
 * Example (eth_truf, units_per_dollar = 10^18):
 *   100 share pairs → 100 × 10^18 base units = 100 TRUF
 *
 * Result:
 * - User holds: $amount YES shares at price=0 (holding, not for sale)
 * - User sells: $amount NO shares at price=(100-true_price) (listed for sale)
 *
 * Price Calculation:
 * - YES price: $true_price (e.g., 56 ⇒ $0.56)
 * - NO price: 100 - $true_price (e.g., 100 - 56 = 44 ⇒ $0.44)
 * - Total: Always $1.00 per share pair
 *
 * LP Reward Eligibility:
 *
 * To qualify for LP rewards, ALL of the following must be true:
 * 1. BOTH buy and sell prices must be within max_spread of market midpoint (50):
 *    - Effective BUY price (true_price) within spread
 *    - SELL price (false_price = 100 - true_price) within spread
 * 2. Amount must meet min_order_size threshold from ob_queries
 *
 * Rewards are calculated based on SELL order volume (NO shares listed for sale),
 * but qualification requires BOTH prices to be within the spread. This encourages
 * tight two-sided markets and provides meaningful liquidity for traders.
 *
 * Example with max_spread = 5:
 * - Market midpoint: 50 (price-points)
 * - Split @ 56/44: Distance = 6 → OUTSIDE spread → NOT qualified
 * - Split @ 52/48: Distance = 2 → WITHIN spread → QUALIFIED
 *
 * Examples:
 *   place_split_limit_order(1, 56, 100)  -- Mint 100 pairs: hold YES, sell NO @ $0.44
 *   place_split_limit_order(1, 35, 50)   -- Mint 50 pairs: hold YES, sell NO @ $0.65
 */
CREATE OR REPLACE ACTION place_split_limit_order(
    $query_id INT,
    $true_price INT,
    $amount INT8
) PUBLIC {
    -- ==========================================================================
    -- SECTION 1: VALIDATION
    -- ==========================================================================

    -- 1.1 Get market bridge (will ERROR if market doesn't exist)
    $bridge TEXT := get_market_bridge($query_id);

    -- 1.2 Validate @caller format and normalize to bytes
    -- Safe caller normalization (handles both TEXT and BYTEA @caller)
    $caller_bytes BYTEA := tn_utils.get_caller_bytes();

    -- 1.3 Validate parameters
    if $query_id IS NULL {
        ERROR('query_id is required');
    }

    if $true_price IS NULL OR $true_price < 1 OR $true_price > 99 {
        ERROR('true_price must be between 1 and 99 ($0.01 to $0.99)');
    }

    if $amount IS NULL OR $amount <= 0 {
        ERROR('amount must be positive');
    }

    if $amount > 1000000000 {
        ERROR('amount exceeds maximum allowed of 1,000,000,000');
    }

    -- 1.4 Validate market exists and is not settled
    $settled BOOL;
    $settle_time INT8;
    $market_found BOOL := false;

    for $row in SELECT settled, settle_time FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
        $settle_time := $row.settle_time;
        $market_found := true;
    }

    if NOT $market_found {
        ERROR('Market does not exist (query_id: ' || $query_id::TEXT || ')');
    }

    -- Note: Markets remain tradable until settlement time is reached or explicitly settled (settled=true).
    -- The settle_time is metadata indicating when settlement CAN occur, and now serves as a hard cutoff for trading.
    -- Users cannot continue trading past settle_time.
    -- This two-phase design allows flexibility in settlement timing while ensuring a fixed trading window.
    if $settled {
        ERROR('Market has already settled (no trading allowed)');
    }

    -- Trading Cutoff: Prevent new orders after settlement time
    if @block_timestamp >= $settle_time {
        ERROR('Trading is closed. Market has passed its settlement time.');
    }

    -- ==========================================================================
    -- SECTION 2: CALCULATE COLLATERAL NEEDED
    -- ==========================================================================

    -- Each minted share pair requires $1.00 of collateral, expressed in
    -- bridge token base units. units_per_dollar is sourced from
    -- get_bridge_units_per_dollar (the single point where per-bridge
    -- decimals are defined).
    --
    --   collateral = $amount * units_per_dollar
    $units_per_dollar NUMERIC(78, 0) := get_bridge_units_per_dollar($bridge);
    $collateral_needed NUMERIC(78, 0) := $amount::NUMERIC(78, 0) * $units_per_dollar;

    -- ==========================================================================
    -- SECTION 3: CHECK BALANCE (bridge-specific)
    -- ==========================================================================

    $caller_balance NUMERIC(78, 0);
    if $bridge = 'hoodi_tt2' {
        $caller_balance := COALESCE(hoodi_tt2.balance(@caller), 0::NUMERIC(78, 0));
    } else if $bridge = 'sepolia_bridge' {
        $caller_balance := COALESCE(sepolia_bridge.balance(@caller), 0::NUMERIC(78, 0));
    } else if $bridge = 'ethereum_bridge' {
        $caller_balance := COALESCE(ethereum_bridge.balance(@caller), 0::NUMERIC(78, 0));
    }

    if $caller_balance < $collateral_needed {
        -- Display in token base units AND in dollar form (collateral / units_per_dollar).
        ERROR('Insufficient balance. Required: ' || $collateral_needed::TEXT || ' base units ($' ||
              ($collateral_needed / $units_per_dollar)::TEXT || ')');
    }

    -- ==========================================================================
    -- SECTION 4: GET OR CREATE PARTICIPANT
    -- ==========================================================================

    -- Safe caller normalization (already done in Section 1.2)
    -- $caller_bytes is already available

    -- Try to get existing participant
    $participant_id INT;
    for $row in SELECT id FROM ob_participants WHERE wallet_address = $caller_bytes {
        $participant_id := $row.id;
    }

    -- Create if not found (MAX(id) + 1 pattern)
    -- Note: This is safe in Kwil because transactions within a block are processed
    -- sequentially by the consensus engine, not concurrently.
    if $participant_id IS NULL {
        INSERT INTO ob_participants (id, wallet_address)
        SELECT COALESCE(MAX(id), 0) + 1, $caller_bytes
        FROM ob_participants;

        -- Retrieve the newly created ID
        for $row in SELECT id FROM ob_participants WHERE wallet_address = $caller_bytes {
            $participant_id := $row.id;
        }
    }



    -- ==========================================================================
    -- SECTION 5: LOCK COLLATERAL (bridge-specific)
    -- ==========================================================================

    -- Lock tokens from user to vault (network-owned balance)
    -- Note: Bridge lock() throws ERROR on failure (insufficient balance, etc.)
    if $bridge = 'hoodi_tt2' {
        hoodi_tt2.lock($collateral_needed);
    } else if $bridge = 'sepolia_bridge' {
        sepolia_bridge.lock($collateral_needed);
    } else if $bridge = 'ethereum_bridge' {
        ethereum_bridge.lock($collateral_needed);
    }

    -- Record initial impacts:
    -- Calculate split collateral (50/50 split for YES/NO legs)
    $collateral_per_leg NUMERIC(78, 0) := $collateral_needed / 2::NUMERIC(78, 0);
    -- Handle dust: add remainder to YES leg if odd amount
    $collateral_yes NUMERIC(78, 0) := $collateral_per_leg + ($collateral_needed - (2::NUMERIC(78, 0) * $collateral_per_leg));
    
    -- 1. Collateral lock (split between outcomes)
    ob_record_tx_impact($participant_id, TRUE, 0::INT8, $collateral_yes, TRUE);
    ob_record_tx_impact($participant_id, FALSE, 0::INT8, $collateral_per_leg, TRUE);
    
    -- 2. Mint YES shares
    ob_record_tx_impact($participant_id, TRUE, $amount, 0::NUMERIC(78,0), FALSE);
    -- 3. Mint NO shares
    ob_record_tx_impact($participant_id, FALSE, $amount, 0::NUMERIC(78,0), FALSE);

    -- ==========================================================================
    -- SECTION 7: CREATE POSITIONS
    -- ==========================================================================


    -- Mint YES shares and hold them (not for sale)
    -- These are stored with price = 0 to indicate holding (not listed)
    --
    -- If user already has YES holdings, amounts accumulate (UPSERT)
    INSERT INTO ob_positions
    (query_id, participant_id, outcome, price, amount, last_updated)
    VALUES ($query_id, $participant_id, TRUE, 0, $amount, @block_timestamp)
    ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
    SET amount = ob_positions.amount + EXCLUDED.amount,
        last_updated = EXCLUDED.last_updated;

    -- ==========================================================================
    -- SECTION 7: MINT NO SHARES (SELL ORDER)
    -- ==========================================================================

    -- Calculate complementary price for NO shares
    -- If user wants YES @ $0.56, they sell NO @ $0.44 (100 - 56 = 44)
    $false_price INT := 100 - $true_price;

    -- Create NO sell order directly (v2 optimization)
    -- Note: We skip the intermediate holding step - go straight to sell order
    -- This is more efficient than: mint at price=0, then call place_sell_order()
    --
    -- If user already has a NO sell order at this price, amounts accumulate (UPSERT)
    INSERT INTO ob_positions
    (query_id, participant_id, outcome, price, amount, last_updated)
    VALUES ($query_id, $participant_id, FALSE, $false_price, $amount, @block_timestamp)
    ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
    SET amount = ob_positions.amount + EXCLUDED.amount,
        last_updated = EXCLUDED.last_updated;

    -- Record order events: YES holding + NO sell
    ob_record_order_event($query_id, $participant_id, 'split_placed', TRUE, $true_price, $amount, NULL);
    ob_record_order_event($query_id, $participant_id, 'split_placed', FALSE, $false_price, $amount, NULL);

    -- ==========================================================================
    -- SECTION 8: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Attempt to match the NO sell order with existing buy orders
    -- Match is attempted on the FALSE (NO) outcome at the false_price
    match_orders($query_id, FALSE, $false_price, $bridge);

    -- ==========================================================================
    -- SECTION 9: CLEANUP & MATERIALIZE IMPACTS
    -- ==========================================================================

    ob_cleanup_tx_payouts($query_id);

    -- Success: Split order placed
    -- - YES shares held at price=0 (not for sale)
    -- - NO shares listed for sale at price=false_price
    -- - May be partially or fully matched by future matching engine
};

-- =============================================================================
-- cancel_order: Cancel an open buy or sell order
-- =============================================================================
/**
 * Cancels an open buy or sell order and refunds collateral or returns shares.
 *
 * Users can cancel their open orders at any time before settlement with no fee.
 * This action allows traders to exit positions or adjust their strategy without penalty.
 *
 * Order Types Supported:
 * - Buy orders (price < 0): Locked collateral is refunded to user's wallet
 * - Sell orders (price > 0): Shares are returned to holding wallet (price = 0)
 * - Holdings (price = 0): Cannot be cancelled via this action (use place_sell_order to list)
 *
 * Example 1: Cancel buy order for YES @ $0.56
 * - User placed buy order: 100 shares × $0.56 = 56 TRUF locked
 * - User calls cancel_order(query_id=1, outcome=TRUE, price=-56)
 * - System refunds 56 TRUF to user's wallet
 * - Order is deleted from ob_positions
 *
 * Example 2: Cancel sell order for NO @ $0.44
 * - User placed sell order: 100 NO shares listed at $0.44
 * - User calls cancel_order(query_id=1, outcome=FALSE, price=44)
 * - System moves 100 NO shares back to holdings (price=0)
 * - Sell order is deleted from ob_positions
 *
 * Collateral Refund Calculation (Buy Orders), in bridge token base units:
 * - Formula: ($order_amount * |price| * units_per_dollar) / 100
 * - Example (eth_truf, units_per_dollar = 10^18):
 *   10 shares @ price 56 → (10 × 56 × 10^18) / 100 = 5.6 × 10^18 base units (5.6 TRUF)
 *
 * Parameters:
 * - $query_id: Market ID from ob_queries
 * - $outcome: TRUE (YES shares) or FALSE (NO shares)
 * - $price: Price of order to cancel (-99 to 99, excluding 0)
 *
 * Returns: Nothing (void action)
 *
 * Errors:
 * - If market doesn't exist
 * - If market is already settled
 * - If order doesn't exist or doesn't belong to caller
 * - If price is 0 (holdings cannot be cancelled)
 * - If price is out of valid range (-99 to 99)
 * - If caller address is invalid format
 */
CREATE OR REPLACE ACTION cancel_order(
    $query_id INT,
    $outcome BOOL,
    $price INT
) PUBLIC {
    -- ==========================================================================
    -- SECTION 0: GET MARKET BRIDGE
    -- ==========================================================================

    -- Get market bridge (will ERROR if market doesn't exist)
    $bridge TEXT := get_market_bridge($query_id);

    -- ==========================================================================
    -- SECTION 1: VALIDATE CALLER
    -- ==========================================================================

    -- Safe caller normalization using precompiles
    $caller_bytes BYTEA := tn_utils.get_caller_bytes();

    -- ==========================================================================
    -- SECTION 2: VALIDATE PARAMETERS
    -- ==========================================================================

    -- Validate query_id
    if $query_id IS NULL OR $query_id < 1 {
        ERROR('Invalid query_id');
    }

    -- Validate outcome
    if $outcome IS NULL {
        ERROR('Outcome must be specified (TRUE for YES, FALSE for NO)');
    }

    -- Validate price range
    -- Buy orders: -99 to -1 (negative)
    -- Sell orders: 1 to 99 (positive)
    -- Holdings: 0 (NOT allowed - holdings cannot be cancelled, use place_sell_order to list)
    if $price IS NULL OR $price < -99 OR $price > 99 OR $price = 0 {
        ERROR('Price must be between -99 and 99 (excluding 0). Holdings (price=0) cannot be cancelled.');
    }

    -- ==========================================================================
    -- SECTION 3: VALIDATE MARKET
    -- ==========================================================================

    -- Check market exists and is not settled
    $settled BOOL;
    for $row in SELECT settled FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
    }

    if $settled IS NULL {
        ERROR('Market does not exist');
    }

    if $settled {
        ERROR('Cannot cancel orders on settled market');
    }

    -- ==========================================================================
    -- SECTION 4: GET PARTICIPANT
    -- ==========================================================================

    -- Get participant ID from caller's wallet address
    -- Note: Don't auto-create participant - they must exist if they have orders
    -- This uses the helper function from 031-order-book-vault.sql
    $participant_id INT := ob_get_participant_id(tn_utils.get_caller_hex());

    if $participant_id IS NULL {
        ERROR('No participant record found for this wallet');
    }



    -- ==========================================================================
    -- SECTION 5: GET ORDER DETAILS
    -- ==========================================================================

    -- Query order from ob_positions table
    -- The composite primary key is: (query_id, participant_id, outcome, price)
    $order_amount INT8;
    for $row in SELECT amount
                FROM ob_positions
                WHERE query_id = $query_id
                  AND participant_id = $participant_id
                  AND outcome = $outcome
                  AND price = $price {
        $order_amount := $row.amount;
    }

    if $order_amount IS NULL {
        ERROR('Order not found or does not belong to you');
    }

    -- ==========================================================================
    -- SECTION 6: REFUND COLLATERAL (BUY ORDERS) OR RETURN SHARES (SELL ORDERS)
    -- ==========================================================================

    -- For buy orders (price < 0): Refund locked collateral to user's wallet
    if $price < 0 {
        -- Refund in bridge token base units. units_per_dollar comes from
        -- get_bridge_units_per_dollar (single source of per-bridge decimals).
        -- $price is INT in [1, 99]; divide by 100 to convert to dollars.
        $abs_price INT := -$price;
        $units_per_dollar NUMERIC(78, 0) := get_bridge_units_per_dollar($bridge);
        $refund_amount NUMERIC(78, 0) := ($order_amount::NUMERIC(78, 0) *
                                            $abs_price::NUMERIC(78, 0) *
                                            $units_per_dollar) / 100::NUMERIC(78, 0);

        -- Unlock collateral back to user using helper from 031-order-book-vault.sql
        -- Passes bridge parameter to unlock from correct bridge
        ob_unlock_collateral($bridge, @caller, $refund_amount);

        -- Record impact for refund (Buy orders)
        ob_record_tx_impact($participant_id, $outcome, 0::INT8, $refund_amount, FALSE);
    }

    -- For sell orders (price > 0): Return shares to holding wallet
    if $price > 0 {
        -- Move shares back to holding wallet (price = 0)
        -- If user already has holdings for this outcome, amounts accumulate (UPSERT)
        INSERT INTO ob_positions (query_id, participant_id, outcome, price, amount, last_updated)
        VALUES ($query_id, $participant_id, $outcome, 0::INT, $order_amount, @block_timestamp)
        ON CONFLICT (query_id, participant_id, outcome, price)
        DO UPDATE SET
            amount = ob_positions.amount + EXCLUDED.amount,
            last_updated = EXCLUDED.last_updated;
    }

    -- Record cancel event (use absolute price for display)
    $abs_cancel_price INT;
    if $price < 0 {
        $abs_cancel_price := -$price;
    } else {
        $abs_cancel_price := $price;
    }
    ob_record_order_event($query_id, $participant_id, 'cancelled', $outcome, $abs_cancel_price, $order_amount, NULL);

    -- ==========================================================================
    -- SECTION 7: DELETE CANCELLED ORDER
    -- ==========================================================================

    -- Delete the cancelled order from ob_positions table
    -- This uses the composite primary key to identify the exact order
    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND participant_id = $participant_id
      AND outcome = $outcome
      AND price = $price;

    -- ==========================================================================
    -- SECTION 8: CLEANUP & MATERIALIZE IMPACTS
    -- ==========================================================================

    ob_cleanup_tx_payouts($query_id);

    -- Success: Order cancelled
    -- - For buy orders: Collateral has been refunded
    -- - For sell orders: Shares have been returned to holdings
    -- - Order is completely removed from the order book
};

-- =============================================================================
-- change_bid: Atomically modify buy order price
-- =============================================================================
/**
 * Atomically cancels an existing buy order and places a new buy order at a different price.
 * This is critical for market makers who need to adjust prices without losing FIFO priority
 * or creating liquidity gaps.
 *
 * Key Features:
 * - ATOMIC: Either both cancel+place succeed, or neither happens
 * - TIMESTAMP PRESERVATION: New order inherits old order's last_updated timestamp (maintains FIFO queue position)
 * - NET COLLATERAL: Only locks/unlocks the difference in collateral needed
 * - FLEXIBLE AMOUNT: Can increase or decrease order size during modification
 *
 * Market Maker Use Case:
 * - MM has: Buy 100 YES @ $0.54 (timestamp T1, 54 TRUF locked)
 * - Market moves down, MM needs to adjust to $0.50
 * - MM calls: change_bid(query_id, TRUE, -54, -50, 100)
 * - Result: Buy 100 YES @ $0.50 (timestamp T1 preserved, 50 TRUF locked, 4 TRUF refunded)
 *
 * Partial Match Scenario:
 * - Original order: 100 shares @ $0.54
 * - Matched: 30 shares (70 remaining in book)
 * - User calls: change_bid(old=-54, new=-50, new_amount=100)
 * - Result: 100 shares @ $0.50 (upsizing from 70 to 100, locks additional 15 TRUF)
 *
 * Collateral Calculation (in bridge token base units):
 * - Old collateral: ($old_amount * |old_price| * units_per_dollar) / 100
 * - New collateral: ($new_amount * |new_price| * units_per_dollar) / 100
 * - Delta: new_collateral - old_collateral
 * - If delta > 0: Lock additional collateral (will ERROR if insufficient balance)
 * - If delta < 0: Unlock excess collateral
 * - If delta = 0: No collateral change
 *
 * Parameters:
 * - $query_id: Market ID from ob_queries
 * - $outcome: TRUE (YES shares) or FALSE (NO shares)
 * - $old_price: Current buy order price (must be negative: -99 to -1)
 * - $new_price: New buy order price (must be negative: -99 to -1, must differ from old_price)
 * - $new_amount: New order amount in shares (can be larger or smaller than old amount)
 *
 * Returns: Nothing (void action)
 *
 * Errors:
 * - If caller address is invalid format
 * - If parameters are invalid (query_id, outcome, prices, amount)
 * - If prices are not both negative (buy orders only)
 * - If new_price equals old_price (use cancel_order instead)
 * - If market doesn't exist or is already settled
 * - If old order doesn't exist at specified price
 * - If insufficient balance for collateral increase
 *
 * Examples:
 *   change_bid(1, TRUE, -54, -50, 100)   -- Adjust buy from $0.54 to $0.50 (lower price)
 *   change_bid(1, TRUE, -50, -56, 100)   -- Adjust buy from $0.50 to $0.56 (higher price)
 *   change_bid(1, FALSE, -44, -40, 200)  -- Adjust NO buy, also change amount
 */
CREATE OR REPLACE ACTION change_bid(
    $query_id INT,
    $outcome BOOL,
    $old_price INT,
    $new_price INT,
    $new_amount INT8
) PUBLIC {
    -- ==========================================================================
    -- SECTION 0: GET MARKET BRIDGE
    -- ==========================================================================

    -- Get market bridge (will ERROR if market doesn't exist)
    $bridge TEXT := get_market_bridge($query_id);

    -- Safe caller normalization using precompiles
    $caller_bytes BYTEA := tn_utils.get_caller_bytes();

    -- ==========================================================================
    -- SECTION 2: VALIDATE PARAMETERS
    -- ==========================================================================

    if $query_id IS NULL OR $query_id < 1 {
        ERROR('Invalid query_id');
    }

    if $outcome IS NULL {
        ERROR('Outcome must be specified (TRUE for YES, FALSE for NO)');
    }

    -- Validate old_price (must be negative for buy orders)
    if $old_price IS NULL OR $old_price >= 0 OR $old_price < -99 {
        ERROR('Old price must be negative (buy order) between -99 and -1');
    }

    -- Validate new_price (must be negative for buy orders)
    if $new_price IS NULL OR $new_price >= 0 OR $new_price < -99 {
        ERROR('New price must be negative (buy order) between -99 and -1');
    }

    -- Validate prices are different
    if $old_price = $new_price {
        ERROR('New price must differ from old price. Use cancel_order() to remove an order.');
    }

    -- Validate amount
    if $new_amount IS NULL OR $new_amount <= 0 {
        ERROR('New amount must be positive');
    }

    if $new_amount > 1000000000 {
        ERROR('amount exceeds maximum allowed of 1,000,000,000');
    }

    -- ==========================================================================
    -- SECTION 3: VALIDATE MARKET
    -- ==========================================================================

    $settled BOOL;
    $settle_time INT8;
    for $row in SELECT settled, settle_time FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
        $settle_time := $row.settle_time;
    }

    if $settled IS NULL {
        ERROR('Market does not exist');
    }

    if $settled {
        ERROR('Cannot modify orders on settled market');
    }

    -- Trading Cutoff: Prevent modifying orders after settlement time
    if @block_timestamp >= $settle_time {
        ERROR('Trading is closed. Market has passed its settlement time.');
    }

    -- ==========================================================================
    -- SECTION 4: GET OLD ORDER DETAILS
    -- ==========================================================================

    $participant_id INT := ob_get_participant_id(tn_utils.get_caller_hex());
    if $participant_id IS NULL {
        ERROR('No participant record found for this wallet');
    }

    -- Get old order amount and timestamp
    $old_amount INT8;
    $old_timestamp INT8;
    for $row in SELECT amount, last_updated FROM ob_positions
                WHERE query_id = $query_id
                  AND participant_id = $participant_id
                  AND outcome = $outcome
                  AND price = $old_price {
        $old_amount := $row.amount;
        $old_timestamp := $row.last_updated;
    }

    if $old_amount IS NULL {
        ERROR('Old order not found at specified price');
    }

    -- ==========================================================================
    -- SECTION 5: CALCULATE COLLATERAL CHANGE
    -- ==========================================================================

    -- Buy order collateral in bridge token base units:
    --   ($amount * |price| * units_per_dollar) / 100
    -- units_per_dollar comes from get_bridge_units_per_dollar (single source
    -- of per-bridge decimals). $price is INT in [1, 99].
    $units_per_dollar NUMERIC(78, 0) := get_bridge_units_per_dollar($bridge);
    $old_abs_price INT := -$old_price;
    $new_abs_price INT := -$new_price;

    $old_collateral NUMERIC(78, 0) := ($old_amount::NUMERIC(78, 0) *
                                        $old_abs_price::NUMERIC(78, 0) *
                                        $units_per_dollar) / 100::NUMERIC(78, 0);

    $new_collateral NUMERIC(78, 0) := ($new_amount::NUMERIC(78, 0) *
                                        $new_abs_price::NUMERIC(78, 0) *
                                        $units_per_dollar) / 100::NUMERIC(78, 0);

    $collateral_delta NUMERIC(78, 0) := $new_collateral - $old_collateral;
    $zero NUMERIC(78, 0) := '0'::NUMERIC(78, 0);

    -- ==========================================================================
    -- SECTION 6: ADJUST COLLATERAL (NET CHANGE ONLY)
    -- ==========================================================================

    if $collateral_delta > $zero {
        -- New order needs MORE collateral
        -- Lock additional amount (will ERROR if insufficient balance)
        if $bridge = 'hoodi_tt2' {
            hoodi_tt2.lock($collateral_delta);
        } else if $bridge = 'sepolia_bridge' {
            sepolia_bridge.lock($collateral_delta);
        } else if $bridge = 'ethereum_bridge' {
            ethereum_bridge.lock($collateral_delta);
        }

        -- Record initial impact (lock)
        ob_record_tx_impact($participant_id, $outcome, 0::INT8, $collateral_delta, TRUE);
        } else if $collateral_delta < $zero {
        -- New order needs LESS collateral
        -- Unlock excess amount
        $unlock_amount NUMERIC(78, 0) := $zero - $collateral_delta;  -- Make positive
        ob_unlock_collateral($bridge, @caller, $unlock_amount);

        -- Record initial impact (refund)
        ob_record_tx_impact($participant_id, $outcome, 0::INT8, $unlock_amount, FALSE);
        }
        -- If $collateral_delta = 0, no collateral adjustment needed

    -- ==========================================================================
    -- SECTION 7: DELETE OLD ORDER
    -- ==========================================================================

    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND participant_id = $participant_id
      AND outcome = $outcome
      AND price = $old_price;

    -- ==========================================================================
    -- SECTION 8: INSERT NEW ORDER (PRESERVING TIMESTAMP)
    -- ==========================================================================

    -- CRITICAL: Use old_timestamp to preserve FIFO priority
    -- If accumulating with existing order at new price, keep EARLIEST timestamp
    INSERT INTO ob_positions
    (query_id, participant_id, outcome, price, amount, last_updated)
    VALUES ($query_id, $participant_id, $outcome, $new_price, $new_amount, $old_timestamp)
    ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
    SET amount = ob_positions.amount + EXCLUDED.amount,
        last_updated = CASE
            WHEN ob_positions.last_updated < EXCLUDED.last_updated
            THEN ob_positions.last_updated  -- Keep earlier timestamp (existing order was first)
            ELSE EXCLUDED.last_updated       -- Keep earlier timestamp (moved order was first)
        END;

    -- Record bid change event (positive price for display)
    ob_record_order_event($query_id, $participant_id, 'bid_changed', $outcome, $new_abs_price, $new_amount, NULL);

    -- ==========================================================================
    -- SECTION 9: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Try to match new order immediately
    -- Note: match_orders expects positive price (1-99), so use $new_abs_price not $new_price
    match_orders($query_id, $outcome, $new_abs_price, $bridge);

    -- ==========================================================================
    -- SECTION 10: CLEANUP & MATERIALIZE IMPACTS
    -- ==========================================================================

    ob_cleanup_tx_payouts($query_id);

    -- Success: Buy order price modified atomically
    -- - Old order deleted, new order placed with preserved timestamp
    -- - Collateral adjusted (net change only)
    -- - FIFO priority maintained
};

-- =============================================================================
-- change_ask: Atomically modify sell order price
-- =============================================================================
/**
 * Atomically cancels an existing sell order and places a new sell order at a different price.
 * Similar to change_bid() but for sell orders, managing shares instead of collateral.
 *
 * Key Features:
 * - ATOMIC: Either both cancel+place succeed, or neither happens
 * - TIMESTAMP PRESERVATION: New order inherits old order's last_updated timestamp
 * - FLEXIBLE AMOUNT: Can increase (pull from holdings) or decrease (return to holdings) order size
 * - NO COLLATERAL: Sell orders just move shares between holdings and order book
 *
 * Market Maker Use Case:
 * - MM has: Sell 100 YES @ $0.60 (timestamp T1)
 * - Market moves up, MM needs to adjust to $0.65
 * - MM calls: change_ask(query_id, TRUE, 60, 65, 100)
 * - Result: Sell 100 YES @ $0.65 (timestamp T1 preserved)
 *
 * Amount Adjustment Scenarios:
 * 1. Increase amount (new_amount > old_amount):
 *    - Pulls additional shares from holdings (price = 0)
 *    - Will ERROR if insufficient shares in holdings
 *
 * 2. Decrease amount (new_amount < old_amount):
 *    - Returns excess shares to holdings (price = 0)
 *
 * 3. Same amount (new_amount = old_amount):
 *    - Just moves order to new price
 *
 * Partial Match Scenario:
 * - Original order: 100 shares @ $0.60
 * - Matched: 40 shares (60 remaining in book)
 * - User calls: change_ask(old=60, new=55, new_amount=100)
 * - Result: 100 shares @ $0.55 (pulls 40 shares from holdings to upsize)
 *
 * Parameters:
 * - $query_id: Market ID from ob_queries
 * - $outcome: TRUE (YES shares) or FALSE (NO shares)
 * - $old_price: Current sell order price (must be positive: 1 to 99)
 * - $new_price: New sell order price (must be positive: 1 to 99, must differ from old_price)
 * - $new_amount: New order amount in shares (can be larger or smaller than old amount)
 *
 * Returns: Nothing (void action)
 *
 * Errors:
 * - If caller address is invalid format
 * - If parameters are invalid
 * - If prices are not both positive (sell orders only)
 * - If new_price equals old_price
 * - If market doesn't exist or is already settled
 * - If old order doesn't exist at specified price
 * - If insufficient shares in holdings for amount increase
 *
 * Examples:
 *   change_ask(1, TRUE, 60, 55, 100)   -- Adjust sell from $0.60 to $0.55 (lower price)
 *   change_ask(1, TRUE, 55, 65, 100)   -- Adjust sell from $0.55 to $0.65 (higher price)
 *   change_ask(1, FALSE, 44, 48, 200)  -- Adjust NO sell, also change amount
 */
CREATE OR REPLACE ACTION change_ask(
    $query_id INT,
    $outcome BOOL,
    $old_price INT,
    $new_price INT,
    $new_amount INT8
) PUBLIC {
    -- ==========================================================================
    -- SECTION 0: GET MARKET BRIDGE
    -- ==========================================================================

    -- Get market bridge (will ERROR if market doesn't exist)
    $bridge TEXT := get_market_bridge($query_id);

    -- Safe caller normalization using precompiles
    $caller_bytes BYTEA := tn_utils.get_caller_bytes();

    -- ==========================================================================
    -- SECTION 2: VALIDATE PARAMETERS
    -- ==========================================================================

    if $query_id IS NULL OR $query_id < 1 {
        ERROR('Invalid query_id');
    }

    if $outcome IS NULL {
        ERROR('Outcome must be specified (TRUE for YES, FALSE for NO)');
    }

    -- Validate old_price (must be positive for sell orders)
    if $old_price IS NULL OR $old_price <= 0 OR $old_price > 99 {
        ERROR('Old price must be positive (sell order) between 1 and 99');
    }

    -- Validate new_price (must be positive for sell orders)
    if $new_price IS NULL OR $new_price <= 0 OR $new_price > 99 {
        ERROR('New price must be positive (sell order) between 1 and 99');
    }

    -- Validate prices are different
    if $old_price = $new_price {
        ERROR('New price must differ from old price. Use cancel_order() to remove an order.');
    }

    -- Validate amount
    if $new_amount IS NULL OR $new_amount <= 0 {
        ERROR('New amount must be positive');
    }

    if $new_amount > 1000000000 {
        ERROR('amount exceeds maximum allowed of 1,000,000,000');
    }

    -- ==========================================================================
    -- SECTION 3: VALIDATE MARKET
    -- ==========================================================================

    $settled BOOL;
    $settle_time INT8;
    for $row in SELECT settled, settle_time FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
        $settle_time := $row.settle_time;
    }

    if $settled IS NULL {
        ERROR('Market does not exist');
    }

    if $settled {
        ERROR('Cannot modify orders on settled market');
    }

    -- Trading Cutoff: Prevent modifying orders after settlement time
    if @block_timestamp >= $settle_time {
        ERROR('Trading is closed. Market has passed its settlement time.');
    }

    -- ==========================================================================
    -- SECTION 4: GET OLD ORDER DETAILS
    -- ==========================================================================

    $participant_id INT := ob_get_participant_id(tn_utils.get_caller_hex());
    if $participant_id IS NULL {
        ERROR('No participant record found for this wallet. You must own shares before selling.');
    }

    -- Get old order amount and timestamp
    $old_amount INT8;
    $old_timestamp INT8;
    for $row in SELECT amount, last_updated FROM ob_positions
                WHERE query_id = $query_id
                  AND participant_id = $participant_id
                  AND outcome = $outcome
                  AND price = $old_price {
        $old_amount := $row.amount;
        $old_timestamp := $row.last_updated;
    }

    if $old_amount IS NULL {
        ERROR('Old order not found at specified price');
    }

    -- ==========================================================================
    -- SECTION 5: ADJUST SHARES (IF AMOUNT CHANGED)
    -- ==========================================================================

    if $new_amount > $old_amount {
        -- New order needs MORE shares
        -- Pull additional shares from holdings (price = 0)
        $additional_shares INT8 := $new_amount - $old_amount;

        -- Check holdings
        $held_amount INT8;
        for $row in SELECT amount FROM ob_positions
                    WHERE query_id = $query_id
                      AND participant_id = $participant_id
                      AND outcome = $outcome
                      AND price = 0 {
            $held_amount := $row.amount;
        }

        if $held_amount IS NULL OR $held_amount < $additional_shares {
            ERROR('Insufficient shares in holdings. Need ' || $additional_shares::TEXT ||
                  ' more shares, but only have ' || COALESCE($held_amount, 0::INT8)::TEXT || ' in holdings.');
        }

        -- Reduce holdings
        if $held_amount = $additional_shares {
            -- Depleting all holdings - delete directly to avoid amount=0 constraint violation
            DELETE FROM ob_positions
            WHERE query_id = $query_id
              AND participant_id = $participant_id
              AND outcome = $outcome
              AND price = 0;
        } else {
            -- Partial reduction - update amount
            UPDATE ob_positions
            SET amount = amount - $additional_shares
            WHERE query_id = $query_id
              AND participant_id = $participant_id
              AND outcome = $outcome
              AND price = 0;
        }

    } else if $new_amount < $old_amount {
        -- New order needs FEWER shares
        -- Return excess shares to holdings (price = 0)
        $excess_shares INT8 := $old_amount - $new_amount;

        INSERT INTO ob_positions
        (query_id, participant_id, outcome, price, amount, last_updated)
        VALUES ($query_id, $participant_id, $outcome, 0::INT, $excess_shares, @block_timestamp)
        ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
        SET amount = ob_positions.amount + EXCLUDED.amount,
            last_updated = EXCLUDED.last_updated;
    }
    -- If $new_amount = $old_amount, no share adjustment needed

    -- ==========================================================================
    -- SECTION 6: DELETE OLD ORDER
    -- ==========================================================================

    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND participant_id = $participant_id
      AND outcome = $outcome
      AND price = $old_price;

    -- ==========================================================================
    -- SECTION 7: INSERT NEW ORDER (PRESERVING TIMESTAMP)
    -- ==========================================================================

    -- CRITICAL: Use old_timestamp to preserve FIFO priority
    -- If accumulating with existing order at new price, keep EARLIEST timestamp
    INSERT INTO ob_positions
    (query_id, participant_id, outcome, price, amount, last_updated)
    VALUES ($query_id, $participant_id, $outcome, $new_price, $new_amount, $old_timestamp)
    ON CONFLICT (query_id, participant_id, outcome, price) DO UPDATE
    SET amount = ob_positions.amount + EXCLUDED.amount,
        last_updated = CASE
            WHEN ob_positions.last_updated < EXCLUDED.last_updated
            THEN ob_positions.last_updated  -- Keep earlier timestamp (existing order was first)
            ELSE EXCLUDED.last_updated       -- Keep earlier timestamp (moved order was first)
        END;

    -- Record ask change event
    ob_record_order_event($query_id, $participant_id, 'ask_changed', $outcome, $new_price, $new_amount, NULL);

    -- ==========================================================================
    -- SECTION 8: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Try to match new order immediately
    match_orders($query_id, $outcome, $new_price, $bridge);

    -- ==========================================================================
    -- SECTION 9: CLEANUP & MATERIALIZE IMPACTS
    -- ==========================================================================

    ob_cleanup_tx_payouts($query_id);

    -- Success: Sell order price modified atomically
    -- - Old order deleted, new order placed with preserved timestamp
    -- - Shares adjusted (pulled from or returned to holdings)
    -- - FIFO priority maintained
};

-- =============================================================================
-- settle_market: Settle a prediction market using attestation results
-- =============================================================================
/**
 * Settles a prediction market by retrieving the signed attestation and marking
 * the winning outcome. This is a permissionless action - anyone can settle a
 * market once the settle_time has been reached and the attestation is available.
 *
 * Settlement Process:
 * 1. Validate market exists, not already settled, and settle_time reached
 * 2. Query attestation by market hash (market.hash = attestation.attestation_hash)
 * 3. Verify attestation has been signed
 * 4. Parse result_canonical to extract boolean outcome (TRUE = YES wins, FALSE = NO wins)
 * 5. Mark market as settled with winning_outcome and settled_at timestamp
 *
 * After settlement:
 * - All trading is permanently blocked (buy, sell, split, cancel, change orders)
 * - Users must call claim_payout() to redeem winning shares
 * - Market state is frozen and cannot be changed
 *
 * Parameters:
 * - $query_id: Market ID from ob_queries.id
 *
 * Returns: Nothing (void action)
 *
 * Errors:
 * - If market doesn't exist
 * - If market is already settled
 * - If settle_time has not been reached
 * - If attestation doesn't exist for market hash
 * - If attestation is not yet signed
 * - If result parsing fails
 *
 * Examples:
 *   settle_market(1)  -- Settle market ID 1 using its attestation
 */
CREATE OR REPLACE ACTION settle_market(
    $query_id INT
) PUBLIC {
    -- Quick input validation before integrity checks
    if $query_id IS NULL OR $query_id < 1 {
        ERROR('Invalid query_id');
    }

    -- ==========================================================================
    -- SECTION 0: MARKET INTEGRITY VALIDATION
    -- ==========================================================================
    -- Validate market health before settlement to prevent settlements with
    -- accounting bugs (orphan shares, missing collateral, etc.)
    --
    -- This automatic validation enforces that:
    -- 1. Binary token parity: TRUE shares = FALSE shares (no orphans)
    -- 2. Vault collateral: vault balance matches obligations
    --
    -- If validation fails, settlement is blocked with detailed error message.
    -- See Migration 037 for validate_market_collateral() implementation.

    $valid_binaries BOOL;
    $valid_collateral BOOL;
    $total_true BIGINT;
    $total_false BIGINT;
    $vault_balance NUMERIC(78, 0);
    $expected_collateral NUMERIC(78, 0);
    $open_buys_value BIGINT;
    $validation_found BOOL := false;

    for $validation in validate_market_collateral($query_id) {
        $valid_binaries := $validation.valid_token_binaries;
        $valid_collateral := $validation.valid_collateral;
        $total_true := $validation.total_true;
        $total_false := $validation.total_false;
        $vault_balance := $validation.vault_balance;
        $expected_collateral := $validation.expected_collateral;
        $open_buys_value := $validation.open_buys_value;
        $validation_found := true;
    }

    -- Guard against missing validation data
    if NOT $validation_found {
        ERROR('Validation data not found for query_id: ' || $query_id::TEXT);
    }

    -- Block settlement if binary token parity is violated
    if NOT $valid_binaries {
        ERROR('Cannot settle market: Binary token parity violation. TRUE shares=' ||
              COALESCE($total_true::TEXT, 'NULL') || ', FALSE shares=' || COALESCE($total_false::TEXT, 'NULL') ||
              '. Orphan shares detected - this indicates an accounting bug.');
    }

    -- Block settlement if vault collateral doesn't match obligations
    -- NOTE: Multi-market limitation - vault_balance is GLOBAL (all markets combined),
    -- so collateral validation is only performed for markets with actual positions.
    -- Empty markets (total_true=0, total_false=0) skip this check since they have
    -- no collateral obligations and the vault may contain funds from other markets.
    if $total_true > 0 OR $total_false > 0 {
        if NOT $valid_collateral {
            ERROR('Cannot settle market: Vault collateral mismatch. Expected=' ||
                  COALESCE($expected_collateral::TEXT, 'NULL') || ' base units, Actual=' || COALESCE($vault_balance::TEXT, 'NULL') ||
                  ' base units. This indicates missing or excess collateral.');
        }
    }

    -- ==========================================================================
    -- SECTION 1: VALIDATE MARKET AND TIMING
    -- ==========================================================================

    -- Get market details
    $market_hash BYTEA;
    $settle_time INT8;
    $settled BOOL;
    $market_found BOOL := false;

    for $row in SELECT hash, settle_time, settled
                FROM ob_queries
                WHERE id = $query_id {
        $market_hash := $row.hash;
        $settle_time := $row.settle_time;
        $settled := $row.settled;
        $market_found := true;
    }

    if NOT $market_found {
        ERROR('Market does not exist (query_id: ' || $query_id::TEXT || ')');
    }

    -- Check if already settled
    if $settled {
        ERROR('Market has already been settled');
    }

    -- Check if settle_time has been reached
    -- Note: @block_timestamp is unix epoch seconds of current block
    if @block_timestamp < $settle_time {
        ERROR('Settlement time not yet reached. settle_time: ' || $settle_time::TEXT ||
              ', current time: ' || @block_timestamp::TEXT);
    }

    -- ==========================================================================
    -- SECTION 2: RETRIEVE ATTESTATION
    -- ==========================================================================

    -- Query attestation by hash
    -- Note: market.hash should match attestation.attestation_hash
    -- This links the market to the cryptographic attestation of the query result
    $result_canonical BYTEA;
    $signature BYTEA;
    $attestation_found BOOL := false;

    for $row in SELECT result_canonical, signature
                FROM attestations
                WHERE attestation_hash = $market_hash
                ORDER BY signed_height DESC NULLS LAST
                LIMIT 1 {
        $result_canonical := $row.result_canonical;
        $signature := $row.signature;
        $attestation_found := true;
    }

    if NOT $attestation_found {
        ERROR('Attestation not found for market hash. Market cannot be settled without attestation.');
    }

    -- Verify attestation has been signed
    if $signature IS NULL {
        ERROR('Attestation not yet signed by validator. Please wait for signing to complete.');
    }

    -- ==========================================================================
    -- SECTION 3: PARSE ATTESTATION RESULT
    -- ==========================================================================

    -- Parse result_canonical to extract boolean outcome
    -- Uses tn_utils.parse_attestation_boolean() precompile
    -- Returns: TRUE (YES wins) or FALSE (NO wins)
    $winning_outcome BOOL := tn_utils.parse_attestation_boolean($result_canonical);

    if $winning_outcome IS NULL {
        ERROR('Failed to parse attestation result (result is NULL)');
    }

    -- ==========================================================================
    -- SECTION 4: PROCESS SETTLEMENT AND MARK AS SETTLED
    -- ==========================================================================

    -- Process all payouts, refunds, and fee collection atomically.
    -- IMPORTANT: This must run BEFORE marking settled=true because
    -- process_settlement() calls sample_lp_rewards() which checks settled flag.
    process_settlement($query_id, $winning_outcome);

    -- Mark market as settled AFTER processing completes successfully.
    -- If process_settlement fails, the market remains unsettled for retry.
    UPDATE ob_queries
    SET settled = true,
        winning_outcome = $winning_outcome,
        settled_at = @block_timestamp
    WHERE id = $query_id;
};
