/*
 * ORDER BOOK ACTIONS
 *
 * User-facing actions for the prediction market:
 * - create_market: Create a new prediction market
 * - get_market_info: Get market details by ID
 * - list_markets: List markets with optional filtering
 */

-- =============================================================================
-- create_market: Create a new prediction market
-- =============================================================================
/**
 * Creates a market with specified settlement parameters. The market is
 * identified by a unique hash derived from the attestation query parameters.
 *
 * Parameters:
 * - $query_hash: SHA256 hash of (data_provider + stream_id + action_id + args) - 32 bytes
 * - $settle_time: Unix timestamp when market can be settled (must be in future)
 * - $max_spread: Maximum spread for LP rewards (1-50 cents)
 * - $min_order_size: Minimum order size for LP rewards (must be positive)
 *
 * Returns:
 * - query_id: The integer ID of the created market
 *
 * Fees:
 * - 2 TRUF market creation fee (TODO: adjust based on spam prevention needs)
 */
CREATE OR REPLACE ACTION create_market(
    $query_hash BYTEA,
    $settle_time INT8,
    $max_spread INT,
    $min_order_size INT8
) PUBLIC RETURNS (query_id INT) {
    -- ==========================================================================
    -- VALIDATION
    -- ==========================================================================

    -- Validate query hash (must be exactly 32 bytes for SHA256)
    -- Note: length() doesn't support BYTEA in Kuneiform, so we use encode() to convert
    -- to hex string and check the length (32 bytes = 64 hex characters)
    if $query_hash IS NULL {
        ERROR('query_hash is required');
    }
    if length(encode($query_hash, 'hex')) != 64 {
        ERROR('query_hash must be exactly 32 bytes (SHA256)');
    }

    -- Validate settlement time (must be in the future)
    -- Use @block_timestamp (unix epoch seconds of current block)
    if $settle_time IS NULL OR $settle_time <= @block_timestamp {
        ERROR('Settlement time must be in the future');
    }

    -- Validate max_spread (1-50 cents)
    if $max_spread IS NULL OR $max_spread < 1 OR $max_spread > 50 {
        ERROR('Max spread must be between 1 and 50 cents');
    }

    -- Validate min_order_size (must be positive)
    if $min_order_size IS NULL OR $min_order_size < 1 {
        ERROR('Minimum order size must be positive');
    }

    -- ==========================================================================
    -- FEE COLLECTION
    -- ==========================================================================
    -- Fee: 2 TRUF (2 * 10^18 wei)
    -- TODO: Adjust fee based on spam prevention needs
    $market_creation_fee NUMERIC(78, 0) := '2000000000000000000'::NUMERIC(78, 0);

    -- Check caller has sufficient balance
    -- TODO: Review when USDC bridge available
    $caller_balance NUMERIC(78, 0) := COALESCE(ethereum_bridge.balance(@caller), 0::NUMERIC(78, 0));

    if $caller_balance < $market_creation_fee {
        ERROR('Insufficient balance for market creation fee. Required: 2 TRUF');
    }

    -- Verify leader address is available for fee transfer
    if @leader_sender IS NULL {
        ERROR('Leader address not available for fee transfer');
    }

    -- Transfer fee to leader
    -- Note: Bridge operations throw ERROR on failure (insufficient balance, etc.)
    -- so no explicit return value check is needed
    $leader_hex TEXT := encode(@leader_sender, 'hex')::TEXT;
    ethereum_bridge.transfer($leader_hex, $market_creation_fee);

    -- ==========================================================================
    -- CREATE MARKET
    -- ==========================================================================

    -- Validate @caller format (must be 0x-prefixed Ethereum address)
    -- Note: Kwil supports both Secp256k1 (EVM) and ED25519 signers. This action
    -- requires a 0x-prefixed Ethereum address format for EVM compatibility.
    if @caller IS NULL OR length(@caller) != 42 OR substring(LOWER(@caller), 1, 2) != '0x' {
        ERROR('Invalid caller address format (expected 0x-prefixed Ethereum address)');
    }

    -- Convert caller address to bytes for storage
    $caller_bytes BYTEA := decode(substring(LOWER(@caller), 3, 40), 'hex');

    -- Insert market record with MAX(id) + 1 pattern
    -- Note: This is safe in Kwil because transactions within a block are processed
    -- sequentially by the consensus engine, not concurrently.
    INSERT INTO ob_queries (
        id,
        hash,
        settle_time,
        max_spread,
        min_order_size,
        created_at,
        creator
    )
    SELECT
        COALESCE(MAX(id), 0) + 1,
        $query_hash,
        $settle_time,
        $max_spread,
        $min_order_size,
        @height,
        $caller_bytes
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
        '0x' || $leader_hex,
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
        SELECT hash, settle_time, settled, winning_outcome, settled_at,
               max_spread, min_order_size, created_at, creator
        FROM ob_queries
        WHERE id = $query_id
    {
        RETURN $market.hash, $market.settle_time, $market.settled,
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
-- match_direct: Direct match implementation
-- =============================================================================
/**
 * Matches buy and sell orders at the same price for the same outcome.
 *
 * Example: Buy YES @ $0.56 matches Sell YES @ $0.56
 *
 * Collateral Flow:
 * - Buyer's locked collateral is transferred to seller as payment
 * - Shares transfer from seller's holdings to buyer's holdings
 *
 * Recursion Behavior:
 * - Uses tail recursion to process multiple matches sequentially
 * - Each iteration matches one order pair and removes/reduces them from ob_positions
 * - Recursion depth = number of orders matched at this price level
 * - Natural termination when no more matching orders exist (LIMIT 1 returns nothing)
 * - Maximum depth is bounded by the number of orders in the order book
 * - In practice, depth rarely exceeds 10-20 due to:
 *   * Economic constraints (gas costs for creating many small orders)
 *   * Market maker behavior (traders prefer larger consolidated orders)
 *   * Natural order book dynamics
 * - Worst case: One large order matching 100+ tiny orders requires 100+ separate
 *   transactions to create those orders, making it economically impractical
 *
 * Parameters:
 * - $query_id: Market identifier
 * - $outcome: TRUE (YES) or FALSE (NO)
 * - $price: Positive price (1-99 cents)
 */
CREATE OR REPLACE ACTION match_direct(
    $query_id INT,
    $outcome BOOL,
    $price INT
) PRIVATE {
    -- Get first buy order (FIFO: earliest first)
    $buy_participant_id INT;
    $buy_amount INT8;
    $buy_found BOOL := false;

    for $buy_order in
        SELECT participant_id, amount
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = $outcome
          AND price = -$price  -- Buy orders have negative price
        ORDER BY last_updated ASC  -- FIFO
        LIMIT 1
    {
        $buy_participant_id := $buy_order.participant_id;
        $buy_amount := $buy_order.amount;
        $buy_found := true;
    }

    -- No buy order, exit
    if NOT $buy_found {
        RETURN;
    }

    -- Get first sell order (FIFO: earliest first)
    $sell_participant_id INT;
    $sell_amount INT8;
    $sell_found BOOL := false;

    for $sell_order in
        SELECT participant_id, amount
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = $outcome
          AND price = $price  -- Sell orders have positive price
        ORDER BY last_updated ASC  -- FIFO
        LIMIT 1
    {
        $sell_participant_id := $sell_order.participant_id;
        $sell_amount := $sell_order.amount;
        $sell_found := true;
    }

    -- No sell order, exit
    if NOT $sell_found {
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

    -- Calculate payment to seller
    $multiplier NUMERIC(78, 0) := '10000000000000000'::NUMERIC(78, 0);
    $seller_payment NUMERIC(78, 0) := ($match_amount::NUMERIC(78, 0) *
                                        $price::NUMERIC(78, 0) *
                                        $multiplier);

    -- Transfer payment from vault to seller
    ob_unlock_collateral($seller_wallet_address, $seller_payment);

    -- Transfer shares from seller to buyer
    -- Step 1: Delete fully matched orders FIRST (prevents amount=0 constraint violation)
    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND ((participant_id = $sell_participant_id AND outcome = $outcome
            AND price = $price AND amount = $match_amount)
        OR (participant_id = $buy_participant_id AND outcome = $outcome
            AND price = -$price AND amount = $match_amount));

    -- Step 2: Reduce seller's sell order (only if partial fill)
    UPDATE ob_positions
    SET amount = amount - $match_amount
    WHERE query_id = $query_id
      AND participant_id = $sell_participant_id
      AND outcome = $outcome
      AND price = $price
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
      AND price = -$price
      AND amount > $match_amount;

    -- Recursively call to match next orders
    match_direct($query_id, $outcome, $price);
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
    $no_price INT
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
    match_mint($query_id, $yes_price, $no_price);
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
    $no_price INT
) PRIVATE {
    -- Multiplier for collateral calculations
    $multiplier NUMERIC(78, 0) := '10000000000000000'::NUMERIC(78, 0);

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

    -- Calculate payouts
    $yes_payout NUMERIC(78, 0) := ($burn_amount::NUMERIC(78, 0) *
                                    $yes_price::NUMERIC(78, 0) *
                                    $multiplier);

    $no_payout NUMERIC(78, 0) := ($burn_amount::NUMERIC(78, 0) *
                                   $no_price::NUMERIC(78, 0) *
                                   $multiplier);

    -- Unlock collateral
    ob_unlock_collateral($yes_wallet_address, $yes_payout);
    ob_unlock_collateral($no_wallet_address, $no_payout);

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
    match_burn($query_id, $yes_price, $no_price);
};

-- =============================================================================
-- match_orders: Main matching orchestrator
-- =============================================================================
/**
 * Coordinates all three matching types for a given order.
 *
 * Called automatically after every order placement to attempt matching.
 * Tries all three match types in sequence:
 * 1. Direct match (most common, fastest)
 * 2. Mint match (creates liquidity)
 * 3. Burn match (removes liquidity)
 *
 * Parameters:
 * - $query_id: Market identifier
 * - $outcome: TRUE (YES) or FALSE (NO)
 * - $price: Price level that triggered matching (1-99)
 */
CREATE OR REPLACE ACTION match_orders(
    $query_id INT,
    $outcome BOOL,
    $price INT
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
    -- SECTION 2: TRY DIRECT MATCH
    -- ==========================================================================
    -- Match buy and sell orders at the same price for same outcome
    -- This is the most common match type and should be tried first

    match_direct($query_id, $outcome, $price);

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
            match_mint($query_id, $price, $complementary_price);
        } else {
            -- Matching NO order: look for YES buy at complementary price
            match_mint($query_id, $complementary_price, $price);
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
            match_burn($query_id, $price, $complementary_price);
        } else {
            -- Matching NO order: look for YES sell at complementary price
            match_burn($query_id, $complementary_price, $price);
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
 * Parameters:
 * - $query_id: Market identifier (from ob_queries.id)
 * - $outcome: TRUE for YES shares, FALSE for NO shares
 * - $price: Price per share in cents (1-99 = $0.01 to $0.99)
 * - $amount: Number of shares to buy
 *
 * Collateral locked: amount × price × 10^16
 * Example: 10 shares at $0.56 = 10 × 56 × 10^16 = 5.6 × 10^18 wei (5.6 TRUF)
 *
 * Price Convention:
 * - Buy orders stored with NEGATIVE price (e.g., -56 for $0.56 buy)
 * - Sell orders stored with POSITIVE price (e.g., 56 for $0.56 sell)
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
    -- Constants
    $collateral_decimals INT := 18;

    -- ==========================================================================
    -- SECTION 1: VALIDATION
    -- ==========================================================================

    -- 1.1 Validate @caller format (must be 0x-prefixed Ethereum address)
    -- Note: Kwil supports both Secp256k1 (EVM) and ED25519 signers. This action
    -- requires a 0x-prefixed Ethereum address format for EVM compatibility.
    if @caller IS NULL OR length(@caller) != 42 OR substring(LOWER(@caller), 1, 2) != '0x' {
        ERROR('Invalid caller address format (expected 0x-prefixed Ethereum address)');
    }

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
    $market_found BOOL := false;

    for $row in SELECT settled FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
        $market_found := true;
    }

    if NOT $market_found {
        ERROR('Market does not exist (query_id: ' || $query_id::TEXT || ')');
    }

    -- Note: Markets remain tradable until explicitly settled (settled=true).
    -- The settle_time is metadata indicating when settlement CAN occur, not when it MUST.
    -- Users can continue trading past settle_time until the settlement action is triggered.
    -- This two-phase design allows flexibility in settlement timing.
    if $settled {
        ERROR('Market has already settled (no trading allowed)');
    }

    -- ==========================================================================
    -- SECTION 2: CALCULATE COLLATERAL NEEDED
    -- ==========================================================================

    -- For buy order: collateral = amount × price × 10^16
    -- Example: 10 shares at $0.56 = 10 × 56 × 10^16 = 5.6 × 10^18 wei
    --
    -- Why 10^16?
    -- - Prices are in cents (1-99)
    -- - Token has 18 decimals
    -- - Formula: 10^(18-2) = 10^16
    -- Note: Kuneiform doesn't have POWER(), so we use hardcoded constant
    -- Cast INT8 and INT to NUMERIC for multiplication
    $collateral_needed NUMERIC(78, 0) := ($amount::NUMERIC(78, 0) * $price::NUMERIC(78, 0) * '10000000000000000'::NUMERIC(78, 0));

    -- ==========================================================================
    -- SECTION 3: CHECK BALANCE
    -- ==========================================================================

    $caller_balance NUMERIC(78, 0) := COALESCE(ethereum_bridge.balance(@caller), 0::NUMERIC(78, 0));

    if $caller_balance < $collateral_needed {
        -- Note: Division by 10^18 for display purposes (convert wei to TRUF)
        ERROR('Insufficient balance. Required: ' || $collateral_needed::TEXT || ' wei (' ||
              ($collateral_needed / '1000000000000000000'::NUMERIC(78, 0))::TEXT || ' TRUF)');
    }

    -- ==========================================================================
    -- SECTION 4: GET OR CREATE PARTICIPANT
    -- ==========================================================================

    -- Convert @caller (TEXT like '0x1234...') to BYTEA (20 bytes)
    $caller_bytes BYTEA := decode(substring(LOWER(@caller), 3, 40), 'hex');

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
    -- SECTION 5: LOCK COLLATERAL
    -- ==========================================================================

    -- Lock tokens from user to vault (network-owned balance)
    -- Note: ethereum_bridge.lock() throws ERROR on failure (insufficient balance, etc.)
    -- TODO: Replace ethereum_bridge with usdc_bridge when USDC bridge is deployed
    ethereum_bridge.lock($collateral_needed);

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

    -- ==========================================================================
    -- SECTION 7: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Attempt to match this buy order with existing sell orders
    match_orders($query_id, $outcome, $price);

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
 * - $price: Price per share in cents (1-99 = $0.01 to $0.99)
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

    -- 1.1 Validate @caller format (must be 0x-prefixed Ethereum address)
    -- Note: Kwil supports both Secp256k1 (EVM) and ED25519 signers. This action
    -- requires a 0x-prefixed Ethereum address format for EVM compatibility.
    if @caller IS NULL OR length(@caller) != 42 OR substring(LOWER(@caller), 1, 2) != '0x' {
        ERROR('Invalid caller address format (expected 0x-prefixed Ethereum address)');
    }

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
    $market_found BOOL := false;

    for $row in SELECT settled FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
        $market_found := true;
    }

    if NOT $market_found {
        ERROR('Market does not exist (query_id: ' || $query_id::TEXT || ')');
    }

    -- Note: Markets remain tradable until explicitly settled (settled=true).
    -- The settle_time is metadata indicating when settlement CAN occur, not when it MUST.
    -- Users can continue trading past settle_time until the settlement action is triggered.
    -- This two-phase design allows flexibility in settlement timing.
    if $settled {
        ERROR('Market has already settled (no trading allowed)');
    }

    -- ==========================================================================
    -- SECTION 2: GET PARTICIPANT (NO AUTO-CREATE FOR SELLS)
    -- ==========================================================================

    -- Convert @caller (TEXT like '0x1234...') to BYTEA (20 bytes)
    $caller_bytes BYTEA := decode(substring(LOWER(@caller), 3, 40), 'hex');

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

    -- ==========================================================================
    -- SECTION 5: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Attempt to match this sell order with existing buy orders
    match_orders($query_id, $outcome, $price);

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
 * Parameters:
 * - $query_id: Market identifier (from ob_queries.id)
 * - $true_price: Price for YES shares in cents (1-99 = $0.01 to $0.99)
 * - $amount: Number of share PAIRS to mint
 *
 * Collateral locked: amount × $1.00 (amount × 10^18 wei)
 * Example: 100 share pairs = 100 × 10^18 wei = 100 TRUF
 *
 * Result:
 * - User holds: $amount YES shares at price=0 (holding, not for sale)
 * - User sells: $amount NO shares at price=(100-true_price) (listed for sale)
 *
 * Price Calculation:
 * - YES price: $true_price (e.g., $0.56 = 56 cents)
 * - NO price: 100 - $true_price (e.g., 100 - 56 = 44 cents = $0.44)
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
 * Example with max_spread = 5 cents:
 * - Market midpoint: $0.50
 * - Split @ $0.56/$0.44: Distance = 6¢ → OUTSIDE spread → NOT qualified ❌
 * - Split @ $0.52/$0.48: Distance = 2¢ → WITHIN spread → QUALIFIED ✅
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

    -- 1.1 Validate @caller format (must be 0x-prefixed Ethereum address)
    -- Note: Kwil supports both Secp256k1 (EVM) and ED25519 signers. This action
    -- requires a 0x-prefixed Ethereum address format for EVM compatibility.
    if @caller IS NULL OR length(@caller) != 42 OR substring(LOWER(@caller), 1, 2) != '0x' {
        ERROR('Invalid caller address format (expected 0x-prefixed Ethereum address)');
    }

    -- 1.2 Validate parameters
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

    -- 1.3 Validate market exists and is not settled
    $settled BOOL;
    $market_found BOOL := false;

    for $row in SELECT settled FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
        $market_found := true;
    }

    if NOT $market_found {
        ERROR('Market does not exist (query_id: ' || $query_id::TEXT || ')');
    }

    -- Note: Markets remain tradable until explicitly settled (settled=true).
    -- The settle_time is metadata indicating when settlement CAN occur, not when it MUST.
    -- Users can continue trading past settle_time until the settlement action is triggered.
    -- This two-phase design allows flexibility in settlement timing.
    if $settled {
        ERROR('Market has already settled (no trading allowed)');
    }

    -- ==========================================================================
    -- SECTION 2: CALCULATE COLLATERAL NEEDED
    -- ==========================================================================

    -- For split order: collateral = amount × $1.00 = amount × 10^18
    -- Example: 100 shares × 10^18 = 100,000,000,000,000,000,000 wei = 100 TRUF
    --
    -- Why 10^18?
    -- - Minting a share pair (YES + NO) requires $1.00 total collateral
    -- - Token has 18 decimals
    -- - Formula: $1.00 × 10^18
    -- Note: Kuneiform doesn't have POWER(), so we use hardcoded constant
    -- Cast INT8 to NUMERIC for multiplication
    $collateral_needed NUMERIC(78, 0) := ($amount::NUMERIC(78, 0) * '1000000000000000000'::NUMERIC(78, 0));

    -- ==========================================================================
    -- SECTION 3: CHECK BALANCE
    -- ==========================================================================

    $caller_balance NUMERIC(78, 0) := COALESCE(ethereum_bridge.balance(@caller), 0::NUMERIC(78, 0));

    if $caller_balance < $collateral_needed {
        -- Note: Division by 10^18 for display purposes (convert wei to TRUF)
        ERROR('Insufficient balance. Required: ' || $collateral_needed::TEXT || ' wei (' ||
              ($collateral_needed / '1000000000000000000'::NUMERIC(78, 0))::TEXT || ' TRUF)');
    }

    -- ==========================================================================
    -- SECTION 4: GET OR CREATE PARTICIPANT
    -- ==========================================================================

    -- Convert @caller (TEXT like '0x1234...') to BYTEA (20 bytes)
    $caller_bytes BYTEA := decode(substring(LOWER(@caller), 3, 40), 'hex');

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
    -- SECTION 5: LOCK COLLATERAL
    -- ==========================================================================

    -- Lock tokens from user to vault (network-owned balance)
    -- Note: ethereum_bridge.lock() throws ERROR on failure (insufficient balance, etc.)
    -- TODO: Replace ethereum_bridge with usdc_bridge when USDC bridge is deployed
    ethereum_bridge.lock($collateral_needed);

    -- ==========================================================================
    -- SECTION 6: MINT YES SHARES (HOLDING)
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

    -- ==========================================================================
    -- SECTION 8: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Attempt to match the NO sell order with existing buy orders
    -- Match is attempted on the FALSE (NO) outcome at the false_price
    match_orders($query_id, FALSE, $false_price);

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
 * Collateral Refund Calculation (Buy Orders):
 * - Formula: amount × |price| × 10^16 wei
 * - Example: 10 shares @ $0.56 = 10 × 56 × 10^16 = 5.6 × 10^18 wei (5.6 TRUF)
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
    -- SECTION 1: VALIDATE CALLER
    -- ==========================================================================

    -- Validate @caller format (must be 0x-prefixed Ethereum address)
    -- Note: Kwil supports both Secp256k1 (EVM) and ED25519 signers. This action
    -- requires a 0x-prefixed Ethereum address format for EVM compatibility.
    if @caller IS NULL OR length(@caller) != 42 OR substring(LOWER(@caller), 1, 2) != '0x' {
        ERROR('Invalid caller address format (expected 0x-prefixed Ethereum address)');
    }

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
    $participant_id INT := ob_get_participant_id(@caller);

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
        -- Calculate locked collateral
        -- Formula: amount × |price| × 10^16 wei
        -- Example: 10 shares @ $0.56 = 10 × 56 × 10^16 = 5.6 × 10^18 wei
        --
        -- Why 10^16? Prices are in cents (1-99), we need to convert to 18-decimal wei:
        -- - Price 56 = $0.56 = 0.56 / 1.00
        -- - Collateral per share = price / 100 * 10^18 = price * 10^16

        $abs_price INT := -$price;  -- Convert negative price to positive
        $multiplier NUMERIC(78, 0) := '10000000000000000'::NUMERIC(78, 0);  -- 10^16
        $refund_amount NUMERIC(78, 0) := $order_amount::NUMERIC(78, 0) * $abs_price::NUMERIC(78, 0) * $multiplier;

        -- Unlock collateral back to user using helper from 031-order-book-vault.sql
        -- This calls ethereum_bridge.unlock() internally
        ob_unlock_collateral(@caller, $refund_amount);
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
 * Collateral Calculation:
 * - Old collateral: old_amount × |old_price| × 10^16 wei
 * - New collateral: new_amount × |new_price| × 10^16 wei
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
    -- SECTION 1: VALIDATE CALLER
    -- ==========================================================================

    if @caller IS NULL OR length(@caller) != 42 OR substring(LOWER(@caller), 1, 2) != '0x' {
        ERROR('Invalid caller address format (expected 0x-prefixed Ethereum address)');
    }

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
    for $row in SELECT settled FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
    }

    if $settled IS NULL {
        ERROR('Market does not exist');
    }

    if $settled {
        ERROR('Cannot modify orders on settled market');
    }

    -- ==========================================================================
    -- SECTION 4: GET OLD ORDER DETAILS
    -- ==========================================================================

    $participant_id INT := ob_get_participant_id(@caller);
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

    -- Buy order collateral formula: amount × |price| × 10^16 wei
    -- Example: 100 shares @ $0.54 = 100 × 54 × 10^16 = 5.4 × 10^19 wei (54 TRUF)
    $multiplier NUMERIC(78, 0) := '10000000000000000'::NUMERIC(78, 0);  -- 10^16

    $old_abs_price INT := -$old_price;  -- Make positive
    $new_abs_price INT := -$new_price;  -- Make positive

    $old_collateral NUMERIC(78, 0) := $old_amount::NUMERIC(78, 0) *
                                       $old_abs_price::NUMERIC(78, 0) *
                                       $multiplier;

    $new_collateral NUMERIC(78, 0) := $new_amount::NUMERIC(78, 0) *
                                       $new_abs_price::NUMERIC(78, 0) *
                                       $multiplier;

    $collateral_delta NUMERIC(78, 0) := $new_collateral - $old_collateral;
    $zero NUMERIC(78, 0) := '0'::NUMERIC(78, 0);

    -- ==========================================================================
    -- SECTION 6: ADJUST COLLATERAL (NET CHANGE ONLY)
    -- ==========================================================================

    if $collateral_delta > $zero {
        -- New order needs MORE collateral
        -- Lock additional amount (will ERROR if insufficient balance)
        ethereum_bridge.lock($collateral_delta);

    } else if $collateral_delta < $zero {
        -- New order needs LESS collateral
        -- Unlock excess amount
        $unlock_amount NUMERIC(78, 0) := $zero - $collateral_delta;  -- Make positive
        ob_unlock_collateral(@caller, $unlock_amount);
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

    -- ==========================================================================
    -- SECTION 9: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Try to match new order immediately
    -- Note: match_orders expects positive price (1-99), so use $new_abs_price not $new_price
    match_orders($query_id, $outcome, $new_abs_price);

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
    -- SECTION 1: VALIDATE CALLER
    -- ==========================================================================

    if @caller IS NULL OR length(@caller) != 42 OR substring(LOWER(@caller), 1, 2) != '0x' {
        ERROR('Invalid caller address format (expected 0x-prefixed Ethereum address)');
    }

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
    for $row in SELECT settled FROM ob_queries WHERE id = $query_id {
        $settled := $row.settled;
    }

    if $settled IS NULL {
        ERROR('Market does not exist');
    }

    if $settled {
        ERROR('Cannot modify orders on settled market');
    }

    -- ==========================================================================
    -- SECTION 4: GET OLD ORDER DETAILS
    -- ==========================================================================

    $participant_id INT := ob_get_participant_id(@caller);
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

    -- ==========================================================================
    -- SECTION 8: TRIGGER MATCHING ENGINE
    -- ==========================================================================

    -- Try to match new order immediately
    match_orders($query_id, $outcome, $new_price);

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
    -- ==========================================================================
    -- SECTION 1: VALIDATE MARKET AND TIMING
    -- ==========================================================================

    -- Validate query_id
    if $query_id IS NULL OR $query_id < 1 {
        ERROR('Invalid query_id');
    }

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
                ORDER BY signed_height DESC
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
    -- SECTION 4: MARK MARKET AS SETTLED
    -- ==========================================================================

    -- Update market with settlement information
    UPDATE ob_queries
    SET settled = true,
        winning_outcome = $winning_outcome,
        settled_at = @block_timestamp
    WHERE id = $query_id;

    -- Process all payouts, refunds, and fee collection atomically
    process_settlement($query_id, $winning_outcome);
};
