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
-- match_orders: Matching engine (stub for Issue 6)
-- =============================================================================
/**
 * Stub for the matching engine. This allows place_buy_order() and place_sell_order()
 * to call match_orders() without blocking on Issue (Order Matching) implementation.
 *
 * TODO: Full implementation in another Issue (Order Matching)
 *
 * The matching engine will implement:
 * 1. Direct match - Buy order meets sell order at same price
 * 2. Mint match - Opposite buy orders at complementary prices create share pairs
 * 3. Burn match - Opposite sell orders at complementary prices destroy shares
 *
 * Parameters:
 * - $query_id: Market identifier
 * - $outcome: TRUE (YES) or FALSE (NO)
 * - $price: Price level to match (1-99)
 */
CREATE OR REPLACE ACTION match_orders(
    $query_id INT,
    $outcome BOOL,
    $price INT
) PRIVATE {
    -- Stub: No-op until Issue (Order Matching)
    -- This prevents errors when called from place_buy_order/place_sell_order
    -- Full matching engine will be implemented in Issue (Order Matching)
    RETURN;
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
    -- Note: This is a stub in Issue 2, full implementation in Issue 6
    match_orders($query_id, $outcome, $price);

    -- Success: Order placed (may be partially or fully matched by future matching engine)
};
