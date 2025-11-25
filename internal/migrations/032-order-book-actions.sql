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

    -- Validate query hash (must not be NULL)
    -- Note: length() doesn't support BYTEA in Kuneiform, so we rely on table constraint
    if $query_hash IS NULL {
        ERROR('query_hash is required');
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
