-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/032-order-book-actions.sql
-- Script : scripts/generate_prod_migrations.py
--
-- Manual-apply mainnet override. The embedded migration loader skips
-- *.prod.sql, so apply via:
--
--     kwil-cli exec-sql --file <this file> --sync \
--         --private-key $PRIVATE_KEY --provider $PROVIDER
--
-- Prerequisite: erc20-bridge/000-extension.prod.sql must be applied
-- FIRST so the eth_truf and eth_usdc bridge instances exist.
-- =============================================================================

CREATE OR REPLACE ACTION validate_bridge($bridge TEXT) PRIVATE {
    if $bridge IS NULL {
        ERROR('bridge parameter is required');
    }

    if $bridge != 'eth_usdc' AND
       $bridge != 'eth_truf' {
        ERROR('Invalid bridge. Supported: eth_usdc, eth_truf');
    }

    RETURN;
};

CREATE OR REPLACE ACTION get_bridge_units_per_dollar($bridge TEXT)
    PUBLIC VIEW RETURNS (units_per_dollar NUMERIC(78, 0)) {
    if $bridge = 'eth_truf' {
        RETURN '1000000000000000000'::NUMERIC(78, 0);
    } else if $bridge = 'eth_usdc' {
        RETURN '1000000'::NUMERIC(78, 0);
    }
    ERROR('Unknown bridge: ' || $bridge);
};

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
    -- Market creation fee is ALWAYS paid in TRUF (eth_truf on testnet)
    -- regardless of which bridge the market uses for collateral
    $market_creation_fee NUMERIC(78, 0) := '2000000000000000000'::NUMERIC(78, 0);

    -- Check caller has sufficient TRUF balance
    -- IMPORTANT: Fee is collected from eth_truf (TRUF), not from market's bridge
    $caller_balance NUMERIC(78, 0) := COALESCE(eth_truf.balance(@caller), 0::NUMERIC(78, 0));

    if $caller_balance < $market_creation_fee {
        ERROR('Insufficient TRUF balance for market creation fee. Required: 2 TRUF (eth_truf balance)');
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

    -- Transfer fee to leader from TRUF bridge (eth_truf)
    eth_truf.transfer($leader_hex, $market_creation_fee);

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
    if $bridge = 'eth_usdc' {
        $caller_balance := COALESCE(eth_usdc.balance(@caller), 0::NUMERIC(78, 0));
    } else if $bridge = 'eth_truf' {
        $caller_balance := COALESCE(eth_truf.balance(@caller), 0::NUMERIC(78, 0));
    }if $caller_balance < $collateral_needed {
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
    if $bridge = 'eth_usdc' {
        eth_usdc.lock($collateral_needed);
    } else if $bridge = 'eth_truf' {
        eth_truf.lock($collateral_needed);
    }-- Record initial impact (collateral spent)
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
    if $bridge = 'eth_usdc' {
        $caller_balance := COALESCE(eth_usdc.balance(@caller), 0::NUMERIC(78, 0));
    } else if $bridge = 'eth_truf' {
        $caller_balance := COALESCE(eth_truf.balance(@caller), 0::NUMERIC(78, 0));
    }if $caller_balance < $collateral_needed {
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
    if $bridge = 'eth_usdc' {
        eth_usdc.lock($collateral_needed);
    } else if $bridge = 'eth_truf' {
        eth_truf.lock($collateral_needed);
    }-- Record initial impacts:
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
        if $bridge = 'eth_usdc' {
            eth_usdc.lock($collateral_delta);
        } else if $bridge = 'eth_truf' {
            eth_truf.lock($collateral_delta);
        }-- Record initial impact (lock)
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
