-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/037-order-book-validation.sql
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

CREATE OR REPLACE ACTION validate_market_collateral($query_id INT)
PUBLIC VIEW RETURNS (
    valid_token_binaries BOOL,
    valid_collateral BOOL,
    total_true BIGINT,
    total_false BIGINT,
    vault_balance NUMERIC(78, 0),
    expected_collateral NUMERIC(78, 0),
    open_buys_value BIGINT
) {
    -- Step 1: Count TRUE shares in circulation for THIS market (holdings + open sells)
    $total_true BIGINT := 0;
    for $row in
        SELECT COALESCE(SUM(amount)::BIGINT, 0::BIGINT) as total
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = TRUE
          AND price >= 0  -- Holdings (price=0) + open sells (price>0)
    {
        $total_true := $row.total;
    }

    -- Step 2: Count FALSE shares in circulation for THIS market (holdings + open sells)
    $total_false BIGINT := 0;
    for $row in
        SELECT COALESCE(SUM(amount)::BIGINT, 0::BIGINT) as total
        FROM ob_positions
        WHERE query_id = $query_id
          AND outcome = FALSE
          AND price >= 0  -- Holdings (price=0) + open sells (price>0)
    {
        $total_false := $row.total;
    }

    -- Step 3: Calculate open buy collateral obligations for THIS market (in cents)
    -- Buy orders: price is negative (stored in cents: -1 to -99)
    -- Collateral per buy order = |price| * amount / 100 (converted to dollars)
    -- We return the value in cents for precision
    $open_buys_value BIGINT := 0;
    for $row in
        SELECT COALESCE(SUM(ABS(price) * amount)::BIGINT, 0::BIGINT) as total_value
        FROM ob_positions
        WHERE query_id = $query_id
          AND price < 0  -- Only buy orders (negative price)
    {
        $open_buys_value := $row.total_value;
    }

    -- Step 4: Get market's bridge
    $bridge TEXT;
    for $row in SELECT bridge FROM ob_queries WHERE id = $query_id {
        $bridge := $row.bridge;
    }
    if $bridge IS NULL {
        ERROR('Market not found for query_id: ' || $query_id::TEXT);
    }

    -- Step 5: Calculate TOTAL expected collateral across ALL unsettled markets using same bridge
    -- This fixes the multi-market validation issue where vault holds collateral for all markets
    $total_shares_all_markets BIGINT := 0;
    $total_buys_cents_all_markets BIGINT := 0;

    -- Sum TRUE shares (holdings + sells) across all unsettled markets with same bridge
    for $row in
        SELECT COALESCE(SUM(p.amount)::BIGINT, 0::BIGINT) as total
        FROM ob_positions p
        JOIN ob_queries q ON p.query_id = q.id
        WHERE q.settled = FALSE
          AND q.bridge = $bridge
          AND p.outcome = TRUE
          AND p.price >= 0
    {
        $total_shares_all_markets := $row.total;
    }

    -- Sum open buy orders (in cents) across all unsettled markets with same bridge
    for $row in
        SELECT COALESCE(SUM(ABS(p.price) * p.amount)::BIGINT, 0::BIGINT) as total_value
        FROM ob_positions p
        JOIN ob_queries q ON p.query_id = q.id
        WHERE q.settled = FALSE
          AND q.bridge = $bridge
          AND p.price < 0
    {
        $total_buys_cents_all_markets := $row.total_value;
    }

    -- Calculate total expected collateral in wei (18 decimals)
    $expected_collateral NUMERIC(78, 0);
    $shares_collateral NUMERIC(78, 0) := $total_shares_all_markets::NUMERIC(78, 0) * '1000000000000000000'::NUMERIC(78, 0);
    $buys_collateral NUMERIC(78, 0) := ($total_buys_cents_all_markets::NUMERIC(78, 0) * '1000000000000000000'::NUMERIC(78, 0)) / 100::NUMERIC(78, 0);
    $expected_collateral := ($shares_collateral + $buys_collateral)::NUMERIC(78, 0);

    -- Step 6: Get actual vault balance from bridge
    $vault_balance NUMERIC(78, 0) := 0::NUMERIC(78, 0);
    $row_count INT := 0;

    if $bridge != 'eth_usdc' {
        ERROR('Invalid bridge. Supported: eth_usdc');
    }
    for $info in eth_usdc.info() {
        $vault_balance := $info.balance;
        $row_count := $row_count + 1;
    }

    -- Validate that bridge returned data (distinguish unavailable from empty vault)
    if $row_count = 0 {
        ERROR('Cannot validate collateral: bridge.info() returned no data. Bridge may be unavailable or not initialized.');
    }

    -- Step 7: Validate binary token parity for THIS market
    $valid_token_binaries BOOL;
    if $total_true = $total_false {
        $valid_token_binaries := TRUE;
    } else {
        $valid_token_binaries := FALSE;
    }

    -- Step 8: Validate collateral balance
    -- Now compares vault balance against TOTAL expected collateral from ALL unsettled markets
    -- Using >= because having MORE collateral than expected is safe (extra margin),
    -- while having LESS would indicate missing funds (which would fail this check)
    $valid_collateral BOOL;
    if $vault_balance >= $expected_collateral {
        $valid_collateral := TRUE;
    } else {
        $valid_collateral := FALSE;
    }

    -- Step 9: Return diagnostics
    RETURN
        $valid_token_binaries,
        $valid_collateral,
        $total_true,
        $total_false,
        $vault_balance,
        $expected_collateral,
        $open_buys_value;
};
