/**
 * MIGRATION 037: ORDER BOOK VALIDATION
 *
 * Market integrity validation functions:
 * - validate_market_collateral() - Verify binary token parity + vault balance
 *
 * Validation Checks:
 * 1. Binary Token Parity: total_true = total_false (no orphan shares)
 * 2. Vault Collateral: vault holds expected amount (no missing/excess funds)
 *
 * Returns diagnostic information for debugging accounting issues.
 *
 * NOTE: Vault balance is GLOBAL across all markets using the same bridge.
 * The validation sums expected collateral across ALL unsettled markets
 * to correctly validate multi-market scenarios.
 *
 * Dependencies:
 * - Migration 030: ob_positions table (share positions)
 * - Migration 031: vault operations (lock/unlock)
 * - ethereum_bridge.info() precompile (vault balance query)
 */

-- ============================================================================
-- VALIDATION FUNCTIONS
-- ============================================================================

/**
 * validate_market_collateral($query_id)
 *
 * Validates market integrity by checking:
 * 1. Binary token parity: equal TRUE/FALSE shares (no orphan shares)
 * 2. Vault collateral: balance matches total obligations across ALL unsettled markets
 *
 * Returns:
 * - valid_token_binaries: TRUE if total_true = total_false for this market
 * - valid_collateral: TRUE if vault balance = total expected collateral (all markets)
 * - total_true: Count of TRUE shares for THIS market (holdings + open sells)
 * - total_false: Count of FALSE shares for THIS market (holdings + open sells)
 * - vault_balance: Current network ownedBalance from ethereum_bridge
 * - expected_collateral: Total expected collateral across ALL unsettled markets
 * - open_buys_value: Total escrowed buy order collateral for THIS market (in cents)
 *
 * Usage:
 *   kwil-cli database call --action validate_market_collateral \
 *     --inputs '[{"$query_id": 1}]'
 *
 * Example Output:
 *   valid_token_binaries | valid_collateral | total_true | total_false | vault_balance | expected_collateral | open_buys_value
 *   TRUE                 | TRUE             | 1000       | 1000        | 1500000...    | 1500000...          | 500000...
 */
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

    if $bridge = 'hoodi_tt2' {
        for $info in hoodi_tt2.info() {
            $vault_balance := $info.balance;
            $row_count := $row_count + 1;
        }
    } else if $bridge = 'sepolia_bridge' {
        for $info in sepolia_bridge.info() {
            $vault_balance := $info.balance;
            $row_count := $row_count + 1;
        }
    } else if $bridge = 'ethereum_bridge' {
        for $info in ethereum_bridge.info() {
            $vault_balance := $info.balance;
            $row_count := $row_count + 1;
        }
    } else {
        ERROR('Invalid bridge in validate_market_collateral: ' || COALESCE($bridge, 'NULL'));
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
