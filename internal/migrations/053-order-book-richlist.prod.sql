/*
 * MIGRATION 053 (PROD TWIN): ORDER-BOOK RICHLIST (ORDERED TOKEN BALANCES)
 *
 * Mainnet twin of 053-order-book-richlist.sql. Identical logic; the only
 * difference is the bridge alias each token resolves to: mainnet `eth_truf` /
 * `eth_usdc` here vs dev `hoodi_tt` / `hoodi_tt2` in the .sql file. A bridge-alias
 * namespace is a compile-time reference, so the token argument is mapped with an
 * if/else — which is why this file exists as a separate prod twin.
 *
 * Only adds a new PUBLIC VIEW read (no ALTER, no writes), so it is safe to apply
 * against a live, settling node.
 */

-- =============================================================================
-- get_ordered_balances: a token's wallets ordered by balance (richlist).
--   $token       'TRUF' | 'USDC' (case-insensitive).
--   $ascending   false (default) = largest first; true = smallest first.
--   $limit       number of rows, clamped to [1, 50] (hard cap 50).
--   $min_balance only wallets with balance >= this (token base units); default 0.
-- Balances are token base units (18 decimals TRUF, 6 decimals USDC). Addresses
-- are returned as '0x'-prefixed lowercase hex. A token with no holders (or none
-- above the threshold) returns an empty result.
-- =============================================================================
CREATE OR REPLACE ACTION get_ordered_balances(
    $token TEXT,
    $ascending BOOL DEFAULT false,
    $limit INT DEFAULT 20,
    $min_balance NUMERIC(78, 0) DEFAULT NULL
) PUBLIC VIEW RETURNS TABLE(
    address TEXT,
    balance NUMERIC(78, 0)
) {
    -- Resolve the token's reward_id from its bridge alias (mainnet: eth_truf/eth_usdc).
    $reward_id UUID;
    $token_key TEXT := lower($token);
    if $token_key = 'truf' {
        $reward_id := eth_truf.id();
    } else if $token_key = 'usdc' {
        $reward_id := eth_usdc.id();
    } else {
        ERROR('unsupported token (want TRUF or USDC): ' || $token);
    }

    -- Enforce the hard cap of 50 (and a sane floor of 1).
    if $limit IS NULL OR $limit < 1 {
        $limit := 1;
    }
    if $limit > 50 {
        $limit := 50;
    }

    -- Threshold in token base units; NULL means no threshold (0).
    $threshold NUMERIC(78, 0) := COALESCE($min_balance, 0::NUMERIC(78, 0));

    -- Ordered read from the per-token balance ledger. Sort direction can't be
    -- parametrized, so branch on $ascending. The address is hex-encoded in the
    -- SELECT projection (function calls are not allowed inside a FOR loop body).
    if $ascending {
        for $row_asc in
            SELECT '0x' || encode(address, 'hex') AS addr, balance
            FROM kwil_erc20_meta.balances
            WHERE reward_id = $reward_id AND balance >= $threshold
            ORDER BY balance ASC
            LIMIT $limit
        {
            RETURN NEXT $row_asc.addr, $row_asc.balance;
        }
    } else {
        for $row_desc in
            SELECT '0x' || encode(address, 'hex') AS addr, balance
            FROM kwil_erc20_meta.balances
            WHERE reward_id = $reward_id AND balance >= $threshold
            ORDER BY balance DESC
            LIMIT $limit
        {
            RETURN NEXT $row_desc.addr, $row_desc.balance;
        }
    }
};
