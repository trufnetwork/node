-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/038-order-book-queries.sql
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

CREATE OR REPLACE ACTION get_user_collateral($bridge TEXT DEFAULT 'eth_truf')
PUBLIC VIEW RETURNS (
    total_locked NUMERIC(78, 0),
    buy_orders_locked NUMERIC(78, 0),
    shares_value NUMERIC(78, 0)
) {
    -- Bridge token base units per $1.00 (single source of per-bridge decimals).
    $units_per_dollar NUMERIC(78, 0) := get_bridge_units_per_dollar($bridge);

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

    -- Buy order collateral, in bridge token base units:
    --   ($amount * |price| * units_per_dollar) / 100
    -- Restricted to markets that use $bridge.
    $buy_locked NUMERIC(78, 0);
    for $row in
        SELECT COALESCE(
            SUM(abs(p.price)::NUMERIC(78, 0) * p.amount::NUMERIC(78, 0))::NUMERIC(78, 0),
            0::NUMERIC(78, 0)
        ) as price_amount_sum
        FROM ob_positions p
        JOIN ob_queries q ON p.query_id = q.id
        WHERE p.participant_id = $participant_id
          AND p.price < 0
          AND q.bridge = $bridge
    {
        $buy_locked := ($row.price_amount_sum * $units_per_dollar) / 100::NUMERIC(78, 0);
    }

    -- Shares value (holdings + open sells), valued at $1.00 per share, in
    -- bridge token base units. Restricted to markets that use $bridge.
    --
    -- KNOWN ISSUE: this currently sums YES + NO unconditionally, so a paired
    -- holding (e.g. 100 YES + 100 NO from a split mint) reports 200 × $1
    -- instead of the 100 × $1 collateral that actually backs it. Correct
    -- per-market netting needs MAX(net_yes, net_no), but changing it shifts
    -- the returned values that off-chain consumers depend on — defer to a
    -- separate, coordinated change.
    $shares_value NUMERIC(78, 0);
    for $row in
        SELECT COALESCE(SUM(p.amount)::NUMERIC(78, 0), 0::NUMERIC(78, 0)) as amount_sum
        FROM ob_positions p
        JOIN ob_queries q ON p.query_id = q.id
        WHERE p.participant_id = $participant_id
          AND p.price >= 0
          AND q.bridge = $bridge
    {
        $shares_value := $row.amount_sum * $units_per_dollar;
    }

    -- Total locked collateral
    $total_locked NUMERIC(78, 0) := $buy_locked + $shares_value;

    RETURN $total_locked, $buy_locked, $shares_value;
};
