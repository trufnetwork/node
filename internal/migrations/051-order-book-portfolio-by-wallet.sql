/*
 * MIGRATION 051: ORDER-BOOK PORTFOLIO BY WALLET
 *
 * Address-parameterized, read-only getters for a wallet's current portfolio:
 * positions (holdings + open orders) and locked collateral.
 *
 * The 038 portfolio getters (get_user_positions / get_user_collateral) read
 * @caller, so they only ever return the signer's own rows. That is the wrong
 * shape for monitoring a wallet you do not sign for, e.g. an owner (or a bot)
 * watching an agent wallet (MAA) a delegated key operates: the positions are
 * recorded under the MAA's participant id, not the signer's. 050 already did
 * this for order-event history; these two complete the set for the live
 * portfolio an inventory-aware market maker needs.
 *
 * Bodies mirror 038 exactly except the participant is resolved from the
 * $wallet_address argument (normalized like 050) instead of @caller. Because
 * $bridge is a required argument here — not an env-defaulted one — this file
 * references no bridge namespace literal and therefore needs no dev/prod twin.
 * It only adds new PUBLIC VIEW reads over existing tables (no ALTER), so it is
 * safe to apply against a live, settling node.
 */

-- =============================================================================
-- get_positions_by_wallet: a wallet's holdings + open orders across all markets.
-- Mirrors get_user_positions (038) but for an arbitrary address. Returns an
-- empty result if the wallet never participated.
-- =============================================================================
CREATE OR REPLACE ACTION get_positions_by_wallet($wallet_address TEXT)
PUBLIC VIEW RETURNS TABLE(
    query_id INT,
    outcome BOOL,
    price INT,
    amount INT8,
    position_type TEXT
) {
    -- Normalize the wallet: accept with/without a 0x prefix; require 40 hex chars
    -- (decode rejects non-hex digits). Mirrors the 050 getter normalization.
    $wallet_hex TEXT := LOWER($wallet_address);
    if substring($wallet_hex, 1, 2) = '0x' { $wallet_hex := substring($wallet_hex, 3, length($wallet_hex)); }
    if length($wallet_hex) != 40 { ERROR('wallet_address must be a 20-byte hex address (40 hex chars, optional 0x prefix)'); }
    $wallet_bytes BYTEA := decode($wallet_hex, 'hex');

    -- Resolve the participant id; a wallet that never traded has no row -> empty result.
    $participant_id INT;
    for $row in SELECT id FROM ob_participants WHERE wallet_address = $wallet_bytes {
        $participant_id := $row.id;
    }
    if $participant_id IS NULL {
        RETURN;
    }

    -- Return all positions for this wallet (same shape/order as get_user_positions).
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

-- =============================================================================
-- get_collateral_by_wallet: a wallet's locked collateral on one bridge.
-- Mirrors get_user_collateral (038) but for an arbitrary address. $bridge is
-- required (per-bridge decimals), which keeps this twin-free. Returns zeros if
-- the wallet never participated.
-- =============================================================================
CREATE OR REPLACE ACTION get_collateral_by_wallet($wallet_address TEXT, $bridge TEXT)
PUBLIC VIEW RETURNS (
    total_locked NUMERIC(78, 0),
    buy_orders_locked NUMERIC(78, 0),
    shares_value NUMERIC(78, 0)
) {
    -- Bridge token base units per $1.00 (single source of per-bridge decimals).
    $units_per_dollar NUMERIC(78, 0) := get_bridge_units_per_dollar($bridge);

    -- Normalize the wallet: accept with/without a 0x prefix; require 40 hex chars.
    $wallet_hex TEXT := LOWER($wallet_address);
    if substring($wallet_hex, 1, 2) = '0x' { $wallet_hex := substring($wallet_hex, 3, length($wallet_hex)); }
    if length($wallet_hex) != 40 { ERROR('wallet_address must be a 20-byte hex address (40 hex chars, optional 0x prefix)'); }
    $wallet_bytes BYTEA := decode($wallet_hex, 'hex');

    -- Lookup participant ID
    $participant_id INT;
    for $row in SELECT id FROM ob_participants WHERE wallet_address = $wallet_bytes {
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
    -- KNOWN ISSUE (inherited from get_user_collateral): this sums YES + NO
    -- unconditionally, so a paired holding (100 YES + 100 NO from a split mint)
    -- reports 200 × $1 instead of the 100 × $1 collateral that actually backs
    -- it. Correct per-market netting needs MAX(net_yes, net_no); kept identical
    -- to get_user_collateral so both getters agree until that coordinated change.
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
