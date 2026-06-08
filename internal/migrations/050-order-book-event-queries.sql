/*
 * MIGRATION 050: ORDER-BOOK EVENT QUERY BY WALLET
 *
 * Read-only getter over ob_order_events (044) so any wallet's order history —
 * placements, fills, cancels, settlement — can be read back by address. The
 * order book already records every event keyed by the acting participant; this
 * exposes a public, address-parameterized view over it.
 *
 * The existing portfolio getters (get_user_positions / get_user_collateral, 038)
 * are @caller-scoped — they only ever return the signer's own rows. That is the
 * wrong shape for monitoring a wallet you do not sign for, e.g. an owner watching
 * an agent wallet (MAA) that a delegated bot operates: the trades are recorded
 * under the MAA's participant id, not the owner's. This getter takes the wallet
 * as an argument so the owner (or an explorer) can read the bot's full activity
 * on the agent wallet without holding its key.
 *
 * Node-side SQL only; reads existing tables, references no bridge namespace, so
 * there is no dev/prod twin.
 */

-- =============================================================================
-- get_order_events_by_wallet: order-event history for one wallet address.
-- $query_id NULL -> all markets; otherwise scoped to that market. Ordered by id
-- (insertion order). Returns an empty result if the wallet never participated.
-- =============================================================================
CREATE OR REPLACE ACTION get_order_events_by_wallet(
    $wallet_address TEXT,
    $query_id INT DEFAULT NULL,
    $limit INT DEFAULT 100,
    $offset INT DEFAULT 0
) PUBLIC VIEW RETURNS TABLE(
    id INT8,
    query_id INT,
    event_type TEXT,
    outcome BOOL,
    price INT,
    amount INT8,
    wallet_address TEXT,
    counterparty_address TEXT,
    block_height INT8,
    block_timestamp INT8
) {
    if $limit IS NULL OR $limit <= 0 { $limit := 100; }
    if $offset IS NULL OR $offset < 0 { $offset := 0; }

    -- Normalize the wallet: accept with/without a 0x prefix; require 40 hex chars
    -- (decode rejects non-hex digits). Mirrors the 048 list-getter normalization.
    $wallet_hex TEXT := LOWER($wallet_address);
    if substring($wallet_hex, 1, 2) = '0x' { $wallet_hex := substring($wallet_hex, 3, length($wallet_hex)); }
    if length($wallet_hex) != 40 { ERROR('wallet_address must be a 20-byte hex address (40 hex chars, optional 0x prefix)'); }
    $wallet_bytes BYTEA := decode($wallet_hex, 'hex');

    -- Resolve the participant id; a wallet that never traded has no row -> empty result.
    $pid INT;
    for $p in SELECT id FROM ob_participants WHERE wallet_address = $wallet_bytes {
        $pid := $p.id;
    }
    if $pid IS NULL {
        RETURN;
    }

    for $e in
        SELECT
            e.id,
            e.query_id,
            e.event_type,
            e.outcome,
            e.price,
            e.amount,
            '0x' || encode(part.wallet_address, 'hex') AS wallet_hex,
            CASE WHEN e.counterparty_id IS NULL THEN NULL
                 ELSE '0x' || encode(cp.wallet_address, 'hex') END AS cp_hex,
            e.block_height,
            e.block_timestamp
        FROM ob_order_events e
        JOIN ob_participants part ON e.participant_id = part.id
        LEFT JOIN ob_participants cp ON e.counterparty_id = cp.id
        WHERE e.participant_id = $pid
          AND ($query_id IS NULL OR e.query_id = $query_id)
        ORDER BY e.id ASC
        LIMIT $limit OFFSET $offset
    {
        RETURN NEXT $e.id, $e.query_id, $e.event_type, $e.outcome, $e.price, $e.amount,
                    $e.wallet_hex, $e.cp_hex, $e.block_height, $e.block_timestamp;
    }
};
