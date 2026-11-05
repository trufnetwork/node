/*
 * MIGRATION 060: ORDER-BOOK COLLATERAL PAUSE
 *
 * Lets the namespace owner stop new collateral from entering one bridge's
 * order-book vault, for example during bridge maintenance, without stopping
 * anyone from leaving. While a bridge is paused:
 * - refused: place_buy_order, place_split_limit_order, and change_bid when the
 *   new bid needs more collateral than the old one
 * - unchanged: cancel_order, change_bid that keeps or lowers collateral,
 *   place_sell_order, change_ask, settlement and fee distribution
 *
 * Each of those lock paths (032) calls ob_check_collateral_lock_open
 * before it locks. A bridge with no row in ob_collateral_lock_pauses is open,
 * so a network that never uses the switch behaves exactly as before.
 *
 * References no bridge namespace, so there is no dev/prod twin. The bridge
 * name is checked by validate_bridge (032), whose prod override accepts the
 * mainnet bridges.
 */

-- =============================================================================
-- ob_collateral_lock_pauses: bridges that take no new collateral
-- =============================================================================
-- One row per paused bridge. paused_at is the block height of the pause.
CREATE TABLE IF NOT EXISTS ob_collateral_lock_pauses (
    bridge TEXT PRIMARY KEY,
    paused_at INT8 NOT NULL
);

-- =============================================================================
-- ob_check_collateral_lock_open: refuse new collateral on a paused bridge
-- =============================================================================
/**
 * Called before every collateral lock in the order book: place_buy_order,
 * place_split_limit_order, change_bid (only when the new bid needs more
 * collateral) and ob_lock_collateral. Actions that only return or pay out
 * collateral never call it.
 */
CREATE OR REPLACE ACTION ob_check_collateral_lock_open($bridge TEXT) PRIVATE VIEW {
    for $row in SELECT bridge FROM ob_collateral_lock_pauses WHERE bridge = $bridge {
        ERROR('Orders that lock new collateral are paused on ' || $row.bridge);
    }
};

-- =============================================================================
-- set_collateral_lock_paused: owner-only switch for one bridge
-- =============================================================================
/**
 * $paused = TRUE refuses new collateral on $bridge (buys, splits, bid raises).
 * $paused = FALSE reopens it. Both directions are idempotent, and pausing an
 * already paused bridge keeps its first paused_at.
 *
 * Errors:
 * - caller is not the namespace owner (OWNER modifier)
 * - $bridge is NULL or not a bridge this network supports (validate_bridge)
 * - $paused is NULL
 *
 * Usage (owner key):
 *   kwil-cli exec-action set_collateral_lock_paused text:eth_usdc bool:true
 */
CREATE OR REPLACE ACTION set_collateral_lock_paused(
    $bridge TEXT,
    $paused BOOL
) PUBLIC OWNER {
    validate_bridge($bridge);

    if $paused IS NULL {
        ERROR('paused is required (TRUE to pause, FALSE to resume)');
    }

    if $paused {
        INSERT INTO ob_collateral_lock_pauses (bridge, paused_at)
        VALUES ($bridge, @height)
        ON CONFLICT (bridge) DO NOTHING;
    } else {
        DELETE FROM ob_collateral_lock_pauses WHERE bridge = $bridge;
    }
};

-- =============================================================================
-- get_collateral_lock_pauses: which bridges are paused, and since when
-- =============================================================================
/**
 * Returns one row per paused bridge; no rows means every bridge is open.
 *
 * Usage:
 *   kwil-cli call-action get_collateral_lock_pauses
 */
CREATE OR REPLACE ACTION get_collateral_lock_pauses()
PUBLIC VIEW RETURNS TABLE(
    bridge TEXT,
    paused_at INT8
) {
    for $row in SELECT bridge, paused_at FROM ob_collateral_lock_pauses ORDER BY bridge {
        RETURN NEXT $row.bridge, $row.paused_at;
    }
};
