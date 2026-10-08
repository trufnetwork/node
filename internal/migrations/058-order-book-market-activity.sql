/*
 * MIGRATION 058: ORDER-BOOK MARKET ACTIVITY
 *
 * get_market_activity answers how much traded on one order book over a time
 * window. The order book's other reads (get_market_depth, get_best_prices, ...)
 * answer what is resting right now. Every SDK calls this action rather than
 * carrying its own copy of the volume definition, so the definition lives here
 * once. Goal: https://github.com/trufnetwork/node/issues/1429
 *
 * Reads ob_order_events (044) and ob_queries; references no bridge namespace, so
 * there is no dev/prod twin.
 */

-- =============================================================================
-- get_market_activity: filled volume and participation for one market over
-- [$from_ts, $to_ts], unix seconds, both ends inclusive.
-- =============================================================================
/**
 * Volume counts each trade once, in cents of the market's own collateral:
 * - direct_buy_fill counts price * amount. Its direct_sell_fill twin is the same
 *   trade, so it adds no volume, but its participant is a trader.
 * - mint_fill and burn_fill count 100 * amount on the YES row only. The NO row
 *   is the other side of the same match.
 * - split_placed, placements, cancels, amends and settlement count nothing.
 * The fill types are an allowlist, so an event type added later stays out of
 * volume until someone decides it is volume.
 *
 * Volume is in the market's own collateral (bridge) and is never summable
 * across bridges. Unique traders compare across bridges.
 *
 * ob_order_events is trimmed once indexed, so the node holds a rolling window.
 * coverage_from_block is the earliest block it still holds, and
 * coverage_complete is false when the market was created before that block: a
 * zero from such a market may be truncation rather than inactivity. A node that
 * holds no order events at all reports 0 and false.
 *
 * Returns one row for a market that exists, even when nothing traded in the
 * window, and no row for a market that does not exist.
 *
 * Usage:
 *   kwil-cli call-action get_market_activity int:782 int:1700000000 int:1800000000
 */
CREATE OR REPLACE ACTION get_market_activity(
    $query_id INT,
    $from_ts INT8,
    $to_ts INT8
) PUBLIC VIEW RETURNS TABLE(
    bridge TEXT,
    volume_cents NUMERIC(78, 0),
    direct_cents NUMERIC(78, 0),
    mint_burn_cents NUMERIC(78, 0),
    unique_traders INT,
    fill_count INT,
    direct_fill_count INT,
    shares_traded INT8,
    first_event_ts INT8,
    last_event_ts INT8,
    coverage_from_block INT8,
    coverage_complete BOOL
) {
    if $query_id IS NULL {
        ERROR('query_id is required');
    }
    if $from_ts IS NULL OR $to_ts IS NULL {
        ERROR('from_ts and to_ts are required');
    }
    if $to_ts < $from_ts {
        ERROR('to_ts must not be before from_ts');
    }

    -- Earliest block still held; NULL when the node holds no order events.
    $coverage_from INT8;
    for $c in SELECT MIN(block_height)::INT8 AS min_height FROM ob_order_events {
        $coverage_from := $c.min_height;
    }

    RETURN SELECT
        q.bridge,
        COALESCE(SUM(CASE WHEN e.event_type = 'direct_buy_fill' THEN e.price * e.amount
                          WHEN e.event_type IN ('mint_fill', 'burn_fill') AND e.outcome = TRUE THEN 100 * e.amount
                          ELSE 0 END)::NUMERIC(78,0), 0::NUMERIC(78,0)) AS volume_cents,
        COALESCE(SUM(CASE WHEN e.event_type = 'direct_buy_fill' THEN e.price * e.amount ELSE 0 END)::NUMERIC(78,0), 0::NUMERIC(78,0)) AS direct_cents,
        COALESCE(SUM(CASE WHEN e.event_type IN ('mint_fill', 'burn_fill') AND e.outcome = TRUE THEN 100 * e.amount ELSE 0 END)::NUMERIC(78,0), 0::NUMERIC(78,0)) AS mint_burn_cents,
        COUNT(DISTINCT CASE WHEN e.event_type IN ('direct_buy_fill', 'direct_sell_fill', 'mint_fill', 'burn_fill') THEN e.participant_id END)::INT AS unique_traders,
        COUNT(CASE WHEN e.event_type = 'direct_buy_fill' OR (e.event_type IN ('mint_fill', 'burn_fill') AND e.outcome = TRUE) THEN 1 END)::INT AS fill_count,
        COUNT(CASE WHEN e.event_type = 'direct_buy_fill' THEN 1 END)::INT AS direct_fill_count,
        COALESCE(SUM(CASE WHEN e.event_type = 'direct_buy_fill' OR (e.event_type IN ('mint_fill', 'burn_fill') AND e.outcome = TRUE) THEN e.amount ELSE 0 END)::INT8, 0::INT8) AS shares_traded,
        MIN(CASE WHEN e.event_type IN ('direct_buy_fill', 'direct_sell_fill', 'mint_fill', 'burn_fill') THEN e.block_timestamp END) AS first_event_ts,
        MAX(CASE WHEN e.event_type IN ('direct_buy_fill', 'direct_sell_fill', 'mint_fill', 'burn_fill') THEN e.block_timestamp END) AS last_event_ts,
        COALESCE($coverage_from, 0::INT8) AS coverage_from_block,
        COALESCE($coverage_from <= q.created_at, FALSE) AS coverage_complete
    FROM ob_queries q
    LEFT JOIN ob_order_events e
      ON e.query_id = q.id AND e.block_timestamp >= $from_ts AND e.block_timestamp <= $to_ts
    WHERE q.id = $query_id
    GROUP BY q.id, q.bridge, q.created_at;
};
