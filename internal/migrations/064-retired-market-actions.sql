/*
 * RETIRED MARKET ACTIONS
 *
 * create_market refuses a new market whose action is listed here. Markets that
 * already exist on a retired action keep settling as before: only create_market
 * reads this table.
 *
 * The table ships empty, so this migration changes nothing on its own. Once the
 * SDKs and the market bots create markets on the outcome actions (ids 13 to 16,
 * migration 063), the network owner retires the versions they replace:
 *
 *   INSERT INTO retired_market_actions (action_name) VALUES
 *     ('price_above_threshold'), ('price_below_threshold'),
 *     ('value_in_range'), ('index_change_in_range')
 *   ON CONFLICT (action_name) DO NOTHING;
 *
 * Apply order: create_market (032, or 032.prod on mainnet) reads this table on
 * every call, so the table must exist first. Apply this file before
 * create_market, or in the same exec-sql.
 */

CREATE TABLE IF NOT EXISTS retired_market_actions (
    action_name TEXT PRIMARY KEY
);
