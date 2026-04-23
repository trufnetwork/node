/**
 * MIGRATION 034: DYNAMIC LP REWARDS SAMPLING
 *
 * Purpose:
 * Store periodic snapshots of liquidity provider reward eligibility.
 * Rewards are calculated by sampling the order book state at specific blocks
 * using the Polymarket scoring formula with dynamic spread requirements.
 *
 * Dynamic Rewards Model:
 * - No order-time tracking (replaced old ob_liquidity_providers table)
 * - Periodic sampling via sample_lp_rewards($query_id, $block)
 * - Dynamic spread based on market certainty (midpoint distance from 50¢)
 * - Proportional distribution across sampled blocks at settlement
 *
 * References:
 * - Polymarket Rewards: https://docs.polymarket.com/developers/rewards/overview
 */

-- ============================================================================
-- SCHEMA: Rewards Sampling Table
-- ============================================================================

/**
 * ob_rewards
 *
 * Stores snapshots of LP reward eligibility at sampled blocks.
 * Each row represents one participant's reward percentage at a specific block.
 *
 * Primary Key: (query_id, participant_id, block)
 * - One record per LP per market per sampled block
 * - Multiple blocks can be sampled for the same market
 * - Reward percentages sum to 100% (±0.01% for rounding) per block
 *
 * Sampling Trigger:
 * - Internal system calls sample_lp_rewards($query_id, $block) periodically via EndBlockHook
 * - Recommended: Every 50 blocks (~10 minutes with 12s block times)
 * - Can only be invoked internally (PRIVATE action)
 *
 * Distribution:
 * - At settlement, total fees divided equally across all sampled blocks
 * - Each participant receives: SUM(reward_per_block * reward_percent / 100)
 * - Rows deleted after distribution to save storage
 *
 * Reward Percent:
 * - NUMERIC(5,2): Stores percentages like 42.50 (42.5%)
 * - Range: 0.00 to 100.00
 * - Precision: 2 decimal places (0.01% = 1 basis point)
 */
CREATE TABLE IF NOT EXISTS ob_rewards (
    query_id INT NOT NULL,
    participant_id INT NOT NULL,
    block INT8 NOT NULL,
    reward_percent NUMERIC(5,2) NOT NULL,
    PRIMARY KEY (query_id, participant_id, block),
    FOREIGN KEY (query_id) REFERENCES ob_queries(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES ob_participants(id),
    CHECK (reward_percent >= 0.00 AND reward_percent <= 100.00)
);

/**
 * Index for settlement queries: Get all rewards for a market.
 * Used by distribute_fees() to find all LPs who earned rewards.
 */
CREATE INDEX IF NOT EXISTS idx_ob_rewards_query ON ob_rewards(query_id);

/**
 * Index for efficient block-based queries.
 * Useful for analytics: "Show me rewards distribution at block X"
 */
CREATE INDEX IF NOT EXISTS idx_ob_rewards_query_block ON ob_rewards(query_id, block);

-- ============================================================================
-- ACTIONS: Rewards Sampling (STUB - Implementation in Task 9R2)
-- ============================================================================

/**
 * sample_lp_rewards: Calculate and store LP reward percentages at current block
 *
 * This action samples the order book state at a specific block and calculates
 * reward eligibility using the Polymarket scoring formula with dynamic spreads.
 *
 * Dynamic Spread Rules (based on midpoint distance from 50¢):
 * - Distance 0-29¢:  spread = 5¢ (very uncertain markets)
 * - Distance 30-59¢: spread = 4¢ (moderately certain)
 * - Distance 60-79¢: spread = 3¢ (quite certain)
 * - Distance 80-99¢: INELIGIBLE (nearly settled, no rewards)
 *
 * Polymarket Scoring Formula:
 * - For each paired buy order (YES @ p + NO @ 100-p):
 *   - Calculate distances from midpoint for both sides
 *   - Use minimum distance: min_dist = LEAST(yes_distance, no_distance)
 *   - Score = amount * ((spread - min_dist) / spread)²
 * - Reward percent = (participant_score / total_score) * 100
 *
 * Parameters:
 * - $query_id: Market to sample (must not be settled)
 * - $block: Block height for this sample (used as key in ob_rewards table)
 */
CREATE OR REPLACE ACTION sample_lp_rewards(
    $query_id INT,
    $block INT8
) PRIVATE {
    NOTICE('sample_lp_rewards: START query_id=' || $query_id::TEXT || ' block=' || $block::TEXT);

    -- Check if this block was already sampled to prevent duplicate key errors
    for $row in SELECT 1 FROM ob_rewards WHERE query_id = $query_id AND block = $block LIMIT 1 {
        NOTICE('sample_lp_rewards: RETURN already sampled');
        RETURN;
    }

    -- Check if market is settled
    $is_settled BOOL;
    for $row in SELECT settled FROM ob_queries WHERE id = $query_id {
        $is_settled := $row.settled;
    }
    if $is_settled {
        ERROR('Market is already settled');
    }

    -- =========================================================================
    -- Midpoint Calculation (Spec-aligned: YES positions only)
    -- =========================================================================

    -- Step A: Best YES bid (highest YES buy = most negative price)
    -- ORDER BY price ASC LIMIT 1 gets the most negative = highest bid
    $x_mid INT;
    $has_bid BOOL := FALSE;
    for $row in SELECT price FROM ob_positions WHERE query_id = $query_id AND outcome = TRUE AND price < 0 ORDER BY price ASC LIMIT 1 {
        $x_mid := $row.price;  -- negative value, e.g. -34
        $has_bid := TRUE;
        NOTICE('sample_lp_rewards: best YES bid price=' || $row.price::TEXT);
    }

    if NOT $has_bid {
        NOTICE('sample_lp_rewards: RETURN no YES buy orders');
        RETURN;  -- No YES buy orders = no bid side
    }

    -- Step B: Lowest YES sell → midpoint = (sell_price + ABS(buy_price)) / 2
    -- Integer division in Kuneiform truncates (= FLOOR for positive results)
    $has_ask BOOL := FALSE;
    for $row in SELECT price FROM ob_positions WHERE query_id = $query_id AND outcome = TRUE AND price > 0 ORDER BY price ASC LIMIT 1 {
        NOTICE('sample_lp_rewards: lowest YES sell price=' || $row.price::TEXT || ' x_mid_before=' || $x_mid::TEXT);
        -- $x_mid is negative (YES buy), negate to get ABS
        $x_mid := ($row.price + (0 - $x_mid)) / 2;
        $has_ask := TRUE;
    }

    if NOT $has_ask {
        NOTICE('sample_lp_rewards: RETURN no YES sell orders');
        RETURN;  -- No YES sell orders = no two-sided liquidity (spec requirement)
    }

    NOTICE('sample_lp_rewards: x_mid=' || $x_mid::TEXT);

    -- =========================================================================
    -- Dynamic Spread
    -- =========================================================================
    $x_spread_base INT := ABS($x_mid - (100 - $x_mid));
    $x_spread INT;
    if $x_spread_base < 30 {
        $x_spread := 5;
    } elseif $x_spread_base < 60 {
        $x_spread := 4;
    } elseif $x_spread_base < 80 {
        $x_spread := 3;
    } else {
        NOTICE('sample_lp_rewards: RETURN market too certain spread_base=' || $x_spread_base::TEXT);
        RETURN;  -- Market too certain, ineligible for rewards
    }

    NOTICE('sample_lp_rewards: spread_base=' || $x_spread_base::TEXT || ' spread=' || $x_spread::TEXT);

    -- =========================================================================
    -- Step 1: Calculate Global Total Score using LEAST(TRUE-side, FALSE-side)
    --
    -- Join condition: p1.price = 100 + p2.price (spec-aligned, directional)
    -- This means p1 is always the higher-price side (positive), p2 is always
    -- the lower-price side (negative). Holdings (price=0) are naturally excluded.
    --
    -- Scoring: LEAST of two side sums ensures balanced two-sided liquidity.
    -- Per participant: LEAST(SUM(TRUE-side scores), SUM(FALSE-side scores))
    -- Global total: SUM of per-participant LEAST scores
    --
    -- We must GROUP BY participant_id and use LEAST(SUM(...), SUM(...))
    -- because each row has p1.outcome as either TRUE or FALSE (never both),
    -- so per-row LEAST would always be 0.
    -- =========================================================================
    $global_total_score NUMERIC(78, 20) := 0::NUMERIC(78, 20);
    for $totals in
        SELECT
            p1.participant_id as pid,
            LEAST(
                SUM(
                    CASE WHEN p1.outcome = TRUE THEN
                        LEAST(p1.amount, p2.amount)::NUMERIC(78, 20) *
                        (($x_spread - LEAST(
                            ABS($x_mid - ABS(p1.price)),
                            ABS(100 - $x_mid - ABS(p2.price))
                        ))::NUMERIC(78, 20) * ($x_spread - LEAST(
                            ABS($x_mid - ABS(p1.price)),
                            ABS(100 - $x_mid - ABS(p2.price))
                        ))::NUMERIC(78, 20))::NUMERIC(78, 20) /
                        ($x_spread * $x_spread)::NUMERIC(78, 20)
                    ELSE 0::NUMERIC(78, 20) END
                ),
                SUM(
                    CASE WHEN p1.outcome = FALSE THEN
                        LEAST(p1.amount, p2.amount)::NUMERIC(78, 20) *
                        (($x_spread - LEAST(
                            ABS($x_mid - ABS(p2.price)),
                            ABS(100 - $x_mid - ABS(p1.price))
                        ))::NUMERIC(78, 20) * ($x_spread - LEAST(
                            ABS($x_mid - ABS(p2.price)),
                            ABS(100 - $x_mid - ABS(p1.price))
                        ))::NUMERIC(78, 20))::NUMERIC(78, 20) /
                        ($x_spread * $x_spread)::NUMERIC(78, 20)
                    ELSE 0::NUMERIC(78, 20) END
                )
            )::NUMERIC(78, 20) as participant_score
        FROM ob_positions p1
        JOIN ob_positions p2
            ON p1.query_id = p2.query_id
           AND p1.participant_id = p2.participant_id
           AND p1.outcome != p2.outcome
           AND p1.amount = p2.amount
           AND p1.price = 100 + p2.price
        WHERE p1.query_id = $query_id
          AND (CASE
              WHEN p1.outcome = TRUE THEN
                  ABS(p1.price) > $x_mid - $x_spread AND
                  ABS(p1.price) < $x_mid + $x_spread AND
                  ABS(p2.price) > 100 - $x_mid - $x_spread AND
                  ABS(p2.price) < 100 - $x_mid + $x_spread
              ELSE
                  ABS(p2.price) > $x_mid - $x_spread AND
                  ABS(p2.price) < $x_mid + $x_spread AND
                  ABS(p1.price) > 100 - $x_mid - $x_spread AND
                  ABS(p1.price) < 100 - $x_mid + $x_spread
          END)
        GROUP BY p1.participant_id
    {
        $ps NUMERIC(78, 20) := $totals.participant_score;
        if $ps IS NOT NULL {
            $global_total_score := $global_total_score + $ps;
        }
    }

    NOTICE('sample_lp_rewards: global_total_score=' || $global_total_score::TEXT);

    if $global_total_score <= 0::NUMERIC(78, 20) {
        NOTICE('sample_lp_rewards: RETURN global_total_score <= 0');
        RETURN;
    }

    -- =========================================================================
    -- Step 2: Calculate per-participant scores and insert into ob_rewards
    -- Uses LEAST(TRUE-side, FALSE-side) per participant for balanced scoring
    -- =========================================================================
    for $row in
        SELECT
            p1.participant_id,
            LEAST(
                SUM(
                    CASE WHEN p1.outcome = TRUE THEN
                        LEAST(p1.amount, p2.amount)::NUMERIC(78, 20) *
                        (($x_spread - LEAST(
                            ABS($x_mid - ABS(p1.price)),
                            ABS(100 - $x_mid - ABS(p2.price))
                        ))::NUMERIC(78, 20) * ($x_spread - LEAST(
                            ABS($x_mid - ABS(p1.price)),
                            ABS(100 - $x_mid - ABS(p2.price))
                        ))::NUMERIC(78, 20))::NUMERIC(78, 20) /
                        ($x_spread * $x_spread)::NUMERIC(78, 20)
                    ELSE 0::NUMERIC(78, 20) END
                ),
                SUM(
                    CASE WHEN p1.outcome = FALSE THEN
                        LEAST(p1.amount, p2.amount)::NUMERIC(78, 20) *
                        (($x_spread - LEAST(
                            ABS($x_mid - ABS(p2.price)),
                            ABS(100 - $x_mid - ABS(p1.price))
                        ))::NUMERIC(78, 20) * ($x_spread - LEAST(
                            ABS($x_mid - ABS(p2.price)),
                            ABS(100 - $x_mid - ABS(p1.price))
                        ))::NUMERIC(78, 20))::NUMERIC(78, 20) /
                        ($x_spread * $x_spread)::NUMERIC(78, 20)
                    ELSE 0::NUMERIC(78, 20) END
                )
            )::NUMERIC(78, 20) as participant_score
        FROM ob_positions p1
        JOIN ob_positions p2
            ON p1.query_id = p2.query_id
           AND p1.participant_id = p2.participant_id
           AND p1.outcome != p2.outcome
           AND p1.amount = p2.amount
           AND p1.price = 100 + p2.price
        WHERE p1.query_id = $query_id
          AND (CASE
              WHEN p1.outcome = TRUE THEN
                  ABS(p1.price) > $x_mid - $x_spread AND
                  ABS(p1.price) < $x_mid + $x_spread AND
                  ABS(p2.price) > 100 - $x_mid - $x_spread AND
                  ABS(p2.price) < 100 - $x_mid + $x_spread
              ELSE
                  ABS(p2.price) > $x_mid - $x_spread AND
                  ABS(p2.price) < $x_mid + $x_spread AND
                  ABS(p1.price) > 100 - $x_mid - $x_spread AND
                  ABS(p1.price) < 100 - $x_mid + $x_spread
          END)
        GROUP BY p1.participant_id
    {
        $pid INT := $row.participant_id;
        $score NUMERIC(78, 20) := $row.participant_score;
        $pct NUMERIC(5,2) := (($score / $global_total_score) * 100.0::NUMERIC(78, 20))::NUMERIC(5,2);

        INSERT INTO ob_rewards (query_id, participant_id, block, reward_percent)
        VALUES ($query_id, $pid, $block, $pct);
    }
};

-- =============================================================================
-- sample_all_active_lp_rewards: Batch sampling for all active markets
-- =============================================================================
/**
 * Iterates through all active markets and performs reward sampling.
 * This consolidated call is more efficient than calling sample_lp_rewards 
 * multiple times from an external process.
 *
 * Parameters:
 * - $block: The block height to record for all samples
 * - $market_limit: Maximum number of markets to process in this run
 */
CREATE OR REPLACE ACTION sample_all_active_lp_rewards(
    $block INT8,
    $market_limit INT
) PRIVATE {
    if $block IS NULL OR $block <= 0 {
        ERROR('block height is required and must be positive');
    }

    if $market_limit IS NULL OR $market_limit <= 0 {
        ERROR('market_limit is required and must be positive');
    }

    for $market in SELECT id FROM ob_queries 
        WHERE settled = false 
        ORDER BY id ASC 
        LIMIT $market_limit 
    {
        -- Call the helper action for each market
        sample_lp_rewards($market.id, $block);
    }
};
