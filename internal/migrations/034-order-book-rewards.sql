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
 * The minimum distance approach rewards balanced liquidity provision.
 * For paired positions with equal amounts, both sides contribute equally
 * since they share the same minimum distance from midpoint.
 *
 * Parameters:
 * - $query_id: Market to sample (must not be settled)
 * - $block: Block height for this sample (used as key in ob_rewards table)
 *
 * Behavior:
 * - Inserts rows into ob_rewards table with reward percentages
 * - Returns nothing on success
 * - Errors if market is already settled
 * - Returns empty if no qualifying LPs (no orders, too wide spread, or ineligible midpoint)
 *
 * Example Usage:
 *   -- External system samples every 50 blocks
 *   sample_lp_rewards(query_id := 1, block := 1000);
 *   sample_lp_rewards(query_id := 1, block := 1050);
 *   sample_lp_rewards(query_id := 1, block := 1100);
 *
 *   -- At settlement, distribute_fees() reads these 3 samples
 *   -- If total_fees = 1000 TRUF:
 *   --   reward_per_block = 1000 / 3 = 333.33 TRUF
 *   --   Each LP gets: 333.33 * (avg of their reward_percents)
 */
CREATE OR REPLACE ACTION sample_lp_rewards(
    $query_id INT,
    $block INT8
) PRIVATE {
    -- Check if this block was already sampled to prevent duplicate key errors
    for $row in SELECT 1 FROM ob_rewards WHERE query_id = $query_id AND block = $block LIMIT 1 {
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

    -- Calculate midpoint
    $best_bid INT;
    $best_ask INT;

    for $row in SELECT price FROM ob_positions
        WHERE query_id = $query_id AND outcome = TRUE AND price < 0
        ORDER BY price DESC LIMIT 1
    {
        $best_bid := $row.price;
    }

    for $row in SELECT price FROM ob_positions
        WHERE query_id = $query_id AND outcome = TRUE AND price > 0
        ORDER BY price ASC LIMIT 1
    {
        $best_ask := $row.price;
    }

    -- If no two-sided liquidity, no rewards
    if $best_bid IS NULL OR $best_ask IS NULL {
        RETURN;
    }

    -- Midpoint is (BestAsk + BestBidMagnitude) / 2
    $x_mid INT := ($best_ask + ABS($best_bid)) / 2;

    -- Dynamic spread
    $x_spread_base INT := ABS($x_mid - (100 - $x_mid));
    $x_spread INT;
    if $x_spread_base < 30 {
        $x_spread := 5;
    } elseif $x_spread_base < 60 {
        $x_spread := 4;
    } elseif $x_spread_base < 80 {
        $x_spread := 3;
    } else {
        RETURN;
    }

    -- Step 1: Calculate Global Total Score
    -- We join positions to find pairs (Outcome1, Outcome2) with same amount
    -- Qualification: EffectivePrice1 + EffectivePrice2 = 100
    $global_total_score NUMERIC(78, 20) := 0::NUMERIC(78, 20);
    for $row in 
        SELECT SUM(
            p1.amount::NUMERIC(78, 20) *
            (($x_spread - ABS($x_mid - (CASE WHEN p1.outcome = TRUE THEN (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END) ELSE (100 - (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END)) END)))::NUMERIC(78, 20) * ($x_spread - ABS($x_mid - (CASE WHEN p1.outcome = TRUE THEN (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END) ELSE (100 - (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END)) END)))::NUMERIC(78, 20))::NUMERIC(78, 20) /
            ($x_spread * $x_spread)::NUMERIC(78, 20)
        ) as total
        FROM ob_positions p1
        JOIN ob_positions p2
            ON p1.query_id = p2.query_id
           AND p1.participant_id = p2.participant_id
           AND p1.outcome != p2.outcome
           AND p1.amount = p2.amount
        WHERE p1.query_id = $query_id
          AND (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END + CASE WHEN p2.price = 0 THEN 100 - ABS(p1.price) ELSE ABS(p2.price) END) = 100
          AND (p1.price != 0 OR p2.price != 0)
          AND ABS($x_mid - (CASE WHEN p1.outcome = TRUE THEN (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END) ELSE (100 - (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END)) END)) < $x_spread
    {
        if $row.total IS NOT NULL {
            $global_total_score := $row.total::NUMERIC(78, 20);
        }
    }

    if $global_total_score <= 0::NUMERIC(78, 20) {
        RETURN;
    }

    -- Step 2: Calculate and Insert Normalized Participant Scores
    -- One insert per participant per block to avoid PK violation
    for $row in 
        SELECT 
            p1.participant_id,
            SUM(
                p1.amount::NUMERIC(78, 20) *
                (($x_spread - ABS($x_mid - (CASE WHEN p1.outcome = TRUE THEN (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END) ELSE (100 - (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END)) END)))::NUMERIC(78, 20) * ($x_spread - ABS($x_mid - (CASE WHEN p1.outcome = TRUE THEN (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END) ELSE (100 - (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END)) END)))::NUMERIC(78, 20))::NUMERIC(78, 20) /
                ($x_spread * $x_spread)::NUMERIC(78, 20)
            ) as participant_score
        FROM ob_positions p1
        JOIN ob_positions p2
            ON p1.query_id = p2.query_id
           AND p1.participant_id = p2.participant_id
           AND p1.outcome != p2.outcome
           AND p1.amount = p2.amount
        WHERE p1.query_id = $query_id
          AND (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END + CASE WHEN p2.price = 0 THEN 100 - ABS(p1.price) ELSE ABS(p2.price) END) = 100
          AND (p1.price != 0 OR p2.price != 0)
          AND ABS($x_mid - (CASE WHEN p1.outcome = TRUE THEN (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END) ELSE (100 - (CASE WHEN p1.price = 0 THEN 100 - ABS(p2.price) ELSE ABS(p1.price) END)) END)) < $x_spread
        GROUP BY p1.participant_id
    {
        $pid INT := $row.participant_id;
        $score NUMERIC(78, 20) := $row.participant_score::NUMERIC(78, 20);
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

