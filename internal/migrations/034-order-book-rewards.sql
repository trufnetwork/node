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
 * - External system calls sample_lp_rewards($query_id, $block) periodically
 * - Recommended: Every 50 blocks (~10 minutes with 12s block times)
 * - Can be called by anyone (PUBLIC action)
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
) PUBLIC {
    -- Check if this block was already sampled to prevent duplicate key errors
    -- This handles retries, race conditions, and scheduler overlap
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

    -- Calculate midpoint (reference line 34-46)
    $best_bid INT;
    $best_ask INT;

    for $row in SELECT price FROM ob_positions
        WHERE query_id = $query_id AND outcome = TRUE AND price < 0
        ORDER BY price ASC LIMIT 1
    {
        $best_bid := $row.price;
    }

    for $row in SELECT price FROM ob_positions
        WHERE query_id = $query_id AND outcome = TRUE AND price > 0
        ORDER BY price ASC LIMIT 1
    {
        $best_ask := $row.price;
    }

    if $best_bid IS NULL OR $best_ask IS NULL {
        RETURN;
    }

    $x_mid INT := ($best_ask + (-$best_bid)) / 2;

    -- Dynamic spread (reference line 48-63)
    $x_spread_base INT := $x_mid - (100 - $x_mid);
    if $x_spread_base < 0 {
        $x_spread_base := -$x_spread_base;
    }

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

    -- Calculate scores (reference line 65-147)
    for $pos in
        SELECT
            p1.participant_id,
            p1.price as yes_price,
            p1.amount,
            p2.price as no_price
        FROM ob_positions p1
        JOIN ob_positions p2
            ON p1.query_id = p2.query_id
           AND p1.participant_id = p2.participant_id
           AND p1.outcome != p2.outcome
           AND p1.amount = p2.amount
        WHERE p1.query_id = $query_id
    {
        $pid INT := $pos.participant_id;
        $yes_price INT := $pos.yes_price;
        $no_price INT := $pos.no_price;
        $amount INT := $pos.amount;

        -- Price range is 0-100 cents (100 represents $1.00)
        -- Constraint: yes_price = 100 + no_price
        -- This ensures we only process (SELL, BUY) pairs, not (BUY, SELL) duplicates
        -- Example: SELL YES @ 48 + BUY NO @ 52 → 48 = 100 + (-52) ✅
        if $yes_price == 100 + $no_price {
            -- Calculate distances from midpoint using ABS()
            -- Handles both positive (SELL) and negative (BUY) prices
            $yes_dist INT := $x_mid - $yes_price;
            if $yes_dist < 0 {
                $yes_dist := -$yes_dist;
            }

            $no_dist INT := (100 - $x_mid) - (-$no_price);
            if $no_dist < 0 {
                $no_dist := -$no_dist;
            }

            -- Filter by spread (reference line 135-146)
            if $yes_dist < $x_spread AND $no_dist < $x_spread {
            -- Get minimum distance (reference uses LEAST())
            $min_dist INT := $yes_dist;
            if $no_dist < $yes_dist {
                $min_dist := $no_dist;
            }

            -- Calculate score (reference line 74: amount * POWER((spread - dist) / spread, 2))
            $spread_minus_dist INT := $x_spread - $min_dist;
            $score NUMERIC(20,4) := (
                $amount::NUMERIC(20,4) *
                ($spread_minus_dist * $spread_minus_dist)::NUMERIC(20,4) /
                ($x_spread * $x_spread)::NUMERIC(20,4)
            );

            INSERT INTO ob_rewards (query_id, participant_id, block, reward_percent)
            VALUES ($query_id, $pid, $block, $score::NUMERIC(5,2));
            }
        }
    }

    -- Normalize to percentages (reference final SELECT)
    $total NUMERIC(20,4) := 0::NUMERIC(20,4);
    for $row in SELECT SUM(reward_percent) as total FROM ob_rewards
        WHERE query_id = $query_id AND block = $block
    {
        $total := $row.total::NUMERIC(20,4);
    }

    if $total > 0::NUMERIC(20,4) {
        for $row in SELECT participant_id, reward_percent FROM ob_rewards
            WHERE query_id = $query_id AND block = $block
        {
            $pid INT := $row.participant_id;
            $raw NUMERIC(20,4) := $row.reward_percent::NUMERIC(20,4);
            $pct NUMERIC(5,2) := (($raw / $total)::NUMERIC(20,4) * 100.0)::NUMERIC(5,2);

            DELETE FROM ob_rewards
            WHERE query_id = $query_id AND participant_id = $pid AND block = $block;

            INSERT INTO ob_rewards (query_id, participant_id, block, reward_percent)
            VALUES ($query_id, $pid, $block, $pct);
        }
    }
};
