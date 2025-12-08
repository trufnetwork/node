/**
 * MIGRATION 034: DYNAMIC LP REWARDS SAMPLING (ISSUE 9 REFACTOR)
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
 * - TRUF Spec: /PredictionMarketTasks/OrderbookSetupGoal/orderBookLiquidityRewardsProgram/0mainDoc.md
 * - Reference SQL: /PredictionMarketTasks/OrderbookSetupGoal/orderBookLiquidityRewardsProgram/2DynamicRewardsCalculation.md
 *
 * Related Issues:
 * - Task 9R1: Schema Migration + Cleanup (THIS MIGRATION)
 * - Task 9R2: Rewards Sampling Implementation (sample_lp_rewards logic)
 * - Task 9R3: Settlement Fee Distribution (uses this table)
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
 * - For each split limit order (paired TRUE/FALSE positions):
 *   - Calculate per-side score: amount * ((spread - distance) / spread)²
 *   - Market score = LEAST(true_score, false_score)
 * - Reward percent = (market_score / total_score) * 100
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
 *
 * TODO (Task 9R2):
 * - Implement midpoint calculation from order book
 * - Implement dynamic spread determination
 * - Implement Polymarket scoring formula
 * - Handle Kuneiform limitations (no POWER, LEAST, temp tables)
 * - Write 11 comprehensive tests
 */
CREATE OR REPLACE ACTION sample_lp_rewards(
    $query_id INT,
    $block INT8
) PUBLIC {
    -- STUB: Implementation in Task 9R2
    -- For now, this action does nothing (no rewards generated)
    RETURN;
};
