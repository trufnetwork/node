/**
 * MIGRATION 036: FEE DISTRIBUTION AUDIT TRAIL (ISSUE 9C PHASE 1)
 *
 * Purpose:
 * Create immutable audit tables to track fee distribution history.
 * Ensures compliance, user transparency, and data preservation.
 *
 * Problem:
 * Currently, distribute_fees() deletes ob_rewards after distribution,
 * leaving NO record of who received how much. This migration creates
 * permanent audit tables that capture distribution history before deletion.
 *
 * Tables:
 * - ob_fee_distributions: Summary of each distribution event
 * - ob_fee_distribution_details: Per-participant reward details
 *
 * Related Issues:
 * - Issue 9C Phase 1: Audit Trail (THIS MIGRATION)
 * - Issue 9B: Fee Distribution Engine (modified to insert audit records)
 * - Issue 9R3: Settlement Fee Distribution (uses zero-loss algorithm)
 *
 * Dependencies:
 * - Migration 030: ob_queries, ob_participants tables
 * - Migration 033: distribute_fees() action (will be modified)
 * - Migration 034: ob_rewards table
 */

-- ============================================================================
-- SCHEMA: Fee Distribution Audit Tables
-- ============================================================================

/**
 * ob_fee_distributions
 *
 * Stores one record per market settlement fee distribution.
 * Acts as the parent table for ob_fee_distribution_details.
 *
 * ID Generation: MAX(id) + 1 pattern (safe in Kwil sequential execution)
 *
 * Lifecycle:
 * - Created by distribute_fees() after successful distribution
 * - CASCADE deleted when market (ob_queries) is deleted
 * - Never updated (immutable audit log)
 *
 * Example row:
 * id=1, query_id=5, total_fees=10000000000000000000 (10 TRUF),
 * total_lp_count=2, block_count=1, distributed_at=1733850000
 */
CREATE TABLE IF NOT EXISTS ob_fee_distributions (
    id INT PRIMARY KEY,
    query_id INT NOT NULL,
    total_fees_distributed NUMERIC(78, 0) NOT NULL,
    total_lp_count INT NOT NULL,
    block_count INT NOT NULL,
    distributed_at INT8 NOT NULL,
    FOREIGN KEY (query_id) REFERENCES ob_queries(id) ON DELETE CASCADE,
    CHECK (total_fees_distributed >= 0),
    CHECK (total_lp_count > 0),
    CHECK (block_count > 0)
);

/**
 * ob_fee_distribution_details
 *
 * Stores individual LP rewards for each distribution.
 * Multiple rows per distribution (one per LP who received fees).
 *
 * Composite PK: (distribution_id, participant_id)
 * - Prevents duplicate LP records in same distribution
 * - Efficient joins on distribution_id
 *
 * Fields:
 * - wallet_address: Snapshot at distribution time (de-normalized for queries)
 * - reward_amount: Actual wei received (includes dust if first LP)
 * - total_reward_percent: SUM(reward_percent) across sampled blocks
 *
 * Example rows for distribution_id=1:
 * | distribution_id | participant_id | wallet_address | reward_amount        | total_reward_percent |
 * |-----------------|----------------|----------------|----------------------|----------------------|
 * | 1               | 10             | 0x1111...      | 6400000000000000000  | 64.00                |
 * | 1               | 20             | 0x2222...      | 3600000000000000000  | 36.00                |
 */
CREATE TABLE IF NOT EXISTS ob_fee_distribution_details (
    distribution_id INT NOT NULL,
    participant_id INT NOT NULL,
    wallet_address BYTEA NOT NULL,
    reward_amount NUMERIC(78, 0) NOT NULL,
    total_reward_percent NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (distribution_id, participant_id),
    FOREIGN KEY (distribution_id) REFERENCES ob_fee_distributions(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES ob_participants(id),
    CHECK (reward_amount > 0),
    CHECK (total_reward_percent > 0)
);

-- ============================================================================
-- Indexes for Query Performance
-- ============================================================================

/**
 * Query pattern: "Show all distributions for Market #123"
 * Used by: Analytics queries, market detail pages
 */
CREATE INDEX IF NOT EXISTS idx_fee_dist_query
    ON ob_fee_distributions(query_id);

/**
 * Query pattern: "Show all rewards earned by Participant #456"
 * Used by: User reward history, portfolio analytics
 */
CREATE INDEX IF NOT EXISTS idx_fee_dist_det_participant
    ON ob_fee_distribution_details(participant_id);

/**
 * Query pattern: "Show recent distributions (last 7 days)"
 * Used by: Analytics dashboards, monitoring
 */
CREATE INDEX IF NOT EXISTS idx_fee_dist_time
    ON ob_fee_distributions(distributed_at);

/**
 * Composite index for efficient wallet-based queries
 * Query pattern: "Find distributions for wallet 0xABCD..."
 */
CREATE INDEX IF NOT EXISTS idx_fee_dist_det_wallet
    ON ob_fee_distribution_details(wallet_address);

-- ============================================================================
-- ACTIONS: Audit Query Helpers
-- ============================================================================

/**
 * get_distribution_summary($query_id)
 *
 * Returns fee distribution summary for a specific market.
 *
 * Parameters:
 * - $query_id: Market ID
 *
 * Returns:
 * - distribution_id: Unique distribution identifier
 * - total_fees_distributed: Total fees distributed (wei)
 * - total_lp_count: Number of LPs who received fees
 * - block_count: Number of blocks sampled
 * - distributed_at: Unix timestamp of distribution
 *
 * Example usage:
 *   SELECT * FROM get_distribution_summary(1);
 */
CREATE OR REPLACE ACTION get_distribution_summary(
    $query_id INT
) PUBLIC VIEW RETURNS TABLE(
    distribution_id INT,
    total_fees_distributed NUMERIC(78, 0),
    total_lp_count INT,
    block_count INT,
    distributed_at INT8
) {
    for $row in
        SELECT
            id as distribution_id,
            total_fees_distributed,
            total_lp_count,
            block_count,
            distributed_at
        FROM ob_fee_distributions
        WHERE query_id = $query_id
        ORDER BY distributed_at DESC
    {
        RETURN NEXT $row.distribution_id, $row.total_fees_distributed, $row.total_lp_count, $row.block_count, $row.distributed_at;
    }
};

/**
 * get_distribution_details($distribution_id)
 *
 * Returns per-LP reward details for a specific distribution.
 *
 * Parameters:
 * - $distribution_id: Distribution identifier
 *
 * Returns:
 * - participant_id: Internal participant ID
 * - wallet_address: Hex wallet address (0x prefix)
 * - reward_amount: Amount received in wei
 * - total_reward_percent: Sum of reward % across blocks
 *
 * Example usage:
 *   SELECT * FROM get_distribution_details(1);
 */
CREATE OR REPLACE ACTION get_distribution_details(
    $distribution_id INT
) PUBLIC VIEW RETURNS TABLE(
    participant_id INT,
    wallet_address TEXT,
    reward_amount NUMERIC(78, 0),
    total_reward_percent NUMERIC(10, 2)
) {
    for $row in
        SELECT
            participant_id,
            '0x' || encode(wallet_address, 'hex') as wallet_hex,
            reward_amount,
            total_reward_percent
        FROM ob_fee_distribution_details
        WHERE distribution_id = $distribution_id
        ORDER BY reward_amount DESC
    {
        RETURN NEXT $row.participant_id, $row.wallet_hex, $row.reward_amount, $row.total_reward_percent;
    }
};

/**
 * get_participant_reward_history($wallet_hex)
 *
 * Returns fee distributions for a wallet.
 * Useful for "Your LP Rewards" history pages.
 *
 * Parameters:
 * - $wallet_hex: Wallet address hex (with/out 0x)
 *
 * Returns:
 * - query_id: Market that distributed fees
 * - reward_amount: Amount received in wei
 * - total_reward_percent: Sum of reward %
 * - distributed_at: Unix timestamp
 * - distribution_id: Distribution reference
 *
 * Example usage:
 *   get_participant_reward_history('0x1111...');
 */
CREATE OR REPLACE ACTION get_participant_reward_history(
    $wallet_hex TEXT
) PUBLIC VIEW RETURNS TABLE(
    query_id INT,
    reward_amount NUMERIC(78, 0),
    total_reward_percent NUMERIC(10, 2),
    distributed_at INT8,
    distribution_id INT
) {
    -- Remove 0x prefix if present
    $clean_hex TEXT := $wallet_hex;
    if substring($wallet_hex, 1, 2) = '0x' {
        $clean_hex := substring($wallet_hex, 3);
    }

    for $row in
        SELECT
            d.query_id,
            dd.reward_amount,
            dd.total_reward_percent,
            d.distributed_at,
            dd.distribution_id
        FROM ob_fee_distribution_details dd
        JOIN ob_fee_distributions d ON dd.distribution_id = d.id
        WHERE LOWER(encode(dd.wallet_address, 'hex')) = LOWER($clean_hex)
        ORDER BY d.distributed_at DESC
    {
        RETURN NEXT $row.query_id, $row.reward_amount, $row.total_reward_percent, $row.distributed_at, $row.distribution_id;
    }
};
