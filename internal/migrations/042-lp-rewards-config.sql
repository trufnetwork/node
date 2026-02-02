/**
 * MIGRATION 042: LP REWARDS SCHEDULER CONFIGURATION
 *
 * Purpose:
 * Configuration table for the tn_lp_rewards extension that samples
 * liquidity provider rewards at block intervals.
 *
 * The scheduler calls sample_lp_rewards($query_id, $block) periodically
 * for all active (unsettled) markets to track LP contributions over time.
 *
 * Dependencies:
 * - Migration 034: sample_lp_rewards action and ob_rewards table
 * - Migration 030: ob_queries table (active markets)
 */

-- ============================================================================
-- CONFIGURATION TABLE
-- ============================================================================

/**
 * lp_rewards_config
 *
 * Single-row configuration for LP rewards sampling scheduler.
 * Similar to settlement_config but uses block intervals instead of cron.
 *
 * Columns:
 * - id: Always 1 (single row)
 * - enabled: Whether sampling is active
 * - sampling_interval_blocks: Sample every N blocks (e.g., 10 = every 10 blocks)
 * - max_markets_per_run: Limit markets sampled per block to prevent overload
 */
CREATE TABLE IF NOT EXISTS lp_rewards_config (
    id INT PRIMARY KEY DEFAULT 1,
    enabled BOOL NOT NULL DEFAULT TRUE,
    sampling_interval_blocks INT NOT NULL DEFAULT 10,
    max_markets_per_run INT NOT NULL DEFAULT 50,
    CHECK (id = 1),
    CHECK (sampling_interval_blocks >= 1),
    CHECK (max_markets_per_run >= 1)
);

-- Insert default configuration
INSERT INTO lp_rewards_config (id, enabled, sampling_interval_blocks, max_markets_per_run)
VALUES (1, TRUE, 10, 50)
ON CONFLICT (id) DO NOTHING;

-- ============================================================================
-- CONFIGURATION ACTIONS
-- ============================================================================

/**
 * get_lp_rewards_config
 *
 * Returns the current LP rewards scheduler configuration.
 * Used by the tn_lp_rewards extension to load settings.
 */
CREATE OR REPLACE ACTION get_lp_rewards_config()
PUBLIC VIEW RETURNS (
    enabled BOOL,
    sampling_interval_blocks INT,
    max_markets_per_run INT
) {
    for $row in SELECT enabled, sampling_interval_blocks, max_markets_per_run
                FROM lp_rewards_config WHERE id = 1
    {
        RETURN $row.enabled, $row.sampling_interval_blocks, $row.max_markets_per_run;
    }
    -- Default if no row exists
    RETURN TRUE, 10, 50;
};

/**
 * update_lp_rewards_config
 *
 * Updates the LP rewards scheduler configuration.
 * Only callable by network_writer role.
 *
 * Parameters:
 * - $enabled: Enable/disable sampling
 * - $sampling_interval_blocks: Sample every N blocks
 * - $max_markets_per_run: Max markets to sample per run
 */
CREATE OR REPLACE ACTION update_lp_rewards_config(
    $enabled BOOL,
    $sampling_interval_blocks INT,
    $max_markets_per_run INT
) PUBLIC {
    -- Check caller has network_writer role (query role_members table)
    $has_role BOOL := FALSE;

    for $row in SELECT 1 FROM role_members
        WHERE owner = 'system'
          AND role_name = 'network_writer'
          AND wallet = LOWER(@caller)
        LIMIT 1
    {
        $has_role := TRUE;
    }

    if $has_role = FALSE {
        ERROR('Only network_writer can update LP rewards config');
    }

    -- Validate inputs
    if $enabled IS NULL {
        ERROR('enabled cannot be NULL');
    }
    if $sampling_interval_blocks IS NULL OR $sampling_interval_blocks < 1 {
        ERROR('sampling_interval_blocks must be >= 1');
    }
    if $max_markets_per_run IS NULL OR $max_markets_per_run < 1 OR $max_markets_per_run > 1000 {
        ERROR('max_markets_per_run must be between 1 and 1000');
    }

    -- Update or insert config
    INSERT INTO lp_rewards_config (id, enabled, sampling_interval_blocks, max_markets_per_run)
    VALUES (1, $enabled, $sampling_interval_blocks, $max_markets_per_run)
    ON CONFLICT (id) DO UPDATE
    SET enabled = $enabled,
        sampling_interval_blocks = $sampling_interval_blocks,
        max_markets_per_run = $max_markets_per_run;
};

/**
 * get_active_markets_for_sampling
 *
 * Returns active (unsettled) markets that can be sampled for LP rewards.
 * Used by the tn_lp_rewards extension.
 *
 * Parameters:
 * - $limit: Maximum number of markets to return
 */
CREATE OR REPLACE ACTION get_active_markets_for_sampling($limit INT)
PUBLIC VIEW RETURNS (query_id INT) {
    -- Validate limit parameter
    if $limit IS NULL OR $limit < 1 {
        ERROR('limit must be >= 1');
    }

    for $row in SELECT id FROM ob_queries
        WHERE settled = FALSE
        ORDER BY id ASC
        LIMIT $limit
    {
        RETURN NEXT $row.id;
    }
};
