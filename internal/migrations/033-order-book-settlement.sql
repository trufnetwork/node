/**
 * MIGRATION 033: ORDER BOOK SETTLEMENT
 *
 * Automatic atomic settlement processing:
 * - Bulk delete losing positions (efficient)
 * - Pay winners (shares × $1.00 - 2% redemption fee)
 * - Refund open buy orders (no fee)
 * - Delete all positions atomically
 * - Collect fees in vault (Issue 9 will distribute them)
 *
 * Implementation Note:
 * Uses CTE + ARRAY_AGG to collect all payout data in a single query, then
 * processes payouts via batch unlock. This avoids nested queries in the main
 * settlement action (Kuneiform limitation: cannot call external functions
 * like ethereum_bridge.unlock() inside FOR loops in the same action).
 *
 * The batch unlock helper (ob_batch_unlock_collateral) CAN loop with function
 * calls because it's a separate action called ONCE with all aggregated data.
 *
 * Transaction Atomicity:
 * All Kwil actions execute in a single database transaction. If ANY operation
 * fails (including ethereum_bridge.unlock()), the ENTIRE action rolls back:
 * - Database changes (position deletions, settled flag) are reverted
 * - Blockchain state changes are NOT committed (Kwil's 2-phase approach)
 * - The settled flag remains false, allowing the settlement extension to retry
 *
 * Retry Mechanism:
 * The tn_settlement extension retries failed settlements (3 attempts with backoff).
 * After exhaustion, the market remains unsettled and requires manual intervention
 * or extension restart to resume retries. This is safe because:
 * 1. The settled flag prevents duplicate settlement attempts within a transaction
 * 2. Rollback ensures partial state never persists
 * 3. Position data remains intact for retry attempts
 */

-- Batch unlock collateral for multiple wallets
-- This helper processes all unlocks in a single call, avoiding nested queries in settlement
CREATE OR REPLACE ACTION ob_batch_unlock_collateral(
    $wallet_addresses TEXT[],
    $amounts NUMERIC(78, 0)[]
) PRIVATE {
    -- Validate input arrays have same length
    if COALESCE(array_length($wallet_addresses), 0) != COALESCE(array_length($amounts), 0) {
        ERROR('wallet_addresses and amounts arrays must have the same length');
    }

    -- Process each unlock (this is the ONLY place we loop with function calls)
    -- This is safe because the settlement action calls THIS function once with all data
    for $payout in
        SELECT wallet, amount
        FROM UNNEST($wallet_addresses, $amounts) AS u(wallet, amount)
    {
        ethereum_bridge.unlock($payout.wallet, $payout.amount);
    }
};

-- ============================================================================
-- Fee Distribution to Liquidity Providers (Issue 9B)
-- ============================================================================

/**
 * distribute_fees($query_id, $total_fees)
 *
 * Distributes settlement fees to liquidity providers based on sampled rewards.
 * Called automatically after winner payouts in process_settlement().
 *
 * DYNAMIC REWARDS MODEL (Issue 9 Refactor):
 * Uses the ob_rewards table populated by periodic sample_lp_rewards() calls.
 * Fees are distributed proportionally across all sampled blocks.
 *
 * Zero-Loss Distribution Algorithm:
 * 1. total_percent = SUM(reward_percent) across all blocks for each participant
 * 2. base_reward = (total_fees * total_percent) / (100 * block_count)
 * 3. dust = total_fees - SUM(base_reward)
 * 4. first_participant gets base_reward + dust (ensures all fees distributed)
 *
 * This approach minimizes truncation (single point vs per-block) and ensures
 * zero fee loss by giving the remainder to the first participant.
 *
 * Parameters:
 * - $query_id: Settled market ID
 * - $total_fees: Total fees collected (2% of redemptions), in wei
 *
 * Behavior:
 * - No samples → fees remain in vault (safe accumulation)
 * - Distributes proportionally across sampled blocks with zero loss
 * - Deletes processed rewards from ob_rewards table
 *
 * Dependencies:
 * - ob_rewards table (created in migration 034-order-book-rewards.sql)
 * - ob_batch_unlock_collateral() helper (defined above in this migration)
 * - ethereum_bridge.unlock() (from Migration 031)
 */
CREATE OR REPLACE ACTION distribute_fees(
    $query_id INT,
    $total_fees NUMERIC(78, 0)
) PRIVATE {
    -- Early return if no fees to distribute
    if $total_fees = '0'::NUMERIC(78, 0) {
        RETURN;
    }

    -- Step 1: Count distinct blocks sampled for this market
    $block_count INT := 0;
    for $row in SELECT COUNT(DISTINCT block) as cnt FROM ob_rewards WHERE query_id = $query_id {
        $block_count := $row.cnt;
    }

    -- Edge case: No samples recorded → fees remain in vault (safe accumulation)
    if $block_count = 0 {
        RETURN;
    }

    -- Step 2-4: Calculate rewards with zero-loss distribution
    -- Improved algorithm: Calculate total percentage first, then distribute
    -- Remainder (dust) is given to the first participant to ensure all fees are distributed
    $wallet_addresses TEXT[];
    $amounts NUMERIC(78, 0)[];

    for $result in
    WITH participant_totals AS (
        -- Sum each participant's reward percentages across all sampled blocks
        -- Cast to INT to truncate decimal (64.00 → 64)
        SELECT
            r.participant_id,
            p.wallet_address,
            SUM(r.reward_percent)::INT as total_percent_int
        FROM ob_rewards r
        JOIN ob_participants p ON r.participant_id = p.id
        WHERE r.query_id = $query_id
        GROUP BY r.participant_id, p.wallet_address
    ),
    calculated_rewards AS (
        -- Calculate base reward using integer division: (total_fees * total_percent_int) / (100 * block_count)
        -- This truncates fractional rewards, creating "dust" that will be distributed to first LP
        SELECT
            participant_id,
            wallet_address,
            (($total_fees * total_percent_int::NUMERIC(78, 0)) / (100::NUMERIC(78, 0) * $block_count::NUMERIC(78, 0)))::NUMERIC(78, 0) as base_reward
        FROM participant_totals
    ),
    total_check AS (
        -- Calculate total distributed to find dust (remainder from integer division)
        SELECT COALESCE(SUM(base_reward)::NUMERIC(78, 0), '0'::NUMERIC(78, 0)) as total_distributed
        FROM calculated_rewards
    ),
    with_remainder AS (
        -- Distribute remainder to first participant (lowest participant_id)
        -- This ensures zero fee loss - all settlement fees go to LPs as intended
        SELECT
            participant_id,
            wallet_address,
            base_reward + CASE
                WHEN participant_id = (SELECT MIN(participant_id) FROM calculated_rewards)
                THEN $total_fees - (SELECT total_distributed FROM total_check)
                ELSE '0'::NUMERIC(78, 0)
            END as final_reward
        FROM calculated_rewards
    ),
    aggregated AS (
        -- Aggregate into arrays for batch processing (same pattern as process_settlement)
        SELECT
            ARRAY_AGG('0x' || encode(wallet_address, 'hex') ORDER BY participant_id) as wallets,
            ARRAY_AGG(final_reward ORDER BY participant_id) as amounts
        FROM with_remainder
    )
    SELECT wallets, amounts FROM aggregated
    {
        $wallet_addresses := $result.wallets;
        $amounts := $result.amounts;
    }

    -- Step 5: Batch unlock to all LPs (single call, no loops)
    if $wallet_addresses IS NOT NULL AND COALESCE(array_length($wallet_addresses), 0) > 0 {
        ob_batch_unlock_collateral($wallet_addresses, $amounts);
    }

    -- Step 6: Cleanup - delete processed rewards to save storage
    DELETE FROM ob_rewards WHERE query_id = $query_id;
};

-- Process settlement: Pay winners, refund open buys, collect fees
CREATE OR REPLACE ACTION process_settlement(
    $query_id INT,
    $winning_outcome BOOL
) PRIVATE {
    $redemption_fee_bps INT := 200;  -- 2% (200 basis points)
    $total_fees_collected NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $one_token NUMERIC(78, 0) := '1000000000000000000'::NUMERIC(78, 0);

    -- Step 1: Bulk delete all losing positions (efficient single operation)
    -- Price semantics: price=0 (holdings), price>0 (open sells), price<0 (open buys)
    -- Deletes losing outcome holdings and sells, which have zero value after settlement
    -- This removes ~50% of positions upfront
    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND outcome = NOT $winning_outcome
      AND price >= 0;  -- Holdings (price=0) and open sells (price>0) only

    -- Step 2: Collect ALL payout data using CTE + ARRAY_AGG (digest pattern!)
    -- Calculate payouts and aggregate into arrays in a SINGLE query
    $wallet_addresses TEXT[];
    $amounts NUMERIC(78, 0)[];

    for $result in
    WITH remaining_positions AS (
        SELECT
            p.participant_id,
            p.outcome,
            p.price,
            p.amount,
            '0x' || encode(part.wallet_address, 'hex') as wallet_address
        FROM ob_positions p
        JOIN ob_participants part ON p.participant_id = part.id
        WHERE p.query_id = $query_id
    ),
    calculated_values AS (
        SELECT
            wallet_address,
            price,
            -- Pre-calculate all monetary values to avoid CASE type issues
            -- All amounts cast to NUMERIC(78, 0) to match ethereum_bridge.unlock() API
            (amount::NUMERIC(78, 0) * $one_token)::NUMERIC(78, 0) as gross_winner_payout,
            ((amount::NUMERIC(78, 0) * $one_token * $redemption_fee_bps::NUMERIC(78, 0)) / 10000::NUMERIC(78, 0))::NUMERIC(78, 0) as winner_fee,
            ((amount::NUMERIC(78, 0) * abs(price)::NUMERIC(78, 0) * $one_token) / 100::NUMERIC(78, 0))::NUMERIC(78, 0) as refund_amount
        FROM remaining_positions
    ),
    payouts AS (
        SELECT
            wallet_address,
            -- Remaining positions after Step 1 are:
            -- 1. Winning holdings/sells (price >= 0): Pay shares × $1 - 2% fee
            -- 2. Open buy orders (price < 0): Refund locked collateral, no fee
            CASE
                WHEN price >= 0 THEN
                    gross_winner_payout - winner_fee
                ELSE
                    refund_amount
            END as payout_amount,
            CASE
                WHEN price >= 0 THEN
                    winner_fee
                ELSE
                    '0'::NUMERIC(78, 0)
            END as fee_amount
        FROM calculated_values
    ),
    wallet_totals AS (
        -- Group by wallet to handle multiple positions per user
        SELECT
            wallet_address,
            SUM(payout_amount) as total_payout,
            SUM(fee_amount) as total_fees
        FROM payouts
        GROUP BY wallet_address
    ),
    aggregated AS (
        SELECT
            ARRAY_AGG(wallet_address ORDER BY wallet_address) as wallets,
            ARRAY_AGG(total_payout::NUMERIC(78, 0) ORDER BY wallet_address) as amounts,
            SUM(total_fees)::NUMERIC(78, 0) as total_fees
        FROM wallet_totals
    )
    SELECT wallets, amounts, COALESCE(total_fees, 0::NUMERIC(78, 0)) as total_fees
    FROM aggregated
    {
        $wallet_addresses := $result.wallets;
        $amounts := $result.amounts;
        $total_fees_collected := $result.total_fees;
    }

    -- Step 3: Delete all processed positions (set-based, no loop!)
    DELETE FROM ob_positions WHERE query_id = $query_id;

    -- Step 4: Process ALL payouts in a SINGLE batch call (no nested queries!)
    if $wallet_addresses IS NOT NULL AND COALESCE(array_length($wallet_addresses), 0) > 0 {
        ob_batch_unlock_collateral($wallet_addresses, $amounts);
    }

    -- Step 5: Fee distribution to liquidity providers
    -- Fees are automatically kept in the vault by deducting from unlocked amounts.
    -- Winners receive (shares × $1 - 2% fee), so 2% remains locked in vault.
    --
    -- distribute_fees() distributes the collected fees to qualified LPs proportionally.
    -- See function definition above in this migration for implementation details.
    -- Edge cases:
    -- - No LPs: Fees remain in vault (safe accumulation)
    -- - Zero fees: No-op, returns early
    distribute_fees($query_id, $total_fees_collected);

    -- Verification: Check vault balance via ethereum_bridge queries
}
