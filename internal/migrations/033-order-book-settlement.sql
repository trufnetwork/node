/**
 * MIGRATION 034: ORDER BOOK SETTLEMENT
 *
 * Automatic atomic settlement processing:
 * - Bulk delete losing positions (efficient)
 * - Pay winners (shares × $1.00 - 2% redemption fee)
 * - Refund open buy orders (no fee)
 * - Delete all positions atomically
 * - Collect fees in vault (Issue 9 will distribute them)
 *
 * Implementation Note:
 * Uses a staging table (ob_settlement_payouts) to collect payout data during
 * position processing, then processes all payouts after the loop completes.
 * This avoids nested queries (Kuneiform limitation: cannot call functions inside FOR loops).
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
        -- DEBUG: Insert unlock attempt into a log table (if it exists)
        -- Actual unlock
        ethereum_bridge.unlock($payout.wallet, $payout.amount);
    }
};

-- Process settlement: Pay winners, refund open buys, collect fees
CREATE OR REPLACE ACTION process_settlement(
    $query_id INT,
    $winning_outcome BOOL
) PRIVATE {
    $collateral_decimals INT8 := 18;
    $redemption_fee_bps INT := 200;  -- 2% (200 basis points)
    $total_fees_collected NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $one_token NUMERIC(78, 0) := '1000000000000000000'::NUMERIC(78, 0);

    -- Step 1: Bulk delete all losing positions (efficient single operation)
    -- Deletes losing holds (price=0) and losing open sells (price>0)
    -- This removes ~50% of positions upfront
    DELETE FROM ob_positions
    WHERE query_id = $query_id
      AND outcome = NOT $winning_outcome
      AND price >= 0;  -- Holdings and open sells only

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
            (amount::NUMERIC(78, 0) * $one_token)::NUMERIC(78, 0) as gross_winner_payout,
            ((amount::NUMERIC(78, 0) * $one_token * $redemption_fee_bps::NUMERIC(78, 0)) / 10000::NUMERIC(78, 0))::NUMERIC(78, 0) as winner_fee,
            ((amount::NUMERIC(78, 0) * abs(price)::NUMERIC(78, 0) * $one_token) / 100::NUMERIC(78, 0))::NUMERIC(78, 0) as refund_amount
        FROM remaining_positions
    ),
    payouts AS (
        SELECT
            wallet_address,
            -- Now use pre-calculated values in CASE (all same type!)
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

    -- Step 5: Fee distribution (Issue 9 will implement this)
    -- For now: Fees accumulate in vault
    -- TODO (Issue 9): Uncomment when distribute_fees() is implemented
    -- distribute_fees($query_id, $total_fees_collected);

    -- Note: Fees remain in vault and can be:
    -- 1. Withdrawn by governance for network treasury
    -- 2. Distributed when Issue 9 is complete
    -- 3. Tracked via vault balance queries
}
