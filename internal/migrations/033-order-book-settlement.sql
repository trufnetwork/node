/**
 * MIGRATION 033: ORDER BOOK SETTLEMENT
 *
 * Automatic atomic settlement processing:
 * - Bulk delete losing positions (efficient)
 * - Pay winners full $1.00 per share (no redemption fee)
 * - Refund open buy orders
 * - Delete all positions atomically
 * - Zero-sum settlement: losers fund winners
 */

-- Batch unlock collateral for multiple wallets
-- This helper processes all unlocks in a single call, avoiding nested queries in settlement
-- The $bridge parameter specifies which bridge to use (hoodi_tt2, sepolia_bridge, ethereum_bridge)
CREATE OR REPLACE ACTION ob_batch_unlock_collateral(
    $query_id INT, -- Pass query_id for impact recording
    $bridge TEXT,
    $wallet_addresses TEXT[],
    $amounts NUMERIC(78, 0)[],
    $outcomes BOOL[]
) PRIVATE {
    -- Validate input arrays have same length
    if COALESCE(array_length($wallet_addresses), 0) != COALESCE(array_length($amounts), 0) OR
       COALESCE(array_length($wallet_addresses), 0) != COALESCE(array_length($outcomes), 0) {
        ERROR('wallet_addresses, amounts and outcomes arrays must have the same length');
    }

    -- Process each unlock
    for $payout in
        SELECT wallet, amount, outcome
        FROM UNNEST($wallet_addresses, $amounts, $outcomes) AS u(wallet, amount, outcome)
    {
        $wallet_hex TEXT := $payout.wallet;
        $amount NUMERIC(78,0) := $payout.amount;
        $current_outcome BOOL := $payout.outcome;

        -- Use the correct bridge based on market configuration
        if $bridge = 'hoodi_tt2' {
            hoodi_tt2.unlock($wallet_hex, $amount);
        } else if $bridge = 'sepolia_bridge' {
            sepolia_bridge.unlock($wallet_hex, $amount);
        } else if $bridge = 'ethereum_bridge' {
            ethereum_bridge.unlock($wallet_hex, $amount);
        } else {
            ERROR('Invalid bridge in ob_batch_unlock_collateral: ' || COALESCE($bridge, 'NULL'));
        }

        -- Record impact for P&L
        $pid INT;
        $wallet_bytes BYTEA := decode(substring($wallet_hex, 3, 40), 'hex');
        for $p in SELECT id FROM ob_participants WHERE wallet_address = $wallet_bytes {
            $pid := $p.id;
        }

        if $pid IS NOT NULL {
            -- Use the actual outcome for this specific payout
            ob_record_net_impact($query_id, $pid, $current_outcome, 0::INT8, $amount, FALSE);
        }
    }
};

-- ============================================================================
-- Fee Distribution to Liquidity Providers
-- ============================================================================

/**
 * distribute_fees($query_id, $total_fees, $winning_outcome)
 *
 * Distributes redemption fees to liquidity providers based on sampled rewards.
 */
CREATE OR REPLACE ACTION distribute_fees(
    $query_id INT,
    $total_fees NUMERIC(78, 0),
    $winning_outcome BOOL
) PRIVATE {
    -- Get market details for fee split and unlock
    $bridge TEXT;
    $query_components BYTEA;
    for $row in SELECT bridge, query_components FROM ob_queries WHERE id = $query_id {
        $bridge := $row.bridge;
        $query_components := $row.query_components;
    }
    if $bridge IS NULL {
        ERROR('Market not found for query_id: ' || $query_id::TEXT);
    }

    -- Step 0: Calculate Shares (75/12.5/12.5 split)
    $lp_share NUMERIC(78, 0) := ($total_fees * 75::NUMERIC(78, 0)) / 100::NUMERIC(78, 0);
    $infra_share NUMERIC(78, 0) := ($total_fees * 125::NUMERIC(78, 0)) / 1000::NUMERIC(78, 0);
    
    -- Ensure 100% distribution: add any rounding dust to LP pool
    $lp_share := $lp_share + ($total_fees - $lp_share - (2::NUMERIC(78, 0) * $infra_share));

    -- Step 1: Payout Data Provider (0.25%)
    $dp_addr BYTEA;
    for $row in tn_utils.unpack_query_components($query_components) {
        $dp_addr := $row.data_provider;
    }
    
    $actual_dp_fees NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    if $dp_addr IS NOT NULL AND $infra_share > '0'::NUMERIC(78, 0) {
        $dp_wallet TEXT := '0x' || encode($dp_addr, 'hex');
        if $bridge = 'hoodi_tt2' {
            hoodi_tt2.unlock($dp_wallet, $infra_share);
            $actual_dp_fees := $infra_share;
        } else if $bridge = 'sepolia_bridge' {
            sepolia_bridge.unlock($dp_wallet, $infra_share);
            $actual_dp_fees := $infra_share;
        } else if $bridge = 'ethereum_bridge' {
            ethereum_bridge.unlock($dp_wallet, $infra_share);
            $actual_dp_fees := $infra_share;
        }

        -- Record DP reward impact (against winning side)
        $dp_pid INT;
        for $p in SELECT id FROM ob_participants WHERE wallet_address = $dp_addr {
            $dp_pid := $p.id;
        }
        if $dp_pid IS NOT NULL {
            ob_record_net_impact($query_id, $dp_pid, $winning_outcome, 0::INT8, $infra_share, FALSE);
        }
    }

    -- Step 2: Payout Validator (Leader) (0.25%)
    $actual_validator_fees NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $validator_wallet TEXT := tn_utils.get_leader_hex();
    if $validator_wallet != '' AND $infra_share > '0'::NUMERIC(78, 0) {
        -- Safe leader address conversion
        $leader_bytes_tmp BYTEA := tn_utils.get_leader_bytes();

        if $bridge = 'hoodi_tt2' {
            hoodi_tt2.unlock($validator_wallet, $infra_share);
            $actual_validator_fees := $infra_share;
        } else if $bridge = 'sepolia_bridge' {
            sepolia_bridge.unlock($validator_wallet, $infra_share);
            $actual_validator_fees := $infra_share;
        } else if $bridge = 'ethereum_bridge' {
            ethereum_bridge.unlock($validator_wallet, $infra_share);
            $actual_validator_fees := $infra_share;
        }

        -- Record Validator reward impact (against winning side)
        $val_pid INT;
        for $p in SELECT id FROM ob_participants WHERE wallet_address = $leader_bytes_tmp {
            $val_pid := $p.id;
        }
        if $val_pid IS NOT NULL {
            ob_record_net_impact($query_id, $val_pid, $winning_outcome, 0::INT8, $infra_share, FALSE);
        }
    }

    -- Step 3: Count distinct blocks sampled for this market
    $block_count INT := 0;
    for $row in SELECT COUNT(DISTINCT block) as cnt FROM ob_rewards WHERE query_id = $query_id {
        $block_count := $row.cnt;
    }

    -- Default values for summary
    $actual_fees_distributed NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $lp_count INT := 0;
    $distribution_id INT;
    for $row in SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM ob_fee_distributions {
        $distribution_id := $row.next_id;
    }

    $total_distributed_base NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $remainder NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $min_participant_id INT;

    -- If we have samples AND fees to distribute, calculate rewards
    if $block_count > 0 AND $lp_share > '0'::NUMERIC(78, 0) {

        $wallet_addresses TEXT[] := ARRAY[]::TEXT[];
        $amounts NUMERIC(78, 0)[] := ARRAY[]::NUMERIC(78, 0)[];
        $outcomes BOOL[] := ARRAY[]::BOOL[];

        -- Step 5: Calculate rewards with zero-loss distribution
        for $row in SELECT MIN(participant_id) as mid FROM ob_rewards WHERE query_id = $query_id {
            $min_participant_id := $row.mid;
        }

        for $row in 
            SELECT SUM((($lp_share::NUMERIC(78, 20) * total_percent_numeric) / (100::NUMERIC(78, 20) * $block_count::NUMERIC(78, 20)))::NUMERIC(78, 0))::NUMERIC(78, 0) as total
            FROM (
                SELECT SUM(reward_percent)::NUMERIC(78, 20) as total_percent_numeric
                FROM ob_rewards
                WHERE query_id = $query_id
                GROUP BY participant_id
            ) AS pt
        {
            $total_distributed_base := $row.total;
        }

        $remainder := $lp_share - $total_distributed_base;

        -- Aggregate into arrays for batch processing
        for $result in
            WITH participant_totals AS (
                SELECT
                    r.participant_id,
                    p.wallet_address,
                    SUM(r.reward_percent)::NUMERIC(78, 20) as total_percent_numeric
                FROM ob_rewards r
                JOIN ob_participants p ON r.participant_id = p.id
                WHERE r.query_id = $query_id
                GROUP BY r.participant_id, p.wallet_address
            ),
            calculated_rewards AS (
                SELECT
                    participant_id,
                    wallet_address,
                    '0x' || encode(wallet_address, 'hex') as wallet_hex,
                    (($lp_share::NUMERIC(78, 20) * total_percent_numeric) / (100::NUMERIC(78, 20) * $block_count::NUMERIC(78, 20)))::NUMERIC(78, 0) + 
                    (CASE WHEN participant_id = $min_participant_id THEN $remainder ELSE '0'::NUMERIC(78, 0) END) as final_reward,
                    total_percent_numeric
                FROM participant_totals
            )
            SELECT participant_id, wallet_address, wallet_hex, final_reward, total_percent_numeric 
            FROM calculated_rewards
        {
            $current_wallet_hex TEXT := $result.wallet_hex;
            $current_final_reward NUMERIC(78, 0) := $result.final_reward;
            $current_pid INT := $result.participant_id;
            $current_wallet_addr BYTEA := $result.wallet_address;
            $current_reward_percent NUMERIC(10, 2) := $result.total_percent_numeric::NUMERIC(10, 2);

            if $current_final_reward > '0'::NUMERIC(78, 0) {
                $wallet_addresses := array_append($wallet_addresses, $current_wallet_hex);
                $amounts := array_append($amounts, $current_final_reward);
                $outcomes := array_append($outcomes, $winning_outcome);
            }
        }

        if $wallet_addresses IS NOT NULL AND COALESCE(array_length($wallet_addresses), 0) > 0 {
            $lp_count := array_length($wallet_addresses);
            $actual_fees_distributed := $lp_share;

            -- Step 6: Batch unlock to all qualifying LPs
            ob_batch_unlock_collateral($query_id, $bridge, $wallet_addresses, $amounts, $outcomes);
        }
    }

    -- Step 7: Insert distribution summary (ALWAYS, even if $lp_count=0)
    INSERT INTO ob_fee_distributions (
        id, query_id, total_fees_distributed, total_dp_fees, total_validator_fees, total_lp_count, block_count, distributed_at
    ) VALUES (
        $distribution_id, $query_id, $actual_fees_distributed, $actual_dp_fees, $actual_validator_fees, $lp_count, $block_count, @block_timestamp
    );

    -- Step 7.5: Insert audit details (must be after summary for FK)
    if $lp_count > 0 {
        for $result in
            WITH participant_totals AS (
                SELECT
                    r.participant_id,
                    p.wallet_address,
                    SUM(r.reward_percent)::NUMERIC(78, 20) as total_percent_numeric
                FROM ob_rewards r
                JOIN ob_participants p ON r.participant_id = p.id
                WHERE r.query_id = $query_id
                GROUP BY r.participant_id, p.wallet_address
            ),
            calculated_rewards AS (
                SELECT
                    participant_id,
                    wallet_address,
                    (($lp_share::NUMERIC(78, 20) * total_percent_numeric) / (100::NUMERIC(78, 20) * $block_count::NUMERIC(78, 20)))::NUMERIC(78, 0) + 
                    (CASE WHEN participant_id = $min_participant_id THEN $remainder ELSE '0'::NUMERIC(78, 0) END) as final_reward,
                    total_percent_numeric
                FROM participant_totals
            )
            SELECT participant_id, wallet_address, final_reward, total_percent_numeric 
            FROM calculated_rewards
        {
            $det_final_reward NUMERIC(78, 0) := $result.final_reward;
            $det_pid INT := $result.participant_id;
            $det_wallet_addr BYTEA := $result.wallet_address;
            $det_reward_percent NUMERIC(10, 2) := $result.total_percent_numeric::NUMERIC(10, 2);

            if $det_final_reward > '0'::NUMERIC(78, 0) {
                INSERT INTO ob_fee_distribution_details (
                    distribution_id, participant_id, wallet_address, reward_amount, total_reward_percent
                ) VALUES (
                    $distribution_id, $det_pid, $det_wallet_addr, $det_final_reward, $det_reward_percent
                );
            }
        }
    }

    -- Step 8: Cleanup processed rewards (ONLY if actual distribution happened)
    if $lp_count > 0 {
        DELETE FROM ob_rewards WHERE query_id = $query_id;
    }
};

-- ============================================================================
-- Main Settlement Process
-- ============================================================================

/**
 * process_settlement($query_id, $winning_outcome)
 *
 * Internal helper to handle the state changes and payouts for settlement.
 */
CREATE OR REPLACE ACTION process_settlement(
    $query_id INT,
    $winning_outcome BOOL
) PRIVATE {
    -- SECTION 0: GET MARKET BRIDGE
    $bridge TEXT;
    for $row in SELECT bridge FROM ob_queries WHERE id = $query_id {
        $bridge := $row.bridge;
    }

    -- SECTION 1: CALCULATE PAYOUTS (Winners and Buy Refunds)
    $wallet_addresses TEXT[];
    $amounts NUMERIC(78, 0)[];
    $outcomes BOOL[];
    $total_fees NUMERIC(78, 0) := '0'::NUMERIC(78, 0);

    -- Aggregate ALL payouts in one query to avoid nested loops
    for $result in
        WITH calculated_payouts AS (
            SELECT
                '0x' || encode(p.wallet_address, 'hex') as wallet,
                CASE
                    WHEN pos.price >= 0 THEN ((pos.amount::NUMERIC(78, 0) * '1000000000000000000'::NUMERIC(78, 0) * 98::NUMERIC(78, 0)) / 100::NUMERIC(78, 0))
                    ELSE (pos.amount::NUMERIC(78, 0) * abs(pos.price)::NUMERIC(78, 0) * '10000000000000000'::NUMERIC(78, 0))
                END as amount,
                CASE
                    WHEN pos.price >= 0 THEN $winning_outcome
                    ELSE pos.outcome
                END as outcome,
                CASE
                    WHEN pos.price >= 0 THEN ((pos.amount::NUMERIC(78, 0) * '1000000000000000000'::NUMERIC(78, 0) * 2::NUMERIC(78, 0)) / 100::NUMERIC(78, 0))
                    ELSE '0'::NUMERIC(78, 0)
                END as fee
            FROM ob_positions pos
            JOIN ob_participants p ON pos.participant_id = p.id
            WHERE pos.query_id = $query_id
              AND (
                (pos.price >= 0 AND pos.outcome = $winning_outcome) -- Winners
                OR (pos.price < 0) -- All open buy orders get refunded
              )
        ),
        aggregated AS (
            SELECT
                ARRAY_AGG(wallet ORDER BY wallet, outcome) as wallets,
                ARRAY_AGG(amount::NUMERIC(78, 0) ORDER BY wallet, outcome) as amounts,
                ARRAY_AGG(outcome ORDER BY wallet, outcome) as outcomes,
                SUM(fee)::NUMERIC(78, 0) as fees
            FROM calculated_payouts
        )
        SELECT wallets, amounts, outcomes, COALESCE(fees, '0'::NUMERIC(78, 0)) as fees FROM aggregated
    {
        $wallet_addresses := $result.wallets;
        $amounts := $result.amounts;
        $outcomes := $result.outcomes;
        $total_fees := $result.fees;
    }

    -- Step 2: Delete all positions for this market atomically
    DELETE FROM ob_positions WHERE query_id = $query_id;

    -- Step 3: Process ALL payouts in a SINGLE batch call
    if $wallet_addresses IS NOT NULL AND COALESCE(array_length($wallet_addresses), 0) > 0 {
        ob_batch_unlock_collateral($query_id, $bridge, $wallet_addresses, $amounts, $outcomes);
    }

    -- Step 4: Distribute collected fees
    if $total_fees IS NOT NULL AND $total_fees > '0'::NUMERIC(78, 0) {
        distribute_fees($query_id, $total_fees, $winning_outcome);
    }
};

// Public trigger
CREATE OR REPLACE ACTION trigger_fee_distribution(
    $query_id INT,
    $total_fees TEXT
) PUBLIC {
    $has_role BOOL := FALSE;
    -- Safe caller normalization using precompiles
    $lower_caller TEXT := tn_utils.get_caller_hex();

    for $row in SELECT 1 FROM role_members WHERE owner = 'system' AND role_name = 'network_writer' AND wallet = $lower_caller LIMIT 1 {
        $has_role := TRUE;
    }
    if $has_role = FALSE { ERROR('Only network_writer can trigger fee distribution'); }

    -- Query market truth for winning outcome
    $winning BOOL;
    $found BOOL := FALSE;
    for $row in SELECT winning_outcome FROM ob_queries WHERE id = $query_id {
        $winning := $row.winning_outcome;
        $found := TRUE;
    }

    if NOT $found {
        ERROR('Market not found: ' || $query_id::TEXT);
    }

    $fees NUMERIC(78, 0) := $total_fees::NUMERIC(78, 0);
    distribute_fees($query_id, $fees, $winning);
}

