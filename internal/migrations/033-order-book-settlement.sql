/**
 * MIGRATION 033: ORDER BOOK SETTLEMENT
 *
 * Refactored version to avoid nested queries and CTE variable errors.
 * IMPORTANT: No direct use of $row.field in DML (INSERT/UPDATE/DELETE).
 */

-- ============================================================================
-- Batch Unlock Helper
-- ============================================================================

CREATE OR REPLACE ACTION ob_batch_unlock_collateral(
    $query_id INT,
    $bridge TEXT,
    $pids INT[],
    $wallet_hexes TEXT[],
    $amounts NUMERIC(78, 0)[],
    $outcomes BOOL[]
) PRIVATE {
    -- 1. Call precompile for each unlock (Safe in loops)
    for $payout in
        SELECT u.wallet_hex, u.amount
        FROM UNNEST($wallet_hexes, $amounts) AS u(wallet_hex, amount)
    {
        if $bridge = 'hoodi_tt2' {
            hoodi_tt2.unlock($payout.wallet_hex, $payout.amount);
        } else if $bridge = 'sepolia_bridge' {
            sepolia_bridge.unlock($payout.wallet_hex, $payout.amount);
        } else if $bridge = 'ethereum_bridge' {
            ethereum_bridge.unlock($payout.wallet_hex, $payout.amount);
        }
    }

    -- 2. Record all impacts using a loop to avoid "unknown variable" in INSERT...SELECT
    $next_id INT;
    for $row in SELECT COALESCE(MAX(id), 0::INT) + 1 as val FROM ob_net_impacts {
        $next_id := $row.val;
    }

    for $imp in
        SELECT pid, outcome, amount
        FROM UNNEST($pids, $outcomes, $amounts) AS u(pid, outcome, amount)
    {
        $cur_pid INT := $imp.pid;
        $cur_outcome BOOL := $imp.outcome;
        $cur_amount NUMERIC(78,0) := $imp.amount;

        if $cur_pid IS NOT NULL {
            ob_record_net_impact($next_id, $query_id, $cur_pid, $cur_outcome, 0::INT8, $cur_amount, FALSE);
            $next_id := $next_id + 1;
        }
    }
};

-- ============================================================================
-- Fee Distribution to Liquidity Providers
-- ============================================================================

CREATE OR REPLACE ACTION distribute_fees(
    $query_id INT,
    $total_fees NUMERIC(78, 0),
    $winning_outcome BOOL
) PRIVATE {
    $bridge TEXT;
    $query_components BYTEA;
    
    $min_pid INT;
    $remainder NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $pids INT[];
    $wallet_hexes TEXT[];
    $amounts NUMERIC(78, 0)[];
    $outcomes BOOL[];

    for $row in SELECT bridge, query_components FROM ob_queries WHERE id = $query_id {
        $bridge := $row.bridge;
        $query_components := $row.query_components;
    }

    -- 75/12.5/12.5 split
    $lp_share NUMERIC(78, 0) := ($total_fees * 75::NUMERIC(78, 0)) / 100::NUMERIC(78, 0);
    $infra_share NUMERIC(78, 0) := ($total_fees * 125::NUMERIC(78, 0)) / 1000::NUMERIC(78, 0);
    $lp_share := $lp_share + ($total_fees - $lp_share - (2::NUMERIC(78, 0) * $infra_share));

    -- Payout Data Provider
    $dp_addr BYTEA;
    for $row in tn_utils.unpack_query_components($query_components) { $dp_addr := $row.data_provider; }

    $actual_dp_fees NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    if $dp_addr IS NOT NULL AND $infra_share > '0'::NUMERIC(78, 0) {
        $dp_wallet_hex TEXT := '0x' || encode($dp_addr, 'hex');
        if $bridge = 'hoodi_tt2' { hoodi_tt2.unlock($dp_wallet_hex, $infra_share); }
        else if $bridge = 'sepolia_bridge' { sepolia_bridge.unlock($dp_wallet_hex, $infra_share); }
        else if $bridge = 'ethereum_bridge' { ethereum_bridge.unlock($dp_wallet_hex, $infra_share); }

        $actual_dp_fees := $infra_share;

        $dp_pid INT;
        for $p in SELECT id FROM ob_participants WHERE wallet_address = $dp_addr { $dp_pid := $p.id; }
        if $dp_pid IS NOT NULL {
            $next_id_dp INT;
            for $row in SELECT COALESCE(MAX(id), 0::INT) + 1 as val FROM ob_net_impacts { $next_id_dp := $row.val; }
            ob_record_net_impact($next_id_dp, $query_id, $dp_pid, $winning_outcome, 0::INT8, $infra_share, FALSE);
        }
    }

    -- Payout Validator (Leader)
    $actual_validator_fees NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $val_wallet TEXT := tn_utils.get_leader_hex();
    if $val_wallet != '' AND $infra_share > '0'::NUMERIC(78, 0) {
        if $bridge = 'hoodi_tt2' { hoodi_tt2.unlock($val_wallet, $infra_share); }
        else if $bridge = 'sepolia_bridge' { sepolia_bridge.unlock($val_wallet, $infra_share); }
        else if $bridge = 'ethereum_bridge' { ethereum_bridge.unlock($val_wallet, $infra_share); }

        $actual_validator_fees := $infra_share;

        $val_pid INT;
        $leader_bytes BYTEA := tn_utils.get_leader_bytes();
        for $p in SELECT id FROM ob_participants WHERE wallet_address = $leader_bytes { $val_pid := $p.id; }
        if $val_pid IS NOT NULL {
            $next_id_val INT;
            for $row in SELECT COALESCE(MAX(id), 0::INT) + 1 as val FROM ob_net_impacts { $next_id_val := $row.val; }
            ob_record_net_impact($next_id_val, $query_id, $val_pid, $winning_outcome, 0::INT8, $infra_share, FALSE);
        }
    }

    -- LP Distribution Pre-calculation
    -- GURANTEE: Take a final sample at settlement to ensure distribution happens even for short-lived markets
    sample_lp_rewards($query_id, @height);

    $block_count INT := 0;
    for $row in SELECT COUNT(DISTINCT block) as cnt FROM ob_rewards WHERE query_id = $query_id { $block_count := $row.cnt; }

    $lp_count INT := 0;
    $actual_fees_distributed NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $distribution_id INT;
    for $row in SELECT COALESCE(MAX(id), 0) + 1 as val FROM ob_fee_distributions { $distribution_id := $row.val; }

    -- PRE-INSERT PARENT RECORD to satisfy FK for details
    INSERT INTO ob_fee_distributions (
        id, query_id, total_fees_distributed, total_dp_fees, total_validator_fees, total_lp_count, block_count, distributed_at
    ) VALUES (
        $distribution_id, $query_id, '0'::NUMERIC(78, 0), $actual_dp_fees, $actual_validator_fees, 0, $block_count, @block_timestamp
    );

    if $block_count > 0 {
        for $row in SELECT MIN(participant_id) as val FROM ob_rewards WHERE query_id = $query_id { $min_pid := $row.val; }

        -- Calculate remainder
        $total_calc NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
        for $res in 
            SELECT participant_id, SUM(reward_percent) as reward_percent
            FROM ob_rewards WHERE query_id = $query_id GROUP BY participant_id
        {
            $reward_tmp NUMERIC(78, 0) := (($lp_share::NUMERIC(78, 20) * $res.reward_percent::NUMERIC(78, 20)) / (100::NUMERIC(78, 20) * $block_count::NUMERIC(78, 20)))::NUMERIC(78, 0);
            $total_calc := $total_calc + $reward_tmp;
        }
        $remainder := $lp_share - $total_calc;

        for $res in
            WITH reward_summary AS (
                SELECT r.participant_id, p.wallet_address, SUM(r.reward_percent) as reward_percent
                FROM ob_rewards r JOIN ob_participants p ON r.participant_id = p.id
                WHERE r.query_id = $query_id GROUP BY r.participant_id, p.wallet_address
            )
            SELECT participant_id, wallet_address, '0x' || encode(wallet_address, 'hex') as wallet_hex, reward_percent
            FROM reward_summary ORDER BY wallet_address
        {
            $reward_final NUMERIC(78, 0) := (($lp_share::NUMERIC(78, 20) * $res.reward_percent::NUMERIC(78, 20)) / (100::NUMERIC(78, 20) * $block_count::NUMERIC(78, 20)))::NUMERIC(78, 0);
            if $res.participant_id = $min_pid { $reward_final := $reward_final + $remainder; }

            if $reward_final > '0'::NUMERIC(78, 0) {
                $pids := array_append($pids, $res.participant_id);
                $wallet_hexes := array_append($wallet_hexes, $res.wallet_hex);
                $amounts := array_append($amounts, $reward_final);
                $outcomes := array_append($outcomes, $winning_outcome);
                
                -- Record audit detail (Parent exists now)
                $curr_pid INT := $res.participant_id;
                $curr_wallet BYTEA := $res.wallet_address;
                $curr_reward NUMERIC(78,0) := $reward_final;
                $curr_pct NUMERIC(10,2) := $res.reward_percent::NUMERIC(10, 2);

                INSERT INTO ob_fee_distribution_details (distribution_id, participant_id, wallet_address, reward_amount, total_reward_percent)
                VALUES ($distribution_id, $curr_pid, $curr_wallet, $curr_reward, $curr_pct);
            }
        }

        if COALESCE(array_length($pids), 0) > 0 {
            $lp_count := array_length($pids);
            $actual_fees_distributed := $lp_share;
            
            -- UPDATE SUMMARY with final values
            UPDATE ob_fee_distributions
            SET total_fees_distributed = $actual_fees_distributed,
                total_lp_count = $lp_count
            WHERE id = $distribution_id;

            ob_batch_unlock_collateral($query_id, $bridge, $pids, $wallet_hexes, $amounts, $outcomes);
        }
    }

    if $lp_count > 0 { DELETE FROM ob_rewards WHERE query_id = $query_id; }
};

-- ============================================================================
-- Main Settlement Process
-- ============================================================================

CREATE OR REPLACE ACTION process_settlement(
    $query_id INT,
    $winning_outcome BOOL
) PRIVATE {
    $bridge TEXT;
    for $row in SELECT bridge FROM ob_queries WHERE id = $query_id { $bridge := $row.bridge; }

    $pids INT[];
    $wallet_hexes TEXT[];
    $amounts NUMERIC(78, 0)[];
    $outcomes BOOL[];
    $total_fees NUMERIC(78, 0) := '0'::NUMERIC(78, 0);

    for $res in
        WITH target_positions AS (
            SELECT pos.participant_id, p.wallet_address, pos.price, pos.amount, pos.outcome
            FROM ob_positions pos JOIN ob_participants p ON pos.participant_id = p.id
            WHERE pos.query_id = $query_id AND ((pos.price >= 0 AND pos.outcome = $winning_outcome) OR (pos.price < 0))
        ),
        payout_calculation AS (
            SELECT 
                participant_id,
                wallet_address,
                '0x' || encode(wallet_address, 'hex') as wallet_hex,
                CASE 
                    WHEN price >= 0 THEN ((amount::NUMERIC(78, 0) * '1000000000000000000'::NUMERIC(78, 0) * 98::NUMERIC(78, 0)) / 100::NUMERIC(78, 0))
                    ELSE (amount::NUMERIC(78, 0) * (CASE WHEN price < 0 THEN -price ELSE price END)::NUMERIC(78, 0) * '10000000000000000'::NUMERIC(78, 0))
                END as pay,
                CASE WHEN price >= 0 THEN $winning_outcome ELSE outcome END as out,
                CASE WHEN price >= 0 THEN ((amount::NUMERIC(78, 0) * '1000000000000000000'::NUMERIC(78, 0) * 2::NUMERIC(78, 0)) / 100::NUMERIC(78, 0)) ELSE 0::NUMERIC(78, 0) END as fee
            FROM target_positions
        )
        SELECT * FROM payout_calculation
    {
        if $res.pay > '0'::NUMERIC(78, 0) {
            $pids := array_append($pids, $res.participant_id);
            $wallet_hexes := array_append($wallet_hexes, $res.wallet_hex);
            $amounts := array_append($amounts, $res.pay);
            $outcomes := array_append($outcomes, $res.out);
            $total_fees := $total_fees + $res.fee;
        }
    }

    DELETE FROM ob_positions WHERE query_id = $query_id;

    if COALESCE(array_length($pids), 0) > 0 {
        ob_batch_unlock_collateral($query_id, $bridge, $pids, $wallet_hexes, $amounts, $outcomes);
    }

    if $total_fees > '0'::NUMERIC(78, 0) {
        distribute_fees($query_id, $total_fees, $winning_outcome);
    }
};

CREATE OR REPLACE ACTION trigger_fee_distribution(
    $query_id INT,
    $total_fees TEXT
) PUBLIC {
    $has_role BOOL := FALSE;
    $lower_caller TEXT := tn_utils.get_caller_hex();
    for $row in SELECT 1 FROM role_members WHERE owner = 'system' AND role_name = 'network_writer' AND wallet = $lower_caller LIMIT 1 { $has_role := TRUE; }
    if $has_role = FALSE { ERROR('Only network_writer can trigger fee distribution'); }

    $winning BOOL;
    $found BOOL := FALSE;
    for $row in SELECT winning_outcome FROM ob_queries WHERE id = $query_id {
        $winning := $row.winning_outcome;
        $found := TRUE;
    }
    if NOT $found { ERROR('Market not found: ' || $query_id::TEXT); }

    $fees NUMERIC(78, 0) := $total_fees::NUMERIC(78, 0);
    distribute_fees($query_id, $fees, $winning);
}
