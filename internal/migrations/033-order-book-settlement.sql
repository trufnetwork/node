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

    -- Pre-compute block_count and create parent distribution record early
    -- so DP and validator detail rows can reference it via FK
    $block_count INT := 0;
    for $row in SELECT COUNT(DISTINCT block) as cnt FROM ob_rewards WHERE query_id = $query_id { $block_count := $row.cnt; }

    $lp_count INT := 0;
    $actual_fees_distributed NUMERIC(78, 0) := '0'::NUMERIC(78, 0);
    $distribution_id INT;
    for $row in SELECT COALESCE(MAX(id), 0) + 1 as val FROM ob_fee_distributions { $distribution_id := $row.val; }

    -- PRE-INSERT PARENT RECORD with placeholders (updated at end with final values)
    INSERT INTO ob_fee_distributions (
        id, query_id, total_fees_distributed, total_dp_fees, total_validator_fees, total_lp_count, block_count, distributed_at
    ) VALUES (
        $distribution_id, $query_id, '0'::NUMERIC(78, 0), '0'::NUMERIC(78, 0), '0'::NUMERIC(78, 0), 0, $block_count, @block_timestamp
    );

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

        -- Ensure DP has a participant record so the fee is tracked in ob_net_impacts
        $dp_pid INT;
        for $p in SELECT id FROM ob_participants WHERE wallet_address = $dp_addr { $dp_pid := $p.id; }
        if $dp_pid IS NULL {
            INSERT INTO ob_participants (id, wallet_address)
            SELECT COALESCE(MAX(id), 0) + 1, $dp_addr
            FROM ob_participants;
            for $p in SELECT id FROM ob_participants WHERE wallet_address = $dp_addr { $dp_pid := $p.id; }
        }
        if $dp_pid IS NOT NULL {
            $next_id_dp INT;
            for $row in SELECT COALESCE(MAX(id), 0::INT) + 1 as val FROM ob_net_impacts { $next_id_dp := $row.val; }
            ob_record_net_impact($next_id_dp, $query_id, $dp_pid, $winning_outcome, 0::INT8, $infra_share, FALSE);

            -- Record DP in distribution details so indexer picks it up
            INSERT INTO ob_fee_distribution_details (distribution_id, participant_id, wallet_address, reward_amount, total_reward_percent)
            VALUES ($distribution_id, $dp_pid, $dp_addr, $infra_share, 12.50::NUMERIC(10, 2));
        }
    }

    -- Payout Validators (split evenly among all active validators)
    $actual_validator_fees NUMERIC(78, 0) := '0'::NUMERIC(78, 0);

    if $infra_share > '0'::NUMERIC(78, 0) {
        $validator_count INT := tn_utils.get_validator_count();

        if $validator_count > 0 {
            $per_validator NUMERIC(78, 0) := $infra_share / $validator_count::NUMERIC(78, 0);
            $val_remainder NUMERIC(78, 0) := $infra_share - ($per_validator * $validator_count::NUMERIC(78, 0));
            $first_validator BOOL := TRUE;
            $val_pct NUMERIC(10, 2) := 12.50::NUMERIC(10, 2) / $validator_count::NUMERIC(10, 2);
            $val_pct_remainder NUMERIC(10, 2) := 12.50::NUMERIC(10, 2) - ($val_pct * $validator_count::NUMERIC(10, 2));

            for $v in tn_utils.get_validators() {
                -- Extract row fields to local variables (Kuneiform SQL generator limitation)
                $v_wallet_hex TEXT := $v.wallet_hex;
                $v_wallet_bytes BYTEA := $v.wallet_bytes;
                $v_payout NUMERIC(78, 0) := $per_validator;
                $v_pct NUMERIC(10, 2) := $val_pct;

                -- Give remainder to first validator (deterministic: sorted by pubkey)
                if $first_validator {
                    $v_payout := $v_payout + $val_remainder;
                    $v_pct := $v_pct + $val_pct_remainder;
                    $first_validator := FALSE;
                }

                -- Skip validators with zero payout (e.g., infra_share < validator_count)
                if $v_payout > '0'::NUMERIC(78, 0) {
                    -- Unlock funds via bridge
                    if $bridge = 'hoodi_tt2' { hoodi_tt2.unlock($v_wallet_hex, $v_payout); }
                    else if $bridge = 'sepolia_bridge' { sepolia_bridge.unlock($v_wallet_hex, $v_payout); }
                    else if $bridge = 'ethereum_bridge' { ethereum_bridge.unlock($v_wallet_hex, $v_payout); }

                    -- Ensure validator has a participant record
                    $v_pid INT;
                    for $p in SELECT id FROM ob_participants WHERE wallet_address = $v_wallet_bytes { $v_pid := $p.id; }
                    if $v_pid IS NULL {
                        INSERT INTO ob_participants (id, wallet_address)
                        SELECT COALESCE(MAX(id), 0) + 1, $v_wallet_bytes
                        FROM ob_participants;
                        for $p in SELECT id FROM ob_participants WHERE wallet_address = $v_wallet_bytes { $v_pid := $p.id; }
                    }
                    if $v_pid IS NOT NULL {
                        $next_id_val INT;
                        for $row in SELECT COALESCE(MAX(id), 0::INT) + 1 as val FROM ob_net_impacts { $next_id_val := $row.val; }
                        ob_record_net_impact($next_id_val, $query_id, $v_pid, $winning_outcome, 0::INT8, $v_payout, FALSE);

                        -- Record validator in distribution details so indexer picks it up
                        INSERT INTO ob_fee_distribution_details (distribution_id, participant_id, wallet_address, reward_amount, total_reward_percent)
                        VALUES ($distribution_id, $v_pid, $v_wallet_bytes, $v_payout, $v_pct)
                        ON CONFLICT (distribution_id, participant_id) DO UPDATE
                        SET reward_amount = ob_fee_distribution_details.reward_amount + $v_payout,
                            total_reward_percent = ob_fee_distribution_details.total_reward_percent + $v_pct;
                    }

                    $actual_validator_fees := $actual_validator_fees + $v_payout;
                }
            }
        }
    }

    -- LP Distribution
    -- NOTE: sample_lp_rewards() is called in process_settlement() BEFORE ob_positions are deleted,
    -- so the final sample reads the live order book. Do NOT call it here.

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

                -- Record LP audit detail (skip if percent rounds to 0 at NUMERIC(10,2) precision)
                $curr_pid INT := $res.participant_id;
                $curr_wallet BYTEA := $res.wallet_address;
                $curr_reward NUMERIC(78,0) := $reward_final;
                $curr_pct NUMERIC(10,2) := $res.reward_percent::NUMERIC(10, 2);

                if $curr_pct > 0.00::NUMERIC(10, 2) {
                    INSERT INTO ob_fee_distribution_details (distribution_id, participant_id, wallet_address, reward_amount, total_reward_percent)
                    VALUES ($distribution_id, $curr_pid, $curr_wallet, $curr_reward, $curr_pct)
                    ON CONFLICT (distribution_id, participant_id) DO UPDATE
                    SET reward_amount = ob_fee_distribution_details.reward_amount + $curr_reward,
                        total_reward_percent = ob_fee_distribution_details.total_reward_percent + $curr_pct;
                }
            }
        }

        if COALESCE(array_length($pids), 0) > 0 {
            $lp_count := array_length($pids);
            $actual_fees_distributed := $lp_share;
            ob_batch_unlock_collateral($query_id, $bridge, $pids, $wallet_hexes, $amounts, $outcomes);
        }
    }

    -- UPDATE SUMMARY with final values
    UPDATE ob_fee_distributions
    SET total_fees_distributed = $actual_fees_distributed,
        total_dp_fees = $actual_dp_fees,
        total_validator_fees = $actual_validator_fees,
        total_lp_count = $lp_count
    WHERE id = $distribution_id;

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

    -- First: record settlement events for ALL positions (winners + losers + open buys)
    -- This uses a broader query than the payout calculation to include losing positions.
    for $evt_row in
        SELECT pos.participant_id, pos.price, pos.amount, pos.outcome
        FROM ob_positions pos
        WHERE pos.query_id = $query_id
    {
        $evt_pid INT := $evt_row.participant_id;
        $evt_out BOOL := $evt_row.outcome;
        $evt_amount INT8 := $evt_row.amount;
        $evt_raw_price INT := $evt_row.price;

        -- Determine settlement price for the event:
        -- - Winning holdings (price=0, winning outcome): 98 (redemption at $0.98)
        -- - Winning sell orders (price>0, winning outcome): 98 (redeemed, not sold)
        -- - Losing positions (wrong outcome, price>=0): 0 (worthless)
        -- - Open buy orders (price<0): abs(price) (collateral refund at buy price)
        $evt_settle_price INT;
        if $evt_raw_price < 0 {
            $evt_settle_price := -$evt_raw_price;
        } else if $evt_out = $winning_outcome {
            $evt_settle_price := 98;
        } else {
            $evt_settle_price := 0;
        }
        ob_record_order_event($query_id, $evt_pid, 'settled', $evt_out, $evt_settle_price, $evt_amount, NULL);
    }

    -- Second: calculate payouts (original logic, only winners + open buys)
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

    -- GUARANTEE: Take a final LP sample BEFORE deleting positions.
    -- sample_lp_rewards reads ob_positions, so it must run while the book is still live.
    sample_lp_rewards($query_id, @height);

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
