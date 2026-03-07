/*
 * ORDER BOOK P&L MIGRATION
 *
 * Creates the ob_net_impacts table to track the net change of every transaction.
 * This version removes the redundant tx_hash from the temporary ob_tx_payouts table
 * to avoid bytea/text comparison errors in Kuneiform.
 */

-- =============================================================================
-- ob_net_impacts: Transaction Summary Audit Trail
-- =============================================================================
CREATE TABLE IF NOT EXISTS ob_net_impacts (
    id INT PRIMARY KEY,
    tx_hash BYTEA NOT NULL,
    query_id INT NOT NULL,
    participant_id INT NOT NULL,
    outcome BOOLEAN NOT NULL,
    shares_change INT8 NOT NULL,     -- Net shares gained (+) or lost (-)
    collateral_change NUMERIC(78,0), -- Net collateral magnitude
    is_negative BOOLEAN NOT NULL,    -- TRUE if collateral was spent, FALSE if received/refunded
    timestamp INT8 NOT NULL,

    FOREIGN KEY (query_id) REFERENCES ob_queries(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES ob_participants(id) ON DELETE CASCADE
);

-- Index for indexer to efficiently sync by ID
CREATE INDEX IF NOT EXISTS idx_ob_net_impacts_id ON ob_net_impacts(id);

-- Index for user portfolio queries
CREATE INDEX IF NOT EXISTS idx_ob_net_impacts_participant ON ob_net_impacts(participant_id);

-- Index for market history
CREATE INDEX IF NOT EXISTS idx_ob_net_impacts_query ON ob_net_impacts(query_id);

-- =============================================================================
-- ob_tx_payouts: Temporary table to accumulate impacts during matching
-- =============================================================================
CREATE TABLE IF NOT EXISTS ob_tx_payouts (
    id INT PRIMARY KEY, -- Surrogate key needed for Kwil
    participant_id INT NOT NULL,
    outcome BOOLEAN NOT NULL,
    shares_change INT8 NOT NULL,
    amount NUMERIC(78,0) NOT NULL,
    is_negative BOOLEAN NOT NULL
);

-- Helper to record a payout (legacy signature, defaults to TRUE outcome)
CREATE OR REPLACE ACTION ob_record_tx_payout(
    $participant_id INT,
    $amount NUMERIC(78,0)
) PRIVATE {
    $next_id INT;
    for $row in SELECT COALESCE(MAX(id), 0::INT) + 1 as val FROM ob_tx_payouts {
        $next_id := $row.val;
    }

    INSERT INTO ob_tx_payouts (
        id, participant_id, outcome, shares_change, amount, is_negative
    ) VALUES (
        $next_id, $participant_id, TRUE, 0::INT8, $amount, FALSE
    );
};

-- Internal helper to record impacts during matching/placement
CREATE OR REPLACE ACTION ob_record_tx_impact(
    $participant_id INT,
    $outcome BOOLEAN,
    $shares_change INT8,
    $amount NUMERIC(78,0),
    $is_negative BOOLEAN
) PRIVATE {
    $next_id INT;
    for $row in SELECT COALESCE(MAX(id), 0::INT) + 1 as val FROM ob_tx_payouts {
        $next_id := $row.val;
    }

    $actual_shares INT8 := $shares_change;
    if $is_negative {
        $actual_shares := - $shares_change;
    }

    INSERT INTO ob_tx_payouts (
        id, participant_id, outcome, shares_change, amount, is_negative
    ) VALUES (
        $next_id, $participant_id, $outcome, $actual_shares, $amount, $is_negative
    );
};

-- Helper to materialize and cleanup impacts for current TX
CREATE OR REPLACE ACTION ob_cleanup_tx_payouts(
    $query_id INT
) PRIVATE {
    $next_id INT;
    for $row in SELECT COALESCE(MAX(id), 0::INT) + 1 as val FROM ob_net_impacts {
        $next_id := $row.val;
    }

    -- Iterate over all touched (participant, outcome) pairs in this TX
    -- Deterministic ordering is CRITICAL for stable ID allocation across validators
    for $p in SELECT DISTINCT participant_id, outcome FROM ob_tx_payouts ORDER BY participant_id, outcome {
        -- Capture into local variables to avoid "unknown variable" error in nested loops
        $current_pid INT := $p.participant_id;
        $current_outcome BOOL := $p.outcome;
        
        $net_shares INT8 := 0;
        $net_collateral NUMERIC(100,0) := 0::NUMERIC(100,0);
        
        for $impact in SELECT shares_change, amount, is_negative FROM ob_tx_payouts WHERE participant_id = $current_pid AND outcome = $current_outcome {
            $net_shares := $net_shares + $impact.shares_change;
            if $impact.is_negative {
                $net_collateral := $net_collateral - $impact.amount::NUMERIC(100,0);
            } else {
                $net_collateral := $net_collateral + $impact.amount::NUMERIC(100,0);
            }
        }
        
        -- Call record_net_impact with final net values if change occurred
        if $net_shares != 0 OR $net_collateral != 0::NUMERIC(100,0) {
            $final_is_neg BOOL := FALSE;
            $final_mag NUMERIC(78,0) := 0::NUMERIC(78,0);
            
            if $net_collateral < 0::NUMERIC(100,0) {
                $final_is_neg := TRUE;
                $final_mag := (0::NUMERIC(100,0) - $net_collateral)::NUMERIC(78,0);
            } else {
                $final_mag := $net_collateral::NUMERIC(78,0);
            }
            
            ob_record_net_impact($next_id, $query_id, $current_pid, $current_outcome, $net_shares, $final_mag, $final_is_neg);
            $next_id := $next_id + 1;
        }
    }

    DELETE FROM ob_tx_payouts;
};

-- Helper to get total payout for a participant in current TX (Legacy helper)
CREATE OR REPLACE ACTION ob_get_tx_payout(
    $participant_id INT
) PRIVATE RETURNS (total_payout NUMERIC(78,0)) {
    $total NUMERIC(78,0) := 0::NUMERIC(78,0);
    for $row in SELECT amount, is_negative FROM ob_tx_payouts WHERE participant_id = $participant_id {
        if NOT $row.is_negative {
            $total := $total + $row.amount;
        }
    }
    RETURN $total;
};

-- Internal helper to record impacts into audit trail
CREATE OR REPLACE ACTION ob_record_net_impact(
    $id INT,
    $query_id INT,
    $participant_id INT,
    $outcome BOOLEAN,
    $shares_change INT8,
    $collateral_change NUMERIC(78,0),
    $is_negative BOOLEAN
) PRIVATE {
    INSERT INTO ob_net_impacts (
        id,
        tx_hash,
        query_id,
        participant_id,
        outcome,
        shares_change,
        collateral_change,
        is_negative,
        timestamp
    ) VALUES (
        $id,
        decode(@txid, 'hex'),
        $query_id,
        $participant_id,
        $outcome,
        $shares_change,
        $collateral_change,
        $is_negative,
        @block_timestamp
    );
};
