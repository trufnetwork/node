/*
 * ORDER BOOK P&L MIGRATION
 *
 * Creates the ob_net_impacts table to track the net change of every transaction.
 * This provides the minimal audit trail needed for the indexer to calculate:
 * 1. Cost Basis
 * 2. Realized P&L
 * 3. Unrealized P&L (via current holdings)
 * 4. Historical P&L Charting
 *
 * This table records ONE summary row per participant per transaction.
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
-- ob_tx_payouts: Temporary table to accumulate payouts during matching
-- =============================================================================
-- Since Kwil doesn't have session-local state, we use this table to track 
-- collateral flows during the recursive matching engine loops.
-- It is cleared at the end of each public transaction.
CREATE TABLE IF NOT EXISTS ob_tx_payouts (
    id INT PRIMARY KEY, -- Surrogate key needed for Kwil
    tx_hash BYTEA NOT NULL,
    participant_id INT NOT NULL,
    amount NUMERIC(78,0) NOT NULL
);

-- Helper to record a payout during matching
CREATE OR REPLACE ACTION ob_record_tx_payout(
    $participant_id INT,
    $amount NUMERIC(78,0)
) PRIVATE {
    INSERT INTO ob_tx_payouts (id, tx_hash, participant_id, amount)
    SELECT COALESCE(MAX(id), 0::INT) + 1, decode(@txid, 'hex'), $participant_id, $amount
    FROM ob_tx_payouts;
};

-- Helper to get total payout for a participant in current TX
CREATE OR REPLACE ACTION ob_get_tx_payout(
    $participant_id INT
) PRIVATE RETURNS (total_payout NUMERIC(78,0)) {
    $total NUMERIC(78,0) := 0::NUMERIC(78,0);
    -- Manual accumulation to avoid SUM() type issues in Kuneiform FOR loops
    for $row in SELECT amount FROM ob_tx_payouts WHERE tx_hash = decode(@txid, 'hex') AND participant_id = $participant_id {
        $total := $total + $row.amount;
    }
    RETURN $total;
};

-- Helper to cleanup payouts for current TX
CREATE OR REPLACE ACTION ob_cleanup_tx_payouts() PRIVATE {
    DELETE FROM ob_tx_payouts WHERE tx_hash = decode(@txid, 'hex');
};

-- =============================================================================
-- Internal helper to record impacts
-- =============================================================================
CREATE OR REPLACE ACTION ob_record_net_impact(
    $query_id INT,
    $participant_id INT,
    $outcome BOOLEAN,
    $shares_change INT8,
    $collateral_change NUMERIC(78,0),
    $is_negative BOOLEAN
) PRIVATE {
    -- Skip if no net change
    if $shares_change = 0 AND $collateral_change = 0::NUMERIC(78,0) {
        RETURN;
    }

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
    )
    SELECT
        COALESCE(MAX(id), 0::INT) + 1,
        decode(@txid, 'hex'), -- Built-in transaction ID/hash
        $query_id,
        $participant_id,
        $outcome,
        $shares_change,
        $collateral_change,
        $is_negative,
        @block_timestamp
    FROM ob_net_impacts;
};
