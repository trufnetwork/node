/*
 * ATTESTATION SCHEMA MIGRATION
 * 
 * Creates the essential tables needed for the attestation system:
 * - attestations: Stores attestation requests and signatures
 * - attestation_actions: Allowlist of actions permitted for attestation with normalized IDs
 */

-- Attestations table with request_tx_id as primary key
CREATE TABLE IF NOT EXISTS attestations (
    request_tx_id TEXT PRIMARY KEY,
    attestation_hash BYTEA NOT NULL,
    requester BYTEA NOT NULL,
    result_canonical BYTEA NOT NULL,
    encrypt_sig BOOLEAN NOT NULL DEFAULT false,
    created_height INT8 NOT NULL,
    signature BYTEA,
    validator_pubkey BYTEA,
    signed_height INT8,
    
    CONSTRAINT uq_att_composite UNIQUE (requester, created_height, attestation_hash),
    CONSTRAINT chk_att_encrypt_sig_false CHECK (encrypt_sig = false)
);

-- Block time (unix seconds) of the block a capture was taken in.
--
-- A market resolves against the value that stood at its settle_time, a unix
-- timestamp, and created_height alone cannot be compared with that. This is what
-- lets settle_market tell a capture taken before settle_time from one taken after.
--
-- Rows written before this column existed keep it NULL: their block time is not
-- recoverable from attestations. Settlement never resolves on a capture without a
-- time, so a market whose only captures predate this column is captured again.
ALTER TABLE attestations
ADD COLUMN IF NOT EXISTS created_timestamp INT8;

-- Allowlist table for actions permitted for attestation
CREATE TABLE IF NOT EXISTS attestation_actions (
    action_name TEXT PRIMARY KEY,
    action_id INT NOT NULL UNIQUE,
    
    CONSTRAINT chk_att_action_id_range CHECK (action_id >= 1 AND action_id <= 255)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS ix_att_created_height 
    ON attestations(created_height);

CREATE INDEX IF NOT EXISTS ix_att_signed_height 
    ON attestations(signed_height);

-- Composite index for signing workflow: fetches unsigned attestations by hash
CREATE INDEX IF NOT EXISTS ix_att_hash_unsigned 
    ON attestations(attestation_hash, signature);

-- Bootstrap the action ID registry per issue #1197
INSERT INTO attestation_actions (action_name, action_id) VALUES 
    ('get_record', 1),
    ('get_index', 2),
    ('get_change_over_time', 3),
    ('get_last_record', 4),
    ('get_first_record', 5)
ON CONFLICT (action_name) DO NOTHING;
