/*
 * ATTESTATION SCHEMA MIGRATION
 * 
 * Creates the essential tables needed for the attestation system:
 * - attestations: Stores attestation requests and signatures with composite PK (requester, attestation_hash)
 * - attestation_actions: Allowlist of actions permitted for attestation with normalized IDs
 */

-- Attestations table with composite primary key supporting per-user attestations
CREATE TABLE IF NOT EXISTS attestations (
    attestation_hash BYTEA NOT NULL,
    requester BYTEA NOT NULL,
    result_canonical BYTEA NOT NULL,
    encrypt_sig BOOLEAN NOT NULL DEFAULT false,
    created_height INT8 NOT NULL,
    signature BYTEA,
    validator_pubkey BYTEA,
    signed_height INT8,
    
    CONSTRAINT pk_attestations PRIMARY KEY (requester, created_height, attestation_hash),
    CONSTRAINT chk_att_encrypt_sig_false CHECK (encrypt_sig = false)
);

-- Allowlist table for actions permitted for attestation
CREATE TABLE IF NOT EXISTS attestation_actions (
    action_name TEXT PRIMARY KEY,
    action_id INT NOT NULL UNIQUE,
    
    CONSTRAINT chk_att_action_id_range CHECK (action_id >= 1 AND action_id <= 255)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS ix_att_req_created 
    ON attestations(requester, created_height);

CREATE INDEX IF NOT EXISTS ix_att_created_height 
    ON attestations(created_height);

CREATE INDEX IF NOT EXISTS ix_att_signed_height 
    ON attestations(signed_height);

-- Bootstrap the action ID registry per issue #1197
INSERT INTO attestation_actions (action_name, action_id) VALUES 
    ('get_record', 1),
    ('get_index', 2),
    ('get_change_over_time', 3),
    ('get_last_record', 4),
    ('get_first_record', 5)
ON CONFLICT (action_name) DO NOTHING;
