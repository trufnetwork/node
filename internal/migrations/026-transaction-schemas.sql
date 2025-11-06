/**
 * =============================================================================
 * Transaction Ledger Schema
 * =============================================================================
 *
 * Defines tables used to record transaction fee events and their per-recipient
 * distributions. Helper actions live in 027-transaction-actions.sql.
 */

CREATE TABLE IF NOT EXISTS transaction_methods (
    method_id INT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

INSERT INTO transaction_methods (method_id, name) VALUES
    (1, 'deployStream'),
    (2, 'insertRecords'),
    (3, 'setTaxonomies'),
    (4, 'transferTN'),
    (5, 'withdrawTN'),
    (6, 'requestAttestation'),
    (7, 'setMetadata')
ON CONFLICT (method_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS transaction_events (
    tx_id TEXT PRIMARY KEY,
    block_height INT8 NOT NULL,
    method_id INT NOT NULL REFERENCES transaction_methods(method_id),
    caller TEXT NOT NULL,
    fee_amount NUMERIC(78, 0) NOT NULL DEFAULT 0,
    fee_recipient TEXT,
    metadata TEXT -- future could be JSONB when supported
);

CREATE INDEX IF NOT EXISTS transaction_events_block_idx
    ON transaction_events (block_height, tx_id);

CREATE INDEX IF NOT EXISTS transaction_events_caller_idx
    ON transaction_events (caller, block_height, tx_id);

CREATE TABLE IF NOT EXISTS transaction_event_distributions (
    tx_id TEXT NOT NULL REFERENCES transaction_events(tx_id) ON DELETE CASCADE,
    sequence INT NOT NULL,
    recipient TEXT NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    note TEXT,
    PRIMARY KEY (tx_id, sequence)
);

CREATE INDEX IF NOT EXISTS tx_event_dist_rec_idx
    ON transaction_event_distributions (recipient, tx_id);
