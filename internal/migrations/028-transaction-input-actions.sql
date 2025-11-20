/**
 * =============================================================================
 * Transaction Input Data Actions
 * =============================================================================
 *
 * Provides on-demand retrieval of transaction input data by querying
 * data tables using tx_id foreign keys. These actions are called from
 * the transaction explorer frontend when users click "Show Input Data".
 *
 * Design Philosophy:
 * - No data duplication: Query existing tables with tx_id columns
 * - On-demand loading: Only fetch when explicitly requested
 * - Direct node calls: No indexer involvement
 * - Simple queries: Single WHERE clause using indexed tx_id
 *
 * Related Tables:
 * - streams (tx_id column added in 000-initial-data.sql:165)
 * - primitive_events (tx_id column added in 000-initial-data.sql:169)
 * - taxonomies (tx_id column added in 000-initial-data.sql:173)
 * - metadata (tx_id column added in 000-initial-data.sql:177)
 * - attestations (uses request_tx_id as PRIMARY KEY)
 *
 * Related Actions:
 * - get_transaction_event (027-transaction-actions.sql:99) - Get basic tx info
 * - record_transaction_event (027-transaction-actions.sql:1) - Record tx events
 */

/**
 * get_transaction_streams: Retrieve streams deployed in a transaction
 *
 * Used for: deployStream transactions (method_id = 1)
 *
 * @param $tx_id - Transaction ID (hex string with or without 0x prefix)
 * @return Array of streams created in the transaction
 */
CREATE OR REPLACE ACTION get_transaction_streams(
    $tx_id TEXT
) PUBLIC VIEW RETURNS TABLE (
    stream_id TEXT,
    data_provider TEXT,
    stream_type TEXT,
    created_at INT8
) {
    IF $tx_id IS NULL OR trim($tx_id) = '' {
        ERROR('tx_id is required');
    }

    -- Normalize tx_id: remove 0x prefix if present (data tables store without 0x)
    $tx_clean TEXT := LOWER($tx_id);
    IF substring($tx_clean, 1, 2) = '0x' {
        $tx_clean := substring($tx_clean, 3);
    }

    RETURN SELECT
        stream_id,
        data_provider,
        stream_type,
        created_at
    FROM streams
    WHERE tx_id = $tx_clean
    ORDER BY stream_id;
};

/**
 * get_transaction_records: Retrieve records inserted in a transaction
 *
 * Used for: insertRecords transactions (method_id = 2)
 *
 * @param $tx_id - Transaction ID (hex string with or without 0x prefix)
 * @return Array of primitive events (records) inserted in the transaction
 *
 * Note: May return 100+ records for batch insertions
 */
CREATE OR REPLACE ACTION get_transaction_records(
    $tx_id TEXT
) PUBLIC VIEW RETURNS TABLE (
    data_provider TEXT,
    stream_id TEXT,
    event_time INT8,
    value NUMERIC(36, 18),
    created_at INT8
) {
    IF $tx_id IS NULL OR trim($tx_id) = '' {
        ERROR('tx_id is required');
    }

    -- Normalize tx_id: remove 0x prefix if present (data tables store without 0x)
    $tx_clean TEXT := LOWER($tx_id);
    IF substring($tx_clean, 1, 2) = '0x' {
        $tx_clean := substring($tx_clean, 3);
    }

    RETURN SELECT
        s.data_provider,
        s.stream_id,
        pe.event_time,
        pe.value,
        pe.created_at
    FROM primitive_events pe
    JOIN streams s ON pe.stream_ref = s.id
    WHERE pe.tx_id = $tx_clean
    ORDER BY s.data_provider, s.stream_id, pe.event_time;
};

/**
 * get_transaction_taxonomies: Retrieve taxonomies created in a transaction
 *
 * Used for: setTaxonomies transactions (method_id = 3)
 *
 * @param $tx_id - Transaction ID (hex string with or without 0x prefix)
 * @return Array of taxonomy relationships created in the transaction
 *
 * Note: Multiple rows with same group_sequence represent one taxonomy version
 */
CREATE OR REPLACE ACTION get_transaction_taxonomies(
    $tx_id TEXT
) PUBLIC VIEW RETURNS TABLE (
    data_provider TEXT,
    stream_id TEXT,
    child_data_provider TEXT,
    child_stream_id TEXT,
    weight NUMERIC(36, 18),
    start_time INT8,
    group_sequence INT8,
    created_at INT8
) {
    IF $tx_id IS NULL OR trim($tx_id) = '' {
        ERROR('tx_id is required');
    }

    -- Normalize tx_id: remove 0x prefix if present (data tables store without 0x)
    $tx_clean TEXT := LOWER($tx_id);
    IF substring($tx_clean, 1, 2) = '0x' {
        $tx_clean := substring($tx_clean, 3);
    }

    RETURN SELECT
        s.data_provider,
        s.stream_id,
        cs.data_provider AS child_data_provider,
        cs.stream_id AS child_stream_id,
        t.weight,
        t.start_time,
        t.group_sequence,
        t.created_at
    FROM taxonomies t
    JOIN streams s ON t.stream_ref = s.id
    JOIN streams cs ON t.child_stream_ref = cs.id
    WHERE t.tx_id = $tx_clean
    ORDER BY s.data_provider, s.stream_id, t.group_sequence, cs.stream_id;
};

/**
 * get_transaction_metadata: Retrieve metadata set in a transaction
 *
 * Used for: setMetadata transactions (method_id = 7)
 *
 * @param $tx_id - Transaction ID (hex string with or without 0x prefix)
 * @return Array of metadata key-value pairs set in the transaction
 *
 * Note: Only one value_* field will be non-null per row
 */
CREATE OR REPLACE ACTION get_transaction_metadata(
    $tx_id TEXT
) PUBLIC VIEW RETURNS TABLE (
    data_provider TEXT,
    stream_id TEXT,
    metadata_key TEXT,
    value_i INT8,
    value_f NUMERIC(36, 18),
    value_b BOOLEAN,
    value_s TEXT,
    value_ref TEXT,
    created_at INT8
) {
    IF $tx_id IS NULL OR trim($tx_id) = '' {
        ERROR('tx_id is required');
    }

    -- Normalize tx_id: remove 0x prefix if present (data tables store without 0x)
    $tx_clean TEXT := LOWER($tx_id);
    IF substring($tx_clean, 1, 2) = '0x' {
        $tx_clean := substring($tx_clean, 3);
    }

    RETURN SELECT
        s.data_provider,
        s.stream_id,
        m.metadata_key,
        m.value_i,
        m.value_f,
        m.value_b,
        m.value_s,
        m.value_ref,
        m.created_at
    FROM metadata m
    JOIN streams s ON m.stream_ref = s.id
    WHERE m.tx_id = $tx_clean
    ORDER BY s.data_provider, s.stream_id, m.metadata_key;
};

/**
 * get_transaction_attestation: Retrieve attestation request details
 *
 * Used for: requestAttestation transactions (method_id = 6)
 *
 * @param $tx_id - Transaction ID (hex string with or without 0x prefix)
 * @return Single attestation record or empty if not found
 *
 * Note: BYTEA fields (attestation_hash, requester, result_canonical, etc.)
 *       are returned as hex-encoded strings
 */
CREATE OR REPLACE ACTION get_transaction_attestation(
    $tx_id TEXT
) PUBLIC VIEW RETURNS TABLE (
    request_tx_id TEXT,
    attestation_hash BYTEA,
    requester BYTEA,
    result_canonical BYTEA,
    encrypt_sig BOOLEAN,
    created_height INT8,
    signature BYTEA,
    validator_pubkey BYTEA,
    signed_height INT8
) {
    IF $tx_id IS NULL OR trim($tx_id) = '' {
        ERROR('tx_id is required');
    }

    -- Normalize tx_id: remove 0x prefix if present (attestations store without 0x)
    $tx_clean TEXT := LOWER($tx_id);
    IF substring($tx_clean, 1, 2) = '0x' {
        $tx_clean := substring($tx_clean, 3);
    }

    RETURN SELECT
        request_tx_id,
        attestation_hash,
        requester,
        result_canonical,
        encrypt_sig,
        created_height,
        signature,
        validator_pubkey,
        signed_height
    FROM attestations
    WHERE request_tx_id = $tx_clean;
};
