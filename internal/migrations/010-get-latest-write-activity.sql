/**
 * Transaction history view
 *
 * `get_last_transactions` returns the most recent ledger-backed transactions,
 * optionally filtered to those involving a given wallet (caller, fee_recipient,
 * or distribution recipient). One row per fee distribution; transactions
 * without distributions return a single row with NULL distribution fields.
 *
 * Parameter name `$data_provider` is preserved from the previous wrapper
 * signature for SDK backward compatibility. The earlier `_v1` and `_v2`
 * variants have been retired — see `maintenance/` for the operational DROP
 * script that removes them from already-migrated databases.
 */

CREATE OR REPLACE ACTION get_last_transactions(
    $data_provider TEXT,
    $limit_size    INT8
) PUBLIC VIEW RETURNS TABLE(
    tx_id TEXT,
    created_at INT8,
    method TEXT,
    caller TEXT,
    fee_amount NUMERIC(78, 0),
    fee_recipient TEXT,
    metadata TEXT,
    distribution_sequence INT,
    distribution_recipient TEXT,
    distribution_amount NUMERIC(78, 0)
) {
    $normalized_wallet TEXT := NULL;
    IF COALESCE($data_provider, '') != '' {
        $normalized_wallet := LOWER($data_provider);
        IF NOT check_ethereum_address($normalized_wallet) {
            ERROR('Invalid wallet. Must be a valid Ethereum address: ' || $data_provider);
        }
    }

    $limit_val INT := COALESCE($limit_size, 6);
    IF $limit_val <= 0 {
        $limit_val := 6;
    }

    IF $limit_val > 100 {
        ERROR('Limit size cannot exceed 100');
    }

    RETURN
    WITH limited_events AS (
        SELECT
            te.tx_id,
            te.block_height,
            tm.name AS method,
            LOWER(te.caller) AS caller,
            te.fee_amount,
            te.fee_recipient,
            te.metadata
        FROM transaction_events te
        JOIN transaction_methods tm ON tm.method_id = te.method_id
        WHERE
            $normalized_wallet IS NULL
            OR te.caller = $normalized_wallet
            OR (te.fee_recipient IS NOT NULL AND te.fee_recipient = $normalized_wallet)
            OR EXISTS (
                SELECT 1
                FROM transaction_event_distributions ted_inner
                WHERE ted_inner.tx_id = te.tx_id
                  AND ted_inner.recipient = $normalized_wallet
            )
        ORDER BY te.block_height DESC, te.tx_id DESC
        LIMIT $limit_val
    )
    SELECT
        le.tx_id,
        le.block_height,
        le.method,
        le.caller,
        le.fee_amount,
        le.fee_recipient,
        le.metadata,
        COALESCE(ted.sequence, 0) AS distribution_sequence,
        ted.recipient AS distribution_recipient,
        ted.amount AS distribution_amount
    FROM limited_events le
    LEFT JOIN transaction_event_distributions ted
        ON ted.tx_id = le.tx_id
    ORDER BY le.block_height DESC,
             le.tx_id DESC,
             COALESCE(ted.sequence, 0) ASC;
};
