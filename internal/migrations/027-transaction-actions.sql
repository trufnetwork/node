/**
 * =============================================================================
 * Transaction Ledger Helper Actions
 * =============================================================================
 */

CREATE OR REPLACE ACTION record_transaction_event(
    -- check internal/migrations/026-transaction-schemas.sql for method_id values
    $method_id INT,
    $fee_amount NUMERIC(78, 0),
    $fee_recipient TEXT,
    $metadata TEXT
) PRIVATE {
    IF $method_id IS NULL {
        ERROR('record_transaction_event: method_id is required');
    }

    IF @txid IS NULL {
        ERROR('record_transaction_event: missing transaction id');
    }

    $tx_hex TEXT := LOWER(@txid);
    IF LENGTH($tx_hex) != 64 {
        ERROR('record_transaction_event: txid must be 32-byte hex string');
    }
    $tx_id TEXT := '0x' || $tx_hex;

    $caller_lower TEXT := LOWER(@caller);
    IF NOT check_ethereum_address($caller_lower) {
        ERROR('record_transaction_event: caller is not a valid address: ' || $caller_lower);
    }

    $recipient_lower TEXT := NULL;
    IF $fee_recipient IS NOT NULL AND $fee_recipient != '' {
        $recipient_lower := LOWER($fee_recipient);
        IF NOT check_ethereum_address($recipient_lower) {
            ERROR('record_transaction_event: fee_recipient is not a valid address: ' || $fee_recipient);
        }
    }

    $amount NUMERIC(78, 0) := COALESCE($fee_amount, 0::NUMERIC(78, 0));

    $event_exists BOOL := FALSE;
    FOR $row IN
        SELECT 1
        FROM transaction_events
        WHERE tx_id = $tx_id
        LIMIT 1
    {
        $event_exists := TRUE;
    }

    IF !$event_exists {
        INSERT INTO transaction_events (
            tx_id,
            block_height,
            method_id,
            caller,
            fee_amount,
            fee_recipient,
            metadata
        ) VALUES (
            $tx_id,
            @height,
            $method_id,
            $caller_lower,
            $amount,
            $recipient_lower,
            $metadata
        );
        IF $recipient_lower IS NOT NULL AND $amount > 0::NUMERIC(78, 0) {
            INSERT INTO transaction_event_distributions (
                tx_id,
                sequence,
                recipient,
                amount,
                note
            ) VALUES (
                $tx_id,
                1,
                $recipient_lower,
                $amount,
                NULL
            );
        }
    }
};

CREATE OR REPLACE ACTION append_fee_distribution(
    $recipient TEXT,
    $amount NUMERIC(78, 0)
) PRIVATE {
    IF @txid IS NULL {
        ERROR('append_fee_distribution: missing transaction id');
    }

    IF $recipient IS NULL {
        ERROR('append_fee_distribution: recipient is required');
    }

    $recipient_lower TEXT := LOWER($recipient);
    IF NOT check_ethereum_address($recipient_lower) {
        ERROR('append_fee_distribution: invalid recipient address: ' || $recipient);
    }

    $tx_hex TEXT := LOWER(@txid);
    IF LENGTH($tx_hex) != 64 {
        ERROR('append_fee_distribution: txid must be 32-byte hex string');
    }
    $tx_id TEXT := '0x' || $tx_hex;

    $amount_sanitized NUMERIC(78, 0) := COALESCE($amount, 0::NUMERIC(78, 0));

    $next_sequence INT := 1;
    FOR $row IN
        SELECT COALESCE(MAX(sequence), 0) + 1 AS seq
        FROM transaction_event_distributions
        WHERE tx_id = $tx_id
    {
        $next_sequence := $row.seq;
    }

    INSERT INTO transaction_event_distributions (
        tx_id,
        sequence,
        recipient,
        amount,
        note
    ) VALUES (
        $tx_id,
        $next_sequence,
        $recipient_lower,
        $amount_sanitized,
        NULL
    );

};

CREATE OR REPLACE ACTION get_transaction_event(
    $tx_id TEXT
) PUBLIC VIEW RETURNS (
    tx_id TEXT,
    block_height INT8,
    method TEXT,
    caller TEXT,
    fee_amount NUMERIC(78, 0),
    fee_recipient TEXT,
    metadata TEXT,
    fee_distributions TEXT
) {
    IF $tx_id IS NULL {
        ERROR('tx_id is required');
    }

    $tx_clean TEXT := LOWER($tx_id);
    IF substring($tx_clean, 1, 2) != '0x' {
        $tx_clean := '0x' || $tx_clean;
    }

    RETURN
    SELECT
        te.tx_id,
        te.block_height,
        tm.name,
        te.caller,
        te.fee_amount,
        te.fee_recipient,
        te.metadata,
        COALESCE(d.fee_distributions, '') AS fee_distributions
    FROM transaction_events te
    JOIN transaction_methods tm ON tm.method_id = te.method_id
    LEFT JOIN (
        SELECT
            tx_id,
            string_agg(
                recipient || ':' || amount::TEXT,
                ',' ORDER BY sequence ASC
            ) AS fee_distributions
        FROM transaction_event_distributions
        GROUP BY tx_id
    ) d ON d.tx_id = te.tx_id
    WHERE te.tx_id = $tx_clean
    LIMIT 1;
};

CREATE OR REPLACE ACTION list_transaction_fees(
    $wallet TEXT,
    $mode TEXT DEFAULT 'paid',
    $limit INT DEFAULT 20,
    $offset INT DEFAULT 0
) PUBLIC VIEW RETURNS TABLE (
    tx_id TEXT,
    block_height INT8,
    method TEXT,
    caller TEXT,
    total_fee NUMERIC(78, 0),
    fee_recipient TEXT,
    metadata TEXT,
    fee_distributions TEXT
) {
    IF $wallet IS NULL OR trim($wallet) = '' {
        ERROR('wallet is required');
    }

    $wallet_lower TEXT := LOWER($wallet);
    IF NOT check_ethereum_address($wallet_lower) {
        ERROR('wallet must be a valid Ethereum address: ' || $wallet);
    }

    $mode_normalized TEXT := LOWER(COALESCE($mode, 'paid'));
    IF $mode_normalized != 'paid' AND $mode_normalized != 'received' AND $mode_normalized != 'both' {
        ERROR('mode must be one of paid, received, or both');
    }

    $limit_val INT := COALESCE($limit, 20);
    IF $limit_val <= 0 {
        $limit_val := 20;
    }
    IF $limit_val > 1000 {
        ERROR('limit cannot exceed 1000');
    }

    $offset_val INT := COALESCE($offset, 0);
    IF $offset_val < 0 {
        $offset_val := 0;
    }

    RETURN
    WITH distributions AS (
        SELECT
            tx_id,
            string_agg(
                recipient || ':' || amount::TEXT,
                ',' ORDER BY sequence ASC
            ) AS fee_distributions
        FROM transaction_event_distributions
        GROUP BY tx_id
    ),
    filtered AS (
        SELECT
            te.tx_id,
            te.block_height,
            tm.name AS method,
            te.caller,
            te.fee_amount,
            te.fee_recipient,
            te.metadata,
            COALESCE(d.fee_distributions, '') AS fee_distributions
        FROM transaction_events te
        JOIN transaction_methods tm ON tm.method_id = te.method_id
        LEFT JOIN distributions d ON d.tx_id = te.tx_id
        WHERE
            ($mode_normalized = 'paid' AND te.caller = $wallet_lower)
            OR ($mode_normalized = 'received' AND (
                te.fee_recipient = $wallet_lower
                OR EXISTS (
                    SELECT 1
                    FROM transaction_event_distributions ted
                    WHERE ted.tx_id = te.tx_id
                      AND ted.recipient = $wallet_lower
                )
            ))
            OR ($mode_normalized = 'both' AND (
                te.caller = $wallet_lower
                OR te.fee_recipient = $wallet_lower
                OR EXISTS (
                    SELECT 1
                    FROM transaction_event_distributions ted
                    WHERE ted.tx_id = te.tx_id
                      AND ted.recipient = $wallet_lower
                )
            ))
    )
    SELECT
        tx_id,
        block_height,
        method,
        caller,
        fee_amount AS total_fee,
        fee_recipient,
        metadata,
        fee_distributions
    FROM filtered
    ORDER BY block_height DESC, tx_id DESC
    LIMIT $limit_val
    OFFSET $offset_val;
};
