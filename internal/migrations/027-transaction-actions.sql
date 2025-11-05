/**
 * =============================================================================
 * Transaction Ledger Helper Actions
 * =============================================================================
 */

CREATE OR REPLACE ACTION record_transaction_event(
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
};

/**
 * append_fee_distribution: Adds an additional fee recipient for the current transaction.
 */
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

/**
 * get_transaction_event: Fetches a single transaction ledger entry by tx hash.
 * Accepts tx id with or without 0x prefix.
 */
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
