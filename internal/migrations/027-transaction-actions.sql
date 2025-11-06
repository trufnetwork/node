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
        -- Ledger rows are idempotent: only create the parent record once even if
        -- record_transaction_event is invoked multiple times within the same tx.
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
        $initial_summary TEXT := '';
        IF $recipient_lower IS NOT NULL AND $amount > 0::NUMERIC(78, 0) {
            -- transaction_event_distributions is the single source of truth for
            -- per-recipient breakdowns; downstream readers aggregate from it.
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
            $initial_summary := $recipient_lower || ':' || $amount::TEXT;
        }

        INSERT INTO transaction_event_summaries (tx_id, fee_distributions)
        VALUES ($tx_id, $initial_summary)
        ON CONFLICT (tx_id) DO NOTHING;
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

    $entry TEXT := $recipient_lower || ':' || $amount_sanitized::TEXT;
    $updated BOOL := FALSE;
    FOR $summary_row IN
        UPDATE transaction_event_summaries
        SET fee_distributions = CASE
            WHEN fee_distributions = '' THEN $entry
            ELSE fee_distributions || ',' || $entry
        END
        WHERE tx_id = $tx_id
        RETURNING tx_id
    {
        $updated := TRUE;
    }

    IF !$updated {
        INSERT INTO transaction_event_summaries (tx_id, fee_distributions)
        VALUES ($tx_id, $entry)
        ON CONFLICT (tx_id) DO NOTHING;
    }
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

    $out_tx_id TEXT := NULL;
    $out_block_height INT8 := 0;
    $out_method TEXT := NULL;
    $out_caller TEXT := NULL;
    $out_fee_amount NUMERIC(78, 0) := 0::NUMERIC(78, 0);
    $out_fee_recipient TEXT := NULL;
    $out_metadata TEXT := NULL;

    FOR $row IN
        SELECT
            te.tx_id,
            te.block_height,
            tm.name AS method,
            te.caller,
            te.fee_amount,
            te.fee_recipient,
            te.metadata
        FROM transaction_events te
        JOIN transaction_methods tm ON tm.method_id = te.method_id
        WHERE te.tx_id = $tx_clean
        LIMIT 1
    {
        $out_tx_id := $row.tx_id;
        $out_block_height := $row.block_height;
        $out_method := $row.method;
        $out_caller := $row.caller;
        $out_fee_amount := $row.fee_amount;
        $out_fee_recipient := $row.fee_recipient;
        $out_metadata := $row.metadata;
    }

    IF $out_tx_id IS NULL {
        RETURN;
    }

    $distribution TEXT := '';
    FOR $dist IN
        SELECT recipient, amount
        FROM transaction_event_distributions
        WHERE tx_id = $tx_clean
        ORDER BY sequence ASC
    {
        $entry TEXT := $dist.recipient || ':' || $dist.amount::TEXT;
        IF $distribution = '' {
            $distribution := $entry;
        } ELSE {
            $distribution := $distribution || ',' || $entry;
        }
    }

    RETURN
        $out_tx_id,
        $out_block_height,
        $out_method,
        $out_caller,
        $out_fee_amount,
        $out_fee_recipient,
        $out_metadata,
        $distribution;
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
    distribution_sequence INT,
    distribution_recipient TEXT,
    distribution_amount NUMERIC(78, 0),
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

    RETURN SELECT
        fe.tx_id,
        fe.block_height,
        fe.method,
        fe.caller,
        fe.fee_amount,
        fe.fee_recipient,
        fe.metadata,
        COALESCE(ted.sequence, 0) AS distribution_sequence,
        ted.recipient AS distribution_recipient,
        ted.amount AS distribution_amount,
        COALESCE(ts.fee_distributions, '') AS fee_distributions
    FROM (
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
            ($mode_normalized = 'paid' AND te.caller = $wallet_lower)
            OR ($mode_normalized = 'received' AND (
                te.fee_recipient = $wallet_lower
                OR EXISTS (
                    SELECT 1
                    FROM transaction_event_distributions ted_inner
                    WHERE ted_inner.tx_id = te.tx_id
                      AND ted_inner.recipient = $wallet_lower
                )
            ))
            OR ($mode_normalized = 'both' AND (
                te.caller = $wallet_lower
                OR te.fee_recipient = $wallet_lower
                OR EXISTS (
                    SELECT 1
                    FROM transaction_event_distributions ted_inner
                    WHERE ted_inner.tx_id = te.tx_id
                      AND ted_inner.recipient = $wallet_lower
                )
            ))
        ORDER BY te.block_height DESC, te.tx_id DESC
        LIMIT $limit_val
        OFFSET $offset_val
    ) fe
    LEFT JOIN transaction_event_distributions ted
        ON ted.tx_id = fe.tx_id
    LEFT JOIN transaction_event_summaries ts
        ON ts.tx_id = fe.tx_id
    ORDER BY fe.block_height DESC,
             fe.tx_id DESC,
             COALESCE(ted.sequence, 0) ASC;
};



CREATE OR REPLACE ACTION get_last_transactions_v2(
    $data_provider TEXT,
    $limit_size   INT8
) PUBLIC VIEW RETURNS TABLE(
    tx_id TEXT,
    created_at INT8,
    method TEXT,
    caller TEXT,
    fee_amount NUMERIC(78, 0),
    fee_recipient TEXT,
    metadata TEXT,
    fee_distributions TEXT
) {
    $normalized_provider TEXT := NULL;
    IF COALESCE($data_provider, '') != '' {
        $normalized_provider := LOWER($data_provider);
        IF NOT check_ethereum_address($normalized_provider) {
            ERROR('Invalid data provider address. Must be a valid Ethereum address: ' || $data_provider);
        }
    }

    $limit_val INT := COALESCE($limit_size, 6);
    IF $limit_val <= 0 {
        $limit_val := 6;
    }

    IF $limit_val > 100 {
        ERROR('Limit size cannot exceed 100');
    }

    $tx_ids TEXT[] := '{}'::TEXT[];
    $block_heights TEXT[] := '{}'::TEXT[];
    $methods TEXT[] := '{}'::TEXT[];
    $callers TEXT[] := '{}'::TEXT[];
    $fee_amounts TEXT[] := '{}'::TEXT[];
    $fee_recipients TEXT[] := '{}'::TEXT[];
    $metadata_values TEXT[] := '{}'::TEXT[];
    $fee_summaries TEXT[] := '{}'::TEXT[];

    FOR $row IN
        SELECT
            te.tx_id,
            te.block_height,
            tm.name AS method,
            te.caller,
            te.fee_amount,
            te.fee_recipient,
            te.metadata
        FROM transaction_events te
        JOIN transaction_methods tm ON tm.method_id = te.method_id
        WHERE $normalized_provider IS NULL OR te.caller = $normalized_provider
        ORDER BY te.block_height DESC, te.tx_id DESC
        LIMIT $limit_val
    {
        $summary TEXT := '';
        FOR $dist IN
            SELECT recipient, amount
            FROM transaction_event_distributions
            WHERE tx_id = $row.tx_id
            ORDER BY sequence ASC
        {
            $entry TEXT := $dist.recipient || ':' || $dist.amount::TEXT;
            IF $summary = '' {
                $summary := $entry;
            } ELSE {
                $summary := $summary || ',' || $entry;
            }
        }

        $tx_ids := array_append($tx_ids, $row.tx_id);
        $block_heights := array_append($block_heights, $row.block_height::TEXT);
        $methods := array_append($methods, $row.method);
        $callers := array_append($callers, LOWER($row.caller));
        $fee_amounts := array_append($fee_amounts, $row.fee_amount::TEXT);
        $fee_recipients := array_append($fee_recipients, COALESCE($row.fee_recipient, ''));
        $metadata_values := array_append($metadata_values, COALESCE($row.metadata, ''));
        $fee_summaries := array_append($fee_summaries, $summary);
    }

    RETURN
    SELECT
        tx_id,
        created_at::INT8,
        method,
        caller,
        fee_amount::NUMERIC(78, 0),
        NULLIF(fee_recipient, ''),
        NULLIF(metadata, ''),
        fee_distributions
    FROM unnest(
        $tx_ids,
        $block_heights,
        $methods,
        $callers,
        $fee_amounts,
        $fee_recipients,
        $metadata_values,
        $fee_summaries
    ) AS entries(tx_id, created_at, method, caller, fee_amount, fee_recipient, metadata, fee_distributions);
};
