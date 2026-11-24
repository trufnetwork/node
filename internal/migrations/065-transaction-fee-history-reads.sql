/*
 * MIGRATION 065: FEE HISTORY READS BY WALLET
 *
 * list_transaction_fees (027) and get_last_transactions (010) find a wallet's
 * fee events with one filter that ORs the caller test with the fee recipient
 * test. No index answers that filter, so Postgres reads
 * transaction_events_block_idx from the newest row and tests every row until
 * it has a page. For a wallet with fewer fee rows than the page asks for, that
 * read covers the whole table.
 *
 * Here each test is its own branch. A branch reads the keys (tx_id,
 * block_height) of the wallet's events from one index, newest first, and stops
 * after the rows the page can use:
 *
 *   caller = wallet          transaction_events_caller_idx (026)
 *   fee_recipient = wallet   transaction_events_fee_recipient_idx (below)
 *
 * paid reads the caller branch, received the fee recipient branch, and both
 * the UNION of the two. The page is cut from those keys in the order
 * (block_height DESC, tx_id DESC), and only the page's events are then read
 * from the table by tx_id. Each index holds every column a branch reads, so
 * Postgres can answer a branch from the index alone. The distribution rows are
 * joined to the page as before, so every mode returns the same rows in the
 * same order as the 027 and 010 definitions.
 *
 * Distribution recipients. The previous filters also matched an event whose
 * transaction_event_distributions rows name the wallet. The only writer of
 * that table, record_transaction_event (027), writes at most one row for an
 * event, and that row's recipient is the event's fee_recipient, so that test
 * matched no event the fee recipient test did not. It is dropped. A writer
 * that splits a fee among other recipients needs its own indexed branch here.
 *
 * The index is built when this file is applied, over every row the table holds.
 *
 * References no bridge namespace, so there is no dev/prod twin.
 */

CREATE INDEX IF NOT EXISTS transaction_events_fee_recipient_idx
    ON transaction_events (fee_recipient, block_height, tx_id);

-- =============================================================================
-- list_transaction_fees: a wallet's fee events, newest first.
-- =============================================================================
/**
 * Same parameters, columns, order and paging as the 027 definition. One row per
 * fee distribution; an event without distributions returns one row with NULL
 * distribution fields. In received mode only the wallet's own distribution row
 * is returned. $limit and $offset count events, not rows.
 *
 * Usage:
 *   kwil-cli call-action list_transaction_fees text:0x... text:both int:20 int:0
 */
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
    distribution_amount NUMERIC(78, 0)
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

    $mode_is_received BOOL := $mode_normalized = 'received';
    -- paid and both read the caller branch; received and both the fee
    -- recipient branch. A branch whose flag is false reads nothing.
    $read_caller BOOL := $mode_normalized != 'received';
    $read_recipient BOOL := $mode_normalized != 'paid';
    -- The page ends at row offset + limit of the union, so neither branch
    -- needs more rows than that. INT addition wraps, so a sum past the INT
    -- maximum reads the whole branch instead.
    $branch_limit INT := 9223372036854775807;
    IF $offset_val <= 9223372036854775807 - $limit_val {
        $branch_limit := $limit_val + $offset_val;
    }

    RETURN SELECT
        te.tx_id,
        te.block_height,
        tm.name AS method,
        LOWER(te.caller) AS caller,
        te.fee_amount,
        te.fee_recipient,
        te.metadata,
        COALESCE(ted.sequence, 0) AS distribution_sequence,
        ted.recipient AS distribution_recipient,
        ted.amount AS distribution_amount
    FROM (
        SELECT * FROM (
            SELECT tx_id, block_height
            FROM transaction_events
            WHERE $read_caller AND caller = $wallet_lower
            ORDER BY block_height DESC, tx_id DESC
            LIMIT $branch_limit
        ) by_caller
        UNION
        SELECT * FROM (
            SELECT tx_id, block_height
            FROM transaction_events
            WHERE $read_recipient AND fee_recipient = $wallet_lower
            ORDER BY block_height DESC, tx_id DESC
            LIMIT $branch_limit
        ) by_recipient
        ORDER BY block_height DESC, tx_id DESC
        LIMIT $limit_val
        OFFSET $offset_val
    ) page
    JOIN transaction_events te ON te.tx_id = page.tx_id
    JOIN transaction_methods tm ON tm.method_id = te.method_id
    LEFT JOIN (
        SELECT
            tx_id,
            sequence,
            recipient,
            amount
        FROM transaction_event_distributions
        WHERE
            NOT $mode_is_received
            OR recipient = $wallet_lower
    ) ted
        ON ted.tx_id = te.tx_id
    ORDER BY te.block_height DESC,
             te.tx_id DESC,
             COALESCE(ted.sequence, 0) ASC;
};

-- =============================================================================
-- get_last_transactions: the newest fee events, for a wallet or for everyone.
-- =============================================================================
/**
 * Same parameters, columns and order as the 010 definition. A wallet matches
 * the events it paid and the events it received the fee for. With no wallet,
 * every event counts. One row per fee distribution, as before.
 *
 * Usage:
 *   kwil-cli call-action get_last_transactions text:0x... int:6
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

    IF $normalized_wallet IS NULL {
        RETURN SELECT
            te.tx_id,
            te.block_height,
            tm.name AS method,
            LOWER(te.caller) AS caller,
            te.fee_amount,
            te.fee_recipient,
            te.metadata,
            COALESCE(ted.sequence, 0) AS distribution_sequence,
            ted.recipient AS distribution_recipient,
            ted.amount AS distribution_amount
        FROM (
            SELECT tx_id, block_height
            FROM transaction_events
            ORDER BY block_height DESC, tx_id DESC
            LIMIT $limit_val
        ) page
        JOIN transaction_events te ON te.tx_id = page.tx_id
        JOIN transaction_methods tm ON tm.method_id = te.method_id
        LEFT JOIN transaction_event_distributions ted
            ON ted.tx_id = te.tx_id
        ORDER BY te.block_height DESC,
                 te.tx_id DESC,
                 COALESCE(ted.sequence, 0) ASC;
    } ELSE {
        RETURN SELECT
            te.tx_id,
            te.block_height,
            tm.name AS method,
            LOWER(te.caller) AS caller,
            te.fee_amount,
            te.fee_recipient,
            te.metadata,
            COALESCE(ted.sequence, 0) AS distribution_sequence,
            ted.recipient AS distribution_recipient,
            ted.amount AS distribution_amount
        FROM (
            SELECT * FROM (
                SELECT tx_id, block_height
                FROM transaction_events
                WHERE caller = $normalized_wallet
                ORDER BY block_height DESC, tx_id DESC
                LIMIT $limit_val
            ) by_caller
            UNION
            SELECT * FROM (
                SELECT tx_id, block_height
                FROM transaction_events
                WHERE fee_recipient = $normalized_wallet
                ORDER BY block_height DESC, tx_id DESC
                LIMIT $limit_val
            ) by_recipient
            ORDER BY block_height DESC, tx_id DESC
            LIMIT $limit_val
        ) page
        JOIN transaction_events te ON te.tx_id = page.tx_id
        JOIN transaction_methods tm ON tm.method_id = te.method_id
        LEFT JOIN transaction_event_distributions ted
            ON ted.tx_id = te.tx_id
        ORDER BY te.block_height DESC,
                 te.tx_id DESC,
                 COALESCE(ted.sequence, 0) ASC;
    }
};
