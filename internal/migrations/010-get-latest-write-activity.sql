/**
 * Transaction history views
 *
 * get_last_transactions_v1  - legacy implementation (no fee/caller metadata)
 * get_last_transactions_v2  - ledger-backed implementation (redefined later in migration 027)
 * get_last_transactions     - temporary wrapper returning the v2 signature but
 *                             still sourcing data from v1. This will be replaced
 *                             with v2 once callers migrate.
 */

-- This is a legacy implementation that is no longer used. It will be removed in a future migration.
CREATE OR REPLACE ACTION get_last_transactions_v1(
    $data_provider TEXT,
    $limit_size   INT8
) PUBLIC VIEW RETURNS TABLE(
    created_at INT8,
    method     TEXT
) {
    $normalized_provider TEXT := NULL;
    IF COALESCE($data_provider, '') != '' {
        $normalized_provider := LOWER($data_provider);
    }

    IF $limit_size IS NULL {
        $limit_size := 6;
    }
    IF $limit_size <= 0 {
        $limit_size := 6;
    }

    IF $limit_size > 100 {
        ERROR('Limit size cannot exceed 100');
    }

    RETURN SELECT created_at, method FROM (
      SELECT created_at, method, ROW_NUMBER() OVER (PARTITION BY created_at ORDER BY priority ASC) AS rn FROM (
          SELECT s.created_at, 'deployStream' AS method, 1 AS priority
          FROM (
              SELECT DISTINCT s.created_at
              FROM streams s
              JOIN data_providers dp ON s.data_provider_id = dp.id
              WHERE COALESCE($normalized_provider, '') = '' OR dp.address = $normalized_provider
              ORDER BY s.created_at DESC
              LIMIT $limit_size
          ) s
          UNION ALL
          SELECT pe.created_at, 'insertRecords', 2
          FROM (
              SELECT DISTINCT pe.created_at
              FROM primitive_events pe
              JOIN streams s ON pe.stream_ref = s.id
              JOIN data_providers dp ON s.data_provider_id = dp.id
              WHERE COALESCE($normalized_provider, '') = '' OR dp.address = $normalized_provider
              ORDER BY pe.created_at DESC
              LIMIT $limit_size
          ) pe
          UNION ALL
          SELECT t.created_at, 'setTaxonomies', 3
          FROM (
              SELECT DISTINCT t.created_at
              FROM taxonomies t
              JOIN streams s ON t.stream_ref = s.id
              JOIN data_providers dp ON s.data_provider_id = dp.id
              WHERE COALESCE($normalized_provider, '') = '' OR dp.address = $normalized_provider
              ORDER BY t.created_at DESC
              LIMIT $limit_size
          ) t
          UNION ALL
          SELECT m.created_at, 'setMetadata', 4
          FROM (
              SELECT DISTINCT m.created_at
              FROM metadata m
              JOIN streams s ON m.stream_ref = s.id
              JOIN data_providers dp ON s.data_provider_id = dp.id
              WHERE COALESCE($normalized_provider, '') = '' OR dp.address = $normalized_provider
              ORDER BY m.created_at DESC
              LIMIT $limit_size
          ) m
      ) AS combined
  ) AS ranked
  WHERE rn = 1
  ORDER BY created_at DESC
  LIMIT $limit_size;
};

CREATE OR REPLACE ACTION get_last_transactions_v2(
    $wallet TEXT,
    $limit_size   INT8
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
    IF COALESCE($wallet, '') != '' {
        $normalized_wallet := LOWER($wallet);
        IF NOT check_ethereum_address($normalized_wallet) {
            ERROR('Invalid wallet. Must be a valid Ethereum address: ' || $wallet);
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

CREATE OR REPLACE ACTION get_last_transactions(
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

    RETURN
    SELECT
        NULL::TEXT AS tx_id,
        lt.created_at,
        lt.method,
        NULL::TEXT AS caller,
        NULL::NUMERIC(78, 0) AS fee_amount,
        NULL::TEXT AS fee_recipient,
        NULL::TEXT AS metadata,
        ''::TEXT AS fee_distributions
    FROM get_last_transactions_v1($normalized_provider, $limit_val) lt;
};
