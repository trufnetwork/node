/**
 * validate_streams_for_write: One-shot replacement for the four-query
 * validation gauntlet that `insert_records` currently runs (`get_stream_ids` +
 * `stream_exists_batch_core` + `is_primitive_stream_batch_core` +
 * `wallet_write_batch_core`). All four hit the same `streams` and `metadata`
 * tables on the same input arrays, so we collapse them into a single SQL
 * pass that resolves the refs, joins owner + write-permission once per ref,
 * and reports four facts in one row.
 *
 * Returned columns:
 *   stream_refs   — array of stream IDs in input order; NULL for
 *                   (data_provider, stream_id) pairs that don't resolve.
 *                   Caller can reuse this directly without re-running
 *                   get_stream_ids.
 *   all_exist     — TRUE iff every input pair resolved to a streams.id.
 *   all_primitive — TRUE iff every resolved stream has stream_type='primitive'.
 *                   Vacuously TRUE when a ref is NULL; the all_exist check
 *                   is what catches missing streams.
 *   all_writable  — TRUE iff for every resolved stream the wallet is the
 *                   current `stream_owner` OR has an active
 *                   `allow_write_wallet` metadata entry. Vacuously TRUE when
 *                   a ref is NULL.
 *
 * The all_primitive / all_writable flags being vacuously TRUE on missing
 * streams is intentional: callers must check all_exist first and ERROR on
 * its failure, so the primitive/writable flags only matter for refs that
 * actually resolved. This avoids a redundant NULL-rejection inside the
 * aggregates and keeps the single-SELECT shape clean.
 *
 * Consumed by `insert_records` (003-primitive-insertion.sql). Replacing the
 * four prior calls with one call cuts ~3 postgres round-trips per insert tx
 * (~150 ms each at observed mainnet latency); that's the largest single
 * lever available against per-tx execution time at the fixed batch=10 cap.
 */
CREATE OR REPLACE ACTION validate_streams_for_write(
    $data_providers TEXT[],
    $stream_ids TEXT[],
    $wallet TEXT
) PUBLIC VIEW RETURNS TABLE(
    stream_refs INT[],
    all_exist BOOL,
    all_primitive BOOL,
    all_writable BOOL
) {
    -- Caller is expected to have already validated array-length consistency
    -- (insert_records does this), but be defensive: matching get_stream_ids'
    -- behavior keeps callers' error messages clear.
    IF COALESCE(array_length($data_providers), 0) != COALESCE(array_length($stream_ids), 0) {
        ERROR(
            'array lengths mismatch: data_providers='
            || COALESCE(array_length($data_providers), 0)::TEXT
            || ', stream_ids='
            || COALESCE(array_length($stream_ids), 0)::TEXT
        );
    }

    $lower_wallet TEXT := LOWER($wallet);

    -- Single SELECT: resolve refs and report the four facts in one pass.
    --
    -- The CTE shape mirrors get_stream_ids (idx + joined) so the resolved
    -- refs come back in input order via array_agg(... ORDER BY idx).
    --
    -- Each fact uses NOT EXISTS over the same `joined` CTE rather than a
    -- bool_and aggregate. Kuneiform's deterministic SQL subset doesn't
    -- support bool_and; the NOT EXISTS form is what the existing
    -- *_batch_core helpers in 001-common-actions.sql / 002-authorization.sql
    -- already use.
    RETURN
    WITH idx AS (
        SELECT
            ord AS idx,
            LOWER(dp) AS dp,
            sid AS sid
        FROM unnest($data_providers, $stream_ids) WITH ORDINALITY AS u(dp, sid, ord)
    ),
    joined AS (
        SELECT i.idx, s.id AS stream_ref, s.stream_type
        FROM idx i
        LEFT JOIN data_providers d ON d.address = i.dp
        LEFT JOIN streams s
          ON s.data_provider_id = d.id
         AND s.stream_id = i.sid
    ),
    -- Refs we actually need to look up metadata for. Used to scope the
    -- stream_owner / allow_write_wallet scans below to just this batch
    -- instead of letting them scan every matching metadata row in the table.
    -- Without this, the latest_owner ROW_NUMBER() partitioned across every
    -- stream_owner row across all streams (with ~50k streams in mainnet,
    -- that's ~50k index entries scanned per insert tx).
    refs AS (
        SELECT DISTINCT stream_ref
        FROM joined
        WHERE stream_ref IS NOT NULL
    ),
    -- One row per resolved stream_ref: the most recent stream_owner.
    -- meta_stream_to_key_idx (stream_ref, metadata_key, created_at) keys this
    -- well once we restrict m.stream_ref to the batch via the JOIN to refs.
    latest_owner AS (
        SELECT stream_ref, owner FROM (
            SELECT
                m.stream_ref,
                m.value_ref AS owner,
                ROW_NUMBER() OVER (
                    PARTITION BY m.stream_ref
                    ORDER BY m.created_at DESC, m.row_id DESC
                ) AS rn
            FROM metadata m
            JOIN refs r ON r.stream_ref = m.stream_ref
            WHERE m.metadata_key = 'stream_owner'
              AND m.disabled_at IS NULL
        ) ranked WHERE rn = 1
    ),
    -- Resolved stream_refs the wallet has been explicitly granted write on.
    -- Already index-bounded by meta_key_ref_to_stream_idx, but we still scope
    -- to refs for consistency.
    allow_perms AS (
        SELECT DISTINCT m.stream_ref
        FROM metadata m
        JOIN refs r ON r.stream_ref = m.stream_ref
        WHERE m.metadata_key = 'allow_write_wallet'
          AND m.disabled_at IS NULL
          AND m.value_ref = $lower_wallet
    )
    SELECT
        COALESCE(array_agg(j.stream_ref ORDER BY j.idx), ARRAY[]::INT[]) AS stream_refs,
        NOT EXISTS(
            SELECT 1 FROM joined WHERE stream_ref IS NULL
        ) AS all_exist,
        NOT EXISTS(
            SELECT 1 FROM joined
            WHERE stream_ref IS NOT NULL
              AND stream_type != 'primitive'
        ) AS all_primitive,
        NOT EXISTS(
            -- A stream is writable iff caller is owner OR has an active
            -- allow_write_wallet metadata entry. This subquery finds any
            -- resolved stream that fails BOTH checks.
            SELECT 1
            FROM joined jw
            LEFT JOIN latest_owner o ON o.stream_ref = jw.stream_ref
            LEFT JOIN allow_perms ap ON ap.stream_ref = jw.stream_ref
            WHERE jw.stream_ref IS NOT NULL
              AND NOT (o.owner = $lower_wallet OR ap.stream_ref IS NOT NULL)
        ) AS all_writable
    FROM joined j;
};
