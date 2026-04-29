/**
 * 046-allow-zeros-config.sql
 *
 * Per-stream `allow_zeros` config: lets a stream owner opt into
 * persisting `value=0` inserts. Default behavior (no metadata row) is
 * preserved exactly — zeros continue to be dropped on insert and
 * skipped by the prune-day enqueue helper, just like today.
 *
 * Storage: a `metadata` row keyed `allow_zeros` with `value_b=TRUE`
 * means "this stream persists zeros". Absence of a row means default
 * FALSE. We deliberately never store FALSE rows so existing streams
 * need no migration — the default-FALSE LEFT JOIN in `insert_records`
 * and `helper_enqueue_prune_days` resolves missing rows to FALSE and
 * reproduces today's filter bit-identically.
 *
 * The four touched actions (`create_stream`, `create_streams`,
 * `insert_records`, `helper_enqueue_prune_days`) are modified directly
 * in their canonical source files (001-common-actions.sql,
 * 003-primitive-insertion.sql, 901-utilities.sql). This file only
 * defines the genuinely new accessor actions: the owner-gated mutator
 * and a public read.
 */

/**
 * set_allow_zeros: Owner-gated mutator for the per-stream allow_zeros flag.
 *
 * Disables any prior allow_zeros metadata row for this stream and, when
 * $value=TRUE, writes a fresh row marking the stream opt-in. When
 * $value=FALSE, disabling the existing row is sufficient — the implicit
 * default of FALSE then applies, restoring today's filter behavior.
 *
 * The toggle is forward-only: zeros that were dropped before the flip
 * stay dropped; zeros that arrive after the flip persist.
 */
CREATE OR REPLACE ACTION set_allow_zeros(
    $data_provider TEXT,
    $stream_id TEXT,
    $value BOOL
) PUBLIC {
    $data_provider := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);

    if !is_stream_owner($data_provider, $stream_id, $lower_caller) {
        ERROR('Only stream owner can set allow_zeros');
    }

    $stream_ref INT := get_stream_id($data_provider, $stream_id);
    $current_block INT := @height;

    -- Soft-delete prior rows (idempotent — disabling already-disabled is a no-op).
    UPDATE metadata
    SET disabled_at = $current_block
    WHERE stream_ref = $stream_ref
      AND metadata_key = 'allow_zeros'
      AND disabled_at IS NULL;

    if $value = TRUE {
        $row_id UUID := uuid_generate_kwil('allow_zeros' || @txid || $data_provider || $stream_id);
        INSERT INTO metadata (
            row_id, metadata_key, value_i, value_f, value_b, value_s, value_ref, created_at, disabled_at, stream_ref, tx_id
        ) VALUES (
            $row_id,
            'allow_zeros',
            NULL,
            NULL,
            TRUE,
            NULL,
            NULL,
            $current_block,
            NULL,
            $stream_ref,
            @txid
        );
    }
};

/**
 * get_allow_zeros: Public read for the latest allow_zeros flag.
 *
 * Returns TRUE only when the stream has an explicit non-disabled
 * allow_zeros metadata row with value_b=TRUE. Absence (or value_b NULL,
 * or value_b=FALSE) collapses to FALSE — the default-FALSE semantics.
 */
CREATE OR REPLACE ACTION get_allow_zeros(
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns (allow_zeros BOOL) {
    $data_provider := LOWER($data_provider);
    $stream_ref INT := get_stream_id($data_provider, $stream_id);

    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Avoid COALESCE inside the FOR loop body (Kuneiform forbids
    -- function calls in nested queries). Test the raw value_b directly.
    $result BOOL := FALSE;
    for $row in SELECT value_b
        FROM metadata
        WHERE stream_ref = $stream_ref
          AND metadata_key = 'allow_zeros'
          AND disabled_at IS NULL
        ORDER BY created_at DESC, row_id DESC
        LIMIT 1 {
        if $row.value_b = TRUE {
            $result := TRUE;
        }
    }

    RETURN $result;
};
