-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/009-truflation-query.sql
-- Script : scripts/generate_prod_migrations.py
--
-- Manual-apply mainnet override. The embedded migration loader skips
-- *.prod.sql, so apply via:
--
--     kwil-cli exec-sql --file <this file> --sync \
--         --private-key $PRIVATE_KEY --provider $PROVIDER
--
-- Prerequisite: erc20-bridge/000-extension.prod.sql must be applied
-- FIRST so the eth_truf and eth_usdc bridge instances exist.
-- =============================================================================

CREATE OR REPLACE ACTION truflation_insert_records(
    $data_provider TEXT[],
    $stream_id TEXT[],
    $event_time INT8[],
    $value NUMERIC(36,18)[],
    $truflation_created_at TEXT[]
) PUBLIC {
    -- Use helper function to avoid expensive for-loop roundtrips
    $data_providers := helper_lowercase_array($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $fee_total NUMERIC(78, 0) := 0::NUMERIC(78, 0);
    $fee_recipient TEXT := NULL;
    $leader_hex TEXT := NULL;
    $num_records INT := array_length($data_provider);
    if $num_records != array_length($stream_id) or $num_records != array_length($event_time) or $num_records != array_length($value) or $num_records != array_length($truflation_created_at) {
        ERROR('array lengths mismatch');
    }

    -- ===== FEE COLLECTION =====
    -- Flat 1 TRUF per transaction (write-fee policy per issue #3805).
    -- Charged universally — no role gate. Every caller pays the flat
    -- per-tx fee regardless of role membership.
    $total_fee := 1000000000000000000::NUMERIC(78, 0); -- 1 TRUF with 18 decimals

    IF @leader_sender IS NULL {
        ERROR('Leader address not available for fee transfer');
    }
    $leader_hex := encode(@leader_sender, 'hex')::TEXT;

    $caller_balance := eth_truf.balance(@caller);

    IF $caller_balance < $total_fee {
        ERROR('Insufficient balance for write fee. Required: 1 TRUF');
    }

    eth_truf.transfer($leader_hex, $total_fee);
    $fee_total := $total_fee;
    $fee_recipient := '0x' || $leader_hex;
    -- ===== END FEE COLLECTION =====

    $current_block INT := @height;

    -- Get stream reference for all streams (get_stream_ids returns NULL for non-existent streams)
    $stream_refs := get_stream_ids($data_providers, $stream_id);

    -- Check stream existence using stream refs (NULL values indicate non-existent streams)
    for $i in 1..array_length($stream_refs) {
        if $stream_refs[$i] IS NULL {
            ERROR('stream does not exist: data_provider=' || $data_provider[$i] || ', stream_id=' || $stream_id[$i]);
        }
    }

    -- Check if streams are primitive in batch
    for $row in is_primitive_stream_batch($data_providers, $stream_id) {
        if !$row.is_primitive {
            ERROR('stream is not a primitive stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Validate that the wallet is allowed to write to each stream
    for $row in is_wallet_allowed_to_write_batch($data_providers, $stream_id, $lower_caller) {
        if !$row.is_allowed {
            ERROR('wallet not allowed to write to stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Insert all records using WITH RECURSIVE pattern to avoid round trips
    WITH RECURSIVE
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < $num_records
    ),
    record_arrays AS (
        SELECT
            $event_time AS event_times,
            $value AS values_array,
            $truflation_created_at AS truflation_created_at_array,
            $stream_refs AS stream_refs_array
    ),
    arguments AS (
        SELECT
            record_arrays.event_times[idx] AS event_time,
            record_arrays.values_array[idx] AS value,
            record_arrays.truflation_created_at_array[idx] AS truflation_created_at,
            record_arrays.stream_refs_array[idx] AS stream_ref
        FROM indexes
        JOIN record_arrays ON 1=1
    )
    INSERT INTO primitive_events (event_time, value, created_at, truflation_created_at, stream_ref)
    SELECT
        event_time,
        value,
        $current_block,
        truflation_created_at,
        stream_ref
    FROM arguments;

    -- Enqueue days for pruning (idempotent, distinct), filtering out zero values
    helper_enqueue_prune_days(
        $stream_refs,
        $event_time,
        $value
    );

    record_transaction_event(
        2,
        $fee_total,
        $fee_recipient,
        NULL
    );
};
