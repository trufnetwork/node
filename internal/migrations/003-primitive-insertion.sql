/**
 * insert_record: Adds a new data point to a primitive stream.
 * Validates write permissions and stream existence before insertion.
 */
CREATE OR REPLACE ACTION insert_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $event_time INT8,
    $value NUMERIC(36,18)
) PUBLIC {
    insert_records(
        ARRAY[$data_provider],
        ARRAY[$stream_id],
        ARRAY[$event_time],
        ARRAY[$value]
    );
};


/**
 * insert_records: Adds multiple new data points to a primitive stream in batch.
 * Validates write permissions and stream existence for each record before insertion.
 */
CREATE OR REPLACE ACTION insert_records(
    $data_provider TEXT[],
    $stream_id TEXT[],
    $event_time INT8[],
    $value NUMERIC(36,18)[]
) PUBLIC {
    -- Use helper function to avoid expensive for-loop roundtrips
    $data_provider := helper_lowercase_array($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    $fee_total NUMERIC(78, 0) := 0::NUMERIC(78, 0);
    $fee_recipient TEXT := NULL;
    $leader_hex TEXT := NULL;

    -- Get record count (used for both fee calculation and validation)
    $num_records INT := array_length($data_provider);

    -- ===== FEE COLLECTION WITH ROLE EXEMPTION =====
    -- Check if caller is exempt (has system:network_writer role)
    $is_exempt BOOL := FALSE;
    FOR $row IN are_members_of('system', 'network_writer', ARRAY[$lower_caller]) {
        IF $row.wallet = $lower_caller AND $row.is_member {
            $is_exempt := TRUE;
            BREAK;
        }
    }

    -- Collect fee only from non-exempt wallets (2 TRUF per record)
    IF NOT $is_exempt {
        $fee_per_record := 2000000000000000000::NUMERIC(78, 0); -- 2 TRUF with 18 decimals
        $total_fee := $fee_per_record * $num_records::NUMERIC(78, 0);

        IF @leader_sender IS NULL {
            ERROR('Leader address not available for fee transfer');
        }
        $leader_hex := encode(@leader_sender, 'hex')::TEXT;

        $caller_balance := ethereum_bridge.balance(@caller);

        IF $caller_balance < $total_fee {
            -- Derive human-readable fee from $total_fee
            ERROR('Insufficient balance for write fee. Required: ' || ($total_fee / 1000000000000000000::NUMERIC(78, 0))::TEXT || ' TRUF for ' || $num_records::TEXT || ' record(s)');
        }

        ethereum_bridge.transfer($leader_hex, $total_fee);
        $fee_total := $total_fee;
        $fee_recipient := '0x' || $leader_hex;
    }
    -- ===== END FEE COLLECTION =====
    if $num_records != array_length($stream_id) or $num_records != array_length($event_time) or $num_records != array_length($value) {
        ERROR('array lengths mismatch');
    }

    $current_block INT := @height;

    -- Get stream reference for all streams early for better performance
    $stream_refs := get_stream_ids($data_provider, $stream_id);

    -- Check stream existence using stream refs (handles nulls for non-existent streams)
    if !stream_exists_batch_core($stream_refs) {
        ERROR('one or more streams do not exist');
    }

    -- Check if streams are primitive using stream refs
    if !is_primitive_stream_batch_core($stream_refs) {
        ERROR('one or more streams are not primitive streams');
    }

    -- Validate that the wallet is allowed to write to each stream using stream refs
    if !wallet_write_batch_core($stream_refs, $lower_caller) {
        ERROR('wallet not allowed to write to one or more streams');
    }

    -- Insert all records using UNNEST to expand arrays efficiently
    INSERT INTO primitive_events (event_time, value, created_at, truflation_created_at, stream_ref)
    SELECT
        unnested.event_time,
        unnested.value,
        $current_block,
        NULL,
        unnested.stream_ref
    FROM UNNEST($event_time, $value, $stream_refs) AS unnested(event_time, value, stream_ref)
    WHERE unnested.value != 0::NUMERIC(36,18)
    ORDER BY unnested.stream_ref, unnested.event_time, $current_block;  -- matches (stream_ref, event_time, created_at)

    -- Enqueue days for pruning using helper (idempotent, distinct per day)
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
