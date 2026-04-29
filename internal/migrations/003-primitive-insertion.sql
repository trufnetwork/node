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

    -- Cap batch size to prevent superlinear block execution time.
    -- With per-tx PG isolation, blocks handle thousands of small txns efficiently.
    -- TODO: Commented out for now to allow larger batch sizes, MUST be re-enabled once ingestors are updated!!!
    -- if $num_records > 10 {
    --     ERROR('insert_records: batch size exceeds maximum of 10 records');
    -- }

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

    -- One-shot validation: resolves stream refs + checks existence, primitive
    -- type, and wallet write-auth in a single SQL pass.  Replaces four
    -- separate calls that each round-tripped to postgres on the same arrays.
    -- See 002-validate-streams-for-write.sql for rationale and shape.
    $stream_refs INT[];
    for $v in validate_streams_for_write($data_provider, $stream_id, $lower_caller) {
        if !$v.all_exist {
            ERROR('one or more streams do not exist');
        }
        if !$v.all_primitive {
            ERROR('one or more streams are not primitive streams');
        }
        if !$v.all_writable {
            ERROR('wallet not allowed to write to one or more streams');
        }
        $stream_refs := $v.stream_refs;
    }

    -- Insert all records using UNNEST to expand arrays efficiently.
    -- The per-stream `allow_zeros` flag (default FALSE — no metadata row)
    -- gates the value=0 drop. Streams without an explicit `allow_zeros`
    -- metadata row inherit the default FALSE, preserving today's behavior.
    -- Streams opted in via `allow_zeros=TRUE` persist value=0 records.
    INSERT INTO primitive_events (event_time, value, created_at, truflation_created_at, stream_ref, tx_id)
    SELECT
        unnested.event_time,
        unnested.value,
        $current_block,
        NULL,
        unnested.stream_ref,
        @txid
    FROM UNNEST($event_time, $value, $stream_refs) AS unnested(event_time, value, stream_ref)
    LEFT JOIN (
        -- Latest non-disabled allow_zeros row per stream in this batch.
        -- Deterministic tiebreaker (created_at DESC, row_id DESC) is required
        -- to keep the action AppHash-stable across nodes. The inner JOIN
        -- against UNNEST scopes the metadata scan to the batch's distinct
        -- stream_refs so insert latency stays bounded by batch size, not
        -- by the global allow_zeros row count.
        SELECT stream_ref, value_b AS allow_zeros FROM (
            SELECT
                md.stream_ref,
                md.value_b,
                ROW_NUMBER() OVER (
                    PARTITION BY md.stream_ref
                    ORDER BY md.created_at DESC, md.row_id DESC
                ) AS rn
            FROM metadata md
            JOIN (SELECT DISTINCT r.stream_ref
                  FROM UNNEST($stream_refs) AS r(stream_ref)
                  WHERE r.stream_ref IS NOT NULL) batch
              ON batch.stream_ref = md.stream_ref
            WHERE md.metadata_key = 'allow_zeros'
              AND md.disabled_at IS NULL
        ) ranked WHERE rn = 1
    ) cfg ON cfg.stream_ref = unnested.stream_ref
    WHERE
        unnested.value != 0::NUMERIC(36,18)
        OR COALESCE(cfg.allow_zeros, FALSE) = TRUE
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
