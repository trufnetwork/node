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

    -- Record count is used for validation only — fees are a flat 1 TRUF
    -- per transaction now, so the count no longer factors into fee math.
    $num_records INT := array_length($data_provider);

    -- Cap batch size to prevent superlinear block execution time.
    -- With per-tx PG isolation, blocks handle thousands of small txns efficiently.
    -- All shipping clients (sdk-go BulkInserter, sdk-py BulkInserter, the
    -- truf-data-provider Go cron, and the tsn-adapters Python pipeline)
    -- already chunk at 10 rows/tx, so this cap is a defense-in-depth bound
    -- against compromised/buggy callers and not a constraint on legitimate
    -- batching. See truflation/website#3887.
    if $num_records > 10 {
        ERROR('insert_records: batch size exceeds maximum of 10 records');
    }

    -- Cheap input validation runs before any precompile call so malformed
    -- requests (NULL/empty/length-mismatched arrays) fail fast instead of
    -- propagating NULL into fee math or wasting bridge.balance() roundtrips.
    if $num_records IS NULL OR $num_records = 0 {
        ERROR('insert_records: empty or NULL data_provider array');
    }
    -- Use IS DISTINCT FROM so a NULL parallel array (array_length(NULL) = NULL)
    -- is treated as a length mismatch and the ERROR fires. Plain `!= NULL`
    -- would yield NULL — falsy in IF — and let mismatched batches reach the
    -- fee transfer with a worse downstream error. Parentheses are required
    -- because Kuneiform's parser otherwise binds OR tighter than the right
    -- operand of IS DISTINCT FROM, producing a type error.
    if ($num_records IS DISTINCT FROM array_length($stream_id))
       OR ($num_records IS DISTINCT FROM array_length($event_time))
       OR ($num_records IS DISTINCT FROM array_length($value)) {
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

    $caller_balance := hoodi_tt.balance(@caller);

    IF $caller_balance < $total_fee {
        ERROR('Insufficient balance for write fee. Required: 1 TRUF');
    }

    hoodi_tt.transfer($leader_hex, $total_fee);
    $fee_total := $total_fee;
    $fee_recipient := '0x' || $leader_hex;
    -- ===== END FEE COLLECTION =====

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
