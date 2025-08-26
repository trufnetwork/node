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

    $num_records INT := array_length($data_provider);
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
    WHERE unnested.value != 0::NUMERIC(36,18);

    -- Enqueue days for pruning using helper (idempotent, distinct per day)
    helper_enqueue_prune_days(
        $stream_refs,
        $event_time,
        $value
    );
};