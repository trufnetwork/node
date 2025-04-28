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
    -- Ensure the wallet is allowed to write
    if !is_wallet_allowed_to_write($data_provider, $stream_id, @caller) {
        ERROR('wallet not allowed to write');
    }

    -- Ensure that the stream/contract is existent
    if !stream_exists($data_provider, $stream_id) {
        ERROR('stream does not exist');
    }

    -- Ensure that the stream is a primitive stream
    if is_primitive_stream($data_provider, $stream_id) == false {
        ERROR('stream is not a primitive stream');
    }

    $current_block INT := @height;

    -- Insert the new record into the primitive_events table
    INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at)
    VALUES ($stream_id, $data_provider, $event_time, $value, $current_block);
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
    $num_records INT := array_length($data_provider);
    if $num_records != array_length($stream_id) or $num_records != array_length($event_time) or $num_records != array_length($value) {
        ERROR('array lengths mismatch');
    }

    $current_block INT := @height;

    -- Check stream existence in batch
    for $row in stream_exists_batch($data_provider, $stream_id) {
        if !$row.stream_exists {
            ERROR('stream does not exist: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Check if streams are primitive in batch
    for $row in is_primitive_stream_batch($data_provider, $stream_id) {
        if !$row.is_primitive {
            ERROR('stream is not a primitive stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Validate that the wallet is allowed to write to each stream
    for $row in is_wallet_allowed_to_write_batch($data_provider, $stream_id, @caller) {
        if !$row.is_allowed {
            ERROR('wallet not allowed to write to stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Insert all records
    FOR $i IN 1..$num_records {
        $stream_id_val TEXT := $stream_id[$i];
        $data_provider_val TEXT := $data_provider[$i];
        $event_time_val INT8 := $event_time[$i];
        $value_val NUMERIC(36,18) := $value[$i];
        INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at)
        VALUES ($stream_id_val, $data_provider_val, $event_time_val, $value_val, $current_block);
    }
};