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
    $data_provider TEXT := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    -- Ensure the wallet is allowed to write
    if !is_wallet_allowed_to_write($data_provider, $stream_id, $lower_caller) {
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

    -- Skip insertion if value is 0
    if $value == 0::NUMERIC(36,18) {
        RETURN;
    }

    $current_block INT := @height;
    $stream_ref UUID := get_stream_id($data_provider, $stream_id);

    -- Insert the new record into the primitive_events table
    INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at, stream_ref)
    VALUES ($stream_id, $data_provider, $event_time, $value, $current_block, $stream_ref);
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
    for $row in is_wallet_allowed_to_write_batch($data_provider, $stream_id, $lower_caller) {
        if !$row.is_allowed {
            ERROR('wallet not allowed to write to stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Get stream reference for all streams
    $stream_refs := []::UUID[];
    for $i in 1..array_length($data_provider) {
      $id := get_stream_id($data_provider[$i], $stream_id[$i]);
      $stream_refs := array_append($stream_refs, $id);
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
            $stream_id AS stream_ids,
            $data_provider AS data_providers,
            $event_time AS event_times,
            $value AS values_array,
            $stream_refs AS stream_refs_array
    ),
    arguments AS (
        SELECT 
            record_arrays.stream_ids[idx] AS stream_id,
            record_arrays.data_providers[idx] AS data_provider,
            record_arrays.event_times[idx] AS event_time,
            record_arrays.values_array[idx] AS value,
            record_arrays.stream_refs_array[idx] AS stream_ref
        FROM indexes
        JOIN record_arrays ON 1=1
        WHERE record_arrays.values_array[idx] != 0::NUMERIC(36,18)
    )
    INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at, truflation_created_at, stream_ref)
    SELECT 
        stream_id, 
        data_provider, 
        event_time, 
        value, 
        $current_block,
        NULL,
        stream_ref
    FROM arguments;
};