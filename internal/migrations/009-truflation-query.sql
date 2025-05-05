/**
 * truflation_last_deployed_date: Returns the last deployed date of the Truflation data provider.
 * This action checks if the caller has read access to the specified stream and ensures that the stream is a primitive stream.
 * If both conditions are met, it retrieves the last deployed date from the primitive_events table.
 */
CREATE OR REPLACE ACTION truflation_last_deployed_date(
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns table(
       value TEXT
) {
    $data_provider  := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    -- Check read access first
    if !is_allowed_to_read($data_provider, $stream_id, $lower_caller, 0, 0) {
        ERROR('wallet not allowed to read');
    }

    -- Ensure that the stream is a primitive stream
    if !is_primitive_stream($data_provider, $stream_id) {
        ERROR('stream is not a primitive stream');
    }

    RETURN SELECT truflation_created_at
           FROM primitive_events
           WHERE data_provider = $data_provider
             AND stream_id = $stream_id
           ORDER BY truflation_created_at DESC LIMIT 1;
};

/**
 * truflation_insert_records: Adds multiple new data points to a primitive stream in batch.
 * Validates write permissions and stream existence for each record before insertion.
 * This action is specifically designed for the Truflation data provider as it requires the truflation_created_at field.
 */
CREATE OR REPLACE ACTION truflation_insert_records(
    $data_provider TEXT[],
    $stream_id TEXT[],
    $event_time INT8[],
    $value NUMERIC(36,18)[],
    $truflation_created_at TEXT[]
) PUBLIC {
    for $i in 1..array_length($data_provider) {
        $data_provider[$i] := LOWER($data_provider[$i]);
    }
    $lower_caller TEXT := LOWER(@caller);
    $num_records INT := array_length($data_provider);
    if $num_records != array_length($stream_id) or $num_records != array_length($event_time) or $num_records != array_length($value) or $num_records != array_length($truflation_created_at) {
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
            $truflation_created_at AS truflation_created_at_array
    ),
    arguments AS (
        SELECT
            record_arrays.stream_ids[idx] AS stream_id,
            record_arrays.data_providers[idx] AS data_provider,
            record_arrays.event_times[idx] AS event_time,
            record_arrays.values_array[idx] AS value,
            record_arrays.truflation_created_at_array[idx] AS truflation_created_at
        FROM indexes
        JOIN record_arrays ON 1=1
    )
    INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at, truflation_created_at)
    SELECT
        stream_id,
        data_provider,
        event_time,
        value,
        $current_block,
        truflation_created_at
    FROM arguments;
};