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

    -- Validate each record in the batch
    FOR $i IN 1..$num_records {
        if !is_wallet_allowed_to_write($data_provider[$i], $stream_id[$i], @caller) {
            ERROR('wallet not allowed to write');
        }
        if !stream_exists($data_provider[$i], $stream_id[$i]) {
            ERROR('stream does not exist');
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