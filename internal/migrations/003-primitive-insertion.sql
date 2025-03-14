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
    if is_wallet_allowed_to_write($data_provider, $stream_id, @caller) == false {
        ERROR('wallet not allowed to write');
    }

    -- Ensure that the stream/contract is existent
    if stream_exists($data_provider, $stream_id) == false {
        ERROR('stream does not exist');
    }

    $current_block INT := @height;

    -- Insert the new record into the primitive_events table
    INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at)
    VALUES ($stream_id, $data_provider, $event_time, $value, $current_block);
};
