CREATE OR REPLACE ACTION insert_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $event_time INT8,
    $value NUMERIC(36,18)
) PUBLIC {
    -- Ensure the wallet is allowed to write
    if is_wallet_allowed_to_write(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to write');
    }

    -- Ensure that the stream/contract is existent
    if is_existent($data_provider, $stream_id) == false {
        ERROR('stream does not exist');
    }

    $current_block INT := @height;

    -- Insert the new record into the primitive_events table
    INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at)
    VALUES ($stream_id, $data_provider, $event_time, $value, $current_block);
};

-- is_existent checks if the stream is existent
CREATE OR REPLACE ACTION is_existent($data_provider TEXT, $stream_id TEXT) PUBLIC view returns (result bool) {
    -- Check if the stream is initiated
    for $row in SELECT * FROM metadata WHERE metadata_key = 'type' AND stream_id = $stream_id AND data_provider = $data_provider LIMIT 1 {
         return true;
    }
    return false;
};

