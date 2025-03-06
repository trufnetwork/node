CREATE OR REPLACE ACTION insert_record(
    $stream_id TEXT,
    $event_time INT8,
    $value NUMERIC(36,18)
) PUBLIC {
    -- Get the caller's address as the data provider
    $data_provider TEXT := @caller;

    -- Ensure the wallet is allowed to write
    if is_wallet_allowed_to_write($data_provider) == false {
        ERROR('wallet not allowed to write');
    }

    -- Ensure that the stream/contract is existent
    if is_existent($stream_id) == false {
        ERROR('contract must be initiated');
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

-- This action wraps metadata selection with pagination parameters.
-- It supports ordering only by created_at ascending or descending.
CREATE OR REPLACE ACTION get_metadata(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT,
    $only_latest BOOL,
    $ref TEXT,
    $limit INT,
    $offset INT,
    $order_by TEXT
) PUBLIC view returns table(
    row_id uuid,
    value_i int,
    value_f NUMERIC(36,18),
    value_b bool,
    value_s TEXT,
    value_ref TEXT,
    created_at INT
) {
    -- Set default values if parameters are null
    if $limit IS NULL{
        $limit := 100;
    }
    if $offset IS NULL{
        $offset := 0;
    }
    if $order_by IS NULL{
        $order_by := 'created_at DESC';
    }

    RETURN SELECT row_id,
                  value_i,
                  value_f,
                  value_b,
                  value_s,
                  value_ref,
                  created_at
        FROM metadata
           WHERE metadata_key = $key
            AND disabled_at IS NULL
            AND ($ref IS NULL OR LOWER(value_ref) = LOWER($ref))
            AND stream_id = $stream_id
            AND data_provider = $data_provider
       ORDER BY
               CASE WHEN $order_by = 'created_at DESC' THEN created_at END DESC,
               CASE WHEN $order_by = 'created_at ASC' THEN created_at END ASC
       LIMIT $limit OFFSET $offset;
};

CREATE OR REPLACE ACTION is_wallet_allowed_to_write(
    $wallet TEXT
) PUBLIC view returns (result bool) {
    -- Check if the wallet is the stream owner
    for $row in SELECT * FROM metadata
                 WHERE metadata_key = 'stream_owner'
                   AND value_ref = LOWER($wallet)
                 LIMIT $limit OFFSET $offset {
         return true;
    }

    -- Check if the wallet is explicitly allowed to write via metadata permissions
    for $row in get_metadata('allow_write_wallet', false, $wallet, 1, 0, 'created_at DESC') {
         return true;
    }

    return false;
};