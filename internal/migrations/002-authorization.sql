-- is_wallet_allowed_to_read checks if a wallet is allowed to read a stream
CREATE OR REPLACE ACTION is_wallet_allowed_to_read(
    $wallet TEXT,
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns (result BOOL) {
    -- TODO: Implement this. But instead of checking only a single stream,
    -- it will recursively check all the streams if it's a composed stream.
    -- the intention is to use only once on a query, and not in the loop.
    return true;
};


CREATE OR REPLACE ACTION is_wallet_allowed_to_write(
    $wallet TEXT,
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns (result bool) {
    -- Check if the wallet is the stream owner
    for $row in SELECT * FROM metadata
                 WHERE metadata_key = 'stream_owner'
                   AND value_ref = LOWER($wallet)
                   AND data_provider = $data_provider
                   AND stream_id = $stream_id
                 LIMIT 1 {
         return true;
    }

    -- Check if the wallet is explicitly allowed to write via metadata permissions
    for $row in get_metadata($data_provider, $stream_id, 'allow_write_wallet', false, $wallet, 1, 0, 'created_at DESC') {
        return true;
    }

    return false;
};
