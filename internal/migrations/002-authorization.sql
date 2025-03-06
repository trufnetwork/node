-- TODO: insert/retrieve authorization data


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