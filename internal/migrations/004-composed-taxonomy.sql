-- TODO: insert/retrieve taxonomy data
CREATE OR REPLACE ACTION insert_taxonomy(
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns (result bool) {
    -- TODO: Implement this.
    return true;
};