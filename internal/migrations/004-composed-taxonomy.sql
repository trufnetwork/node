CREATE OR REPLACE ACTION insert_taxonomy(
    $data_providers TEXT,
    $stream_ids TEXT,
    $child_stream_id TEXT,
    $child_data_provider TEXT,
    $weights NUMERIC(36,18),
    $start_date INT
) PUBLIC view returns (result bool) {
    -- Ensure the wallet is allowed to write
    if is_wallet_allowed_to_write(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to write');
    }

--     DECLARE current_version INT;
    $current_version::Int=0;
    -- Retrieve the latest version for the given parent/child combination.
    for $row in SELECT version
        FROM taxonomies
        WHERE data_provider = $data_providers
          AND stream_id = $stream_ids
          AND child_data_provider = $child_data_provider
          AND child_stream_id = $child_stream_id
        ORDER BY version DESC
            LIMIT 1 {
        $current_version := $row.version;
    }

    if $current_version != 0 {
        -- Disable the existing taxonomy record by setting disabled_at to the current block height.
        UPDATE taxonomies
        SET disabled_at = @height
        WHERE data_provider = $data_providers
          AND stream_id = $stream_ids
          AND child_data_provider = $child_data_provider
          AND child_stream_id = $child_stream_id
          AND version = $current_version;
    }

    -- Insert a new taxonomy record with version incremented by one.
    INSERT INTO taxonomies (
        data_provider,
        stream_id,
        taxonomy_id,
        child_stream_id,
        child_data_provider,
        weight,
        created_at,
        disabled_at,
        version,
        start_time
    ) VALUES (
        $data_providers,
        $stream_ids,
        uuid_generate_v4(),   -- Generate a new unique taxonomy ID.
        child_stream_id,
        child_data_provider,
        $weights,
        @height,              -- Use the current block height as the created_at timestamp.
        NULL,                 -- New record is active, so no disabled_at.
        current_version + 1,  -- Increment version.
        $start_date           -- Use the provided start date.
    );

    return true;
};