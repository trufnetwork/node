CREATE OR REPLACE ACTION insert_taxonomy(
    $data_provider TEXT,            -- The data provider of the parent stream.
    $stream_id TEXT,                -- The stream ID of the parent stream.
    $child_data_providers TEXT[],   -- The data providers of the child streams.
    $child_stream_ids TEXT[],       -- The stream IDs of the child streams.
    $weights NUMERIC(36,18)[],      -- The weights of the child streams.
    $start_date INT                 -- The start date of the taxonomy.
) PUBLIC view returns (result bool) {
    -- Ensure the wallet is allowed to write
    if is_wallet_allowed_to_write(@caller, $data_provider, $stream_id) == false {
        ERROR('wallet not allowed to write');
    }

    -- Determine the number of child records provided.
    $num_children := array_length($child_stream_ids);

    -- Validate that all child arrays have the same length.
    if $num_children IS NULL OR $num_children == 0 OR
    $num_children != array_length($child_data_providers) OR
    $num_children != array_length($weights) {
        error('All child arrays must be of the same length');
    }

    -- Retrieve the current version for this parent and increment it by 1.
    $new_version := get_current_version($data_provider, $stream_id, true) + 1;

    FOR $i IN 1..$num_children {
        $child_data_provider_value := $child_data_providers[$i];
        $child_stream_id_value := $child_stream_ids[$i];
        $weight_value := $weights[$i];

        INSERT INTO taxonomies (
            data_provider,
            stream_id,
            taxonomy_id,
            child_data_provider,
            child_stream_id,
            weight,
            created_at,
            disabled_at,
            version,
            start_time
        ) VALUES (
            $data_provider,
            $stream_id,
            uuid_generate_kwil(@txid||$i::TEXT), -- Generate a new UUID for the taxonomy.
            $child_data_provider_value,
            $child_stream_id_value,
            $weight_value,
            @height,             -- Use the current block height for created_at.
            NULL,               -- New record is active.
            $new_version,          -- Use the new version for all child records.
            $start_date          -- Start date of the taxonomy.
        );
    }
    return true;
};

------------------------------------------------------------
-- Helper action: Get the latest taxonomy version for a parent.
-- When $show_disabled is false, only active (non-disabled) records are considered.
CREATE OR REPLACE ACTION get_current_version(
    $data_provider TEXT,
    $stream_id TEXT,
    $show_disabled bool
) private view returns (result int) {
    if $show_disabled == false {
        for $row in SELECT version
        FROM taxonomies
        WHERE data_provider = $data_provider
        AND stream_id = $stream_id
        AND disabled_at IS NULL
        ORDER BY version DESC
        LIMIT 1 {
            return $row.version;
        }
    } else {
        for $row in SELECT version
        FROM taxonomies
        WHERE data_provider = $data_provider
        AND stream_id = $stream_id
        ORDER BY version DESC
        LIMIT 1 {
            return $row.version;
        }
    }
    return 0;
}
