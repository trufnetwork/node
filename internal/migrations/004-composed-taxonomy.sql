CREATE OR REPLACE ACTION insert_taxonomy(
    $data_provider TEXT, -- The data provider of the parent stream.
    $stream_id TEXT,    -- The stream ID of the parent stream.
    $child_data_providers TEXT[], -- The data providers of the child streams.
    $child_stream_ids TEXT[], -- The stream IDs of the child streams.
    $weights NUMERIC(36,18)[], -- The weights of the child streams.
    $start_date INT -- The start date of the taxonomy.
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

    FOR $i IN 1..$num_children {
        $current_version := 0::INT;
        -- Retrieve the latest version for the given parent/child combination.

        $child_data_provider_value := $child_data_providers[$i];
        $child_stream_id_value := $child_stream_ids[$i];
        for $row in SELECT version
            FROM taxonomies
            WHERE data_provider = $data_provider
              AND stream_id = $stream_id
              AND child_data_provider = $child_data_provider_value
              AND child_stream_id = $child_stream_id_value
            ORDER BY version DESC
                LIMIT 1 {
                $current_version := $row.version;
        }

        if $current_version != 0 {
            -- Disable the existing taxonomy record by setting disabled_at to the current block height.
            $child_data_provider_value := $child_data_providers[$i];
            $child_stream_id_value := $child_stream_ids[$i];
            UPDATE taxonomies
                SET disabled_at = @height
                WHERE data_provider = $data_provider
                AND stream_id = $stream_id
                AND child_data_provider = $child_data_provider_value
                AND child_stream_id = $child_stream_id_value
                AND version = $current_version;
        }

        $child_data_provider_value := $child_data_providers[$i];
        $child_stream_id_value := $child_stream_ids[$i];
        $weight_value := $weights[$i];
        $current_version_value := $current_version + 1;
        -- Insert the new taxonomy record with version incremented by one.
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
            NULL,                -- New record is active.
            $current_version_value, -- Increment the version.
            $start_date          -- Start date of the taxonomy.
        );
    }
    return true;
};