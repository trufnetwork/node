/**
 * insert_taxonomy: Creates a new taxonomy version for a composed stream.
 * Validates input arrays, increments version, and inserts child stream relationships.
 */
CREATE OR REPLACE ACTION insert_taxonomy(
    $data_provider TEXT,            -- The data provider of the parent stream.
    $stream_id TEXT,                -- The stream ID of the parent stream.
    $child_data_providers TEXT[],   -- The data providers of the child streams.
    $child_stream_ids TEXT[],       -- The stream IDs of the child streams.
    $weights NUMERIC(36,18)[],      -- The weights of the child streams.
    $start_date INT                 -- The start date of the taxonomy.
) PUBLIC view returns (result bool) {
    -- ensure it's a composed stream
    if is_primitive_stream($data_provider, $stream_id) == true {
        ERROR('stream is not a composed stream');
    }

    -- Ensure the wallet is allowed to write
    if is_wallet_allowed_to_write($data_provider, $stream_id, @caller) == false {
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

    -- ensure there is at least 1 child, otherwise we might have silent bugs, with the user thinking he added something
    if $num_children == 0 {
        error('There must be at least 1 child');
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

/**
 * get_current_version: Helper to find the latest taxonomy version.
 * When $show_disabled is false, only active (non-disabled) records are considered.
 */
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
};

CREATE OR REPLACE ACTION describe_taxonomies(
    $data_provider TEXT,    -- Parent data provider
    $stream_id TEXT,        -- Parent stream id
    $latest_version BOOL    -- If true, only the latest (active) version is returned
) PUBLIC view returns table(
    data_provider TEXT,         -- Parent data provider
    stream_id TEXT,             -- Parent stream id
    child_data_provider TEXT,   -- Child data provider
    child_stream_id TEXT,       -- Child stream id
    weight NUMERIC(36,18),
    created_at INT,
    version INT,
    start_date INT             -- Aliased from start_time
) {
    if $latest_version == true {
        $version := get_current_version($data_provider, $stream_id, false);
        return SELECT
            t.data_provider,
            t.stream_id,
            t.child_data_provider,
            t.child_stream_id,
            t.weight,
            t.created_at,
            t.version,
            t.start_time AS start_date
        FROM taxonomies t
        WHERE t.disabled_at IS NULL
            AND t.data_provider = $data_provider
            AND t.stream_id = $stream_id
            AND t.version = $version
        ORDER BY t.created_at DESC;
    } else {
        return SELECT
            t.data_provider,
            t.stream_id,
            t.child_data_provider,
            t.child_stream_id,
            t.weight,
            t.created_at,
            t.version,
            t.start_time AS start_date
        FROM taxonomies t
        WHERE t.disabled_at IS NULL
            AND t.data_provider = $data_provider
            AND t.stream_id = $stream_id
        ORDER BY t.version DESC;
    }
};

