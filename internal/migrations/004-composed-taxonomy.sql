/**
 * insert_taxonomy: Creates a new taxonomy group_sequence for a composed stream.
 * Validates input arrays, increments group_sequence, and inserts child stream relationships.
 */
CREATE OR REPLACE ACTION insert_taxonomy(
    $data_provider TEXT,            -- The data provider of the parent stream.
    $stream_id TEXT,                -- The stream ID of the parent stream.
    $child_data_providers TEXT[],   -- The data providers of the child streams.
    $child_stream_ids TEXT[],       -- The stream IDs of the child streams.
    $weights NUMERIC(36,18)[],      -- The weights of the child streams.
    $start_date INT                 -- The start date of the taxonomy.
) PUBLIC {
    $data_provider := LOWER($data_provider);
    for $i in 1..array_length($child_data_providers) {
        $child_data_providers[$i] := LOWER($child_data_providers[$i]);
    }
    $lower_caller  := LOWER(@caller);

    -- ensure it's a composed stream
    if is_primitive_stream($data_provider, $stream_id) == true {
        ERROR('stream is not a composed stream');
    }

    -- Ensure the wallet is allowed to write
    if is_wallet_allowed_to_write($data_provider, $stream_id, $lower_caller) == false {
        ERROR('wallet not allowed to write');
    }
 
    -- Determine the number of child records provided.
    $num_children := array_length($child_stream_ids);

    if $num_children IS NULL {
       $num_children := 0;
    }
    -- Validate that all child arrays have the same length.
    if $num_children == 0 OR $num_children != array_length($child_data_providers) OR $num_children != array_length($weights) {
        error('All child arrays must be of the same length');
    }

    -- ensure there is at least 1 child, otherwise we might have silent bugs, with the user thinking he added something
    if $num_children == 0 {
        error('There must be at least 1 child');
    }

    -- Default start time to 0 if not provided
    if $start_date IS NULL {
        $start_date := 0;
    }

    -- Retrieve the current group_sequence for this parent and increment it by 1.
    $new_group_sequence := get_current_group_sequence($data_provider, $stream_id, true) + 1;

    FOR $i IN 1..$num_children {
        $child_data_provider_value := $child_data_providers[$i];
        $child_stream_id_value := $child_stream_ids[$i];
        $weight_value := $weights[$i];

        $taxonomy_id := uuid_generate_kwil(@txid||$data_provider||$stream_id||$child_data_provider_value||$child_stream_id_value||$i::TEXT);

        INSERT INTO taxonomies (
            data_provider,
            stream_id,
            taxonomy_id,
            child_data_provider,
            child_stream_id,
            weight,
            created_at,
            disabled_at,
            group_sequence,
            start_time
        ) VALUES (
            $data_provider,
            $stream_id,
            $taxonomy_id,
            $child_data_provider_value,
            $child_stream_id_value,
            $weight_value,
            @height,             -- Use the current block height for created_at.
            NULL,               -- New record is active.
            $new_group_sequence,          -- Use the new group_sequence for all child records.
            $start_date          -- Start date of the taxonomy.
        );
    }
};

/**
 * get_current_group_sequence: Helper to find the latest taxonomy group_sequence.
 * When $show_disabled is false, only active (non-disabled) records are considered.
 */
CREATE OR REPLACE ACTION get_current_group_sequence(
    $data_provider TEXT,
    $stream_id TEXT,
    $show_disabled bool
) private view returns (result int) {
    $data_provider  := LOWER($data_provider);

    if $show_disabled == false {
        for $row in SELECT group_sequence
        FROM taxonomies
        WHERE data_provider = $data_provider
        AND stream_id = $stream_id
        AND disabled_at IS NULL
        ORDER BY group_sequence DESC
        LIMIT 1 {
            return $row.group_sequence;
        }
    } else {
        for $row in SELECT group_sequence
        FROM taxonomies
        WHERE data_provider = $data_provider
        AND stream_id = $stream_id
        ORDER BY group_sequence DESC
        LIMIT 1 {
            return $row.group_sequence;
        }
    }
    return 0;
};

CREATE OR REPLACE ACTION describe_taxonomies(
    $data_provider TEXT,    -- Parent data provider
    $stream_id TEXT,        -- Parent stream id
    $latest_group_sequence BOOL    -- If true, only the latest (active) group_sequence is returned
) PUBLIC view returns table(
    data_provider TEXT,         -- Parent data provider
    stream_id TEXT,             -- Parent stream id
    child_data_provider TEXT,   -- Child data provider
    child_stream_id TEXT,       -- Child stream id
    weight NUMERIC(36,18),
    created_at INT,
    group_sequence INT,
    start_date INT             -- Aliased from start_time
) {
    $data_provider  := LOWER($data_provider);

    if $latest_group_sequence == true {
        $group_sequence := get_current_group_sequence($data_provider, $stream_id, false);
        return SELECT
            t.data_provider,
            t.stream_id,
            t.child_data_provider,
            t.child_stream_id,
            t.weight,
            t.created_at,
            t.group_sequence,
            t.start_time AS start_date
        FROM taxonomies t
        WHERE t.disabled_at IS NULL
            AND t.data_provider = $data_provider
            AND t.stream_id = $stream_id
            AND t.group_sequence = $group_sequence
        ORDER BY t.created_at DESC;
    } else {
        return SELECT
            t.data_provider,
            t.stream_id,
            t.child_data_provider,
            t.child_stream_id,
            t.weight,
            t.created_at,
            t.group_sequence,
            t.start_time AS start_date
        FROM taxonomies t
        WHERE t.disabled_at IS NULL
            AND t.data_provider = $data_provider
            AND t.stream_id = $stream_id
        ORDER BY t.group_sequence DESC;
    }
};

CREATE OR REPLACE ACTION disable_taxonomy(
    $data_provider TEXT,
    $stream_id TEXT,
    $group_sequence INT
) PUBLIC {
    $data_provider  := LOWER($data_provider);
    $lower_caller  := LOWER(@caller);
    -- Ensure the wallet is allowed to write
    if is_wallet_allowed_to_write($data_provider, $stream_id, $lower_caller) == false {
        ERROR('wallet not allowed to write');
    }

    UPDATE taxonomies
    SET disabled_at = @height
    WHERE data_provider = $data_provider
    AND stream_id = $stream_id
    AND group_sequence = $group_sequence;
};
