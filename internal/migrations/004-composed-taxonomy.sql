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
    if $num_children != array_length($child_data_providers) OR $num_children != array_length($weights) {
        ERROR('All child arrays must be of the same length');
    }
    -- ensure there is at least 1 child, otherwise we might have silent bugs
    if $num_children == 0 {
        ERROR('There must be at least 1 child');
    }

    -- ===== FEE COLLECTION WITH ROLE EXEMPTION =====

    -- Check if caller is exempt (has system:network_writer role)
    $is_exempt BOOL := FALSE;
    FOR $row IN are_members_of('system', 'network_writer', ARRAY[$lower_caller]) {
        IF $row.wallet = $lower_caller AND $row.is_member {
            $is_exempt := TRUE;
            BREAK;
        }
    }

    -- Collect fee only from non-exempt wallets (2 TRUF per stream)
    IF NOT $is_exempt {
        $fee_per_stream := 2000000000000000000::NUMERIC(78, 0); -- 2 TRUF with 18 decimals
        $total_fee := $fee_per_stream * $num_children::NUMERIC(78, 0);

        $caller_balance := ethereum_bridge.balance(@caller);

        IF $caller_balance < $total_fee {
            -- Derive human-readable fee from $total_fee
            ERROR('Insufficient balance for taxonomies creation. Required: ' || ($total_fee / 1000000000000000000::NUMERIC(78, 0))::TEXT || ' TRUF for ' || $num_children::TEXT || ' child stream(s)');
        }

        $leader_addr TEXT := encode(@leader_sender, 'hex')::TEXT;
        ethereum_bridge.transfer($leader_addr, $total_fee);
    }
    -- ===== END FEE COLLECTION =====

    -- Default start time to 0 if not provided
    if $start_date IS NULL {
        $start_date := 0;
    }

    -- Retrieve the current group_sequence for this parent and increment it by 1.
    $new_group_sequence := get_current_group_sequence($data_provider, $stream_id, true) + 1;
    
    $stream_ref := get_stream_id($data_provider, $stream_id);
    if $stream_ref IS NULL {
        ERROR('parent stream does not exist: ' || $data_provider || ':' || $stream_id);
    }

    FOR $i IN 1..$num_children {
        $child_data_provider_value := $child_data_providers[$i];
        $child_stream_id_value := $child_stream_ids[$i];
        $child_stream_ref := get_stream_id($child_data_provider_value, $child_stream_id_value);
        $weight_value := $weights[$i];

        if $child_stream_ref IS NULL {
            ERROR('child stream does not exist: ' || $child_data_provider_value || ':' || $child_stream_id_value);
        }

        $taxonomy_id := uuid_generate_kwil(@txid||$data_provider||$stream_id||$child_data_provider_value||$child_stream_id_value||$i::TEXT);

        INSERT INTO taxonomies (
            taxonomy_id,
            weight,
            created_at,
            disabled_at,
            group_sequence,
            start_time,
            stream_ref,
            child_stream_ref
        ) VALUES (
            $taxonomy_id,
            $weight_value,
            @height,             -- Use the current block height for created_at.
            NULL,               -- New record is active.
            $new_group_sequence,          -- Use the new group_sequence for all child records.
            $start_date,          -- Start date of the taxonomy.
            $stream_ref,
            $child_stream_ref
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
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    if $show_disabled == false {
        for $row in SELECT group_sequence
        FROM taxonomies
        WHERE stream_ref = $stream_ref
        AND disabled_at IS NULL
        ORDER BY group_sequence DESC
        LIMIT 1 {
            return $row.group_sequence;
        }
    } else {
        for $row in SELECT group_sequence
        FROM taxonomies
        WHERE stream_ref = $stream_ref
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
    $stream_ref := get_stream_id($data_provider, $stream_id);

    if $latest_group_sequence == true {
        $group_sequence := get_current_group_sequence($data_provider, $stream_id, false);
        return SELECT
            parent_dp.address as data_provider,
            parent_s.stream_id,
            child_dp.address as child_data_provider,
            child_s.stream_id as child_stream_id,
            t.weight,
            t.created_at,
            t.group_sequence,
            t.start_time AS start_date
        FROM taxonomies t
        JOIN streams parent_s ON t.stream_ref = parent_s.id
        JOIN data_providers parent_dp ON parent_s.data_provider_id = parent_dp.id
        JOIN streams child_s ON t.child_stream_ref = child_s.id
        JOIN data_providers child_dp ON child_s.data_provider_id = child_dp.id
        WHERE t.disabled_at IS NULL
            AND t.stream_ref = $stream_ref
            AND t.group_sequence = $group_sequence
        ORDER BY t.created_at DESC;
    } else {
        return SELECT
            parent_dp.address as data_provider,
            parent_s.stream_id,
            child_dp.address as child_data_provider,
            child_s.stream_id as child_stream_id,
            t.weight,
            t.created_at,
            t.group_sequence,
            t.start_time AS start_date
        FROM taxonomies t
        JOIN streams parent_s ON t.stream_ref = parent_s.id
        JOIN data_providers parent_dp ON parent_s.data_provider_id = parent_dp.id
        JOIN streams child_s ON t.child_stream_ref = child_s.id
        JOIN data_providers child_dp ON child_s.data_provider_id = child_dp.id
        WHERE t.disabled_at IS NULL
            AND t.stream_ref = $stream_ref
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

    $stream_ref := get_stream_id($data_provider, $stream_id);

    UPDATE taxonomies
    SET disabled_at = @height
    WHERE stream_ref = $stream_ref
    AND group_sequence = $group_sequence;
};
