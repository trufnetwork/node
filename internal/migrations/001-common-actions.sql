CREATE OR REPLACE ACTION create_stream(
    $stream_id TEXT,
    $stream_type TEXT
) PUBLIC {
    -- Get caller's address (data provider) first
    $data_provider TEXT := @caller;
    
    -- Check if caller is a valid ethereum address
    -- TODO: really check if it's a valid address
    if LENGTH($data_provider) != 42 
        OR substring($data_provider, 1, 2) != '0x' {
        ERROR('Invalid data provider address. Must be a valid Ethereum address: ' || $data_provider);
    }

    -- Check if stream_type is valid
    if $stream_type != 'primitive' AND $stream_type != 'composed' {
        ERROR('Invalid stream type. Must be "primitive" or "composed": ' || $stream_type);
    }
    
    -- Check if stream_id has valid format (st followed by 30 lowercase alphanumeric chars)
    -- TODO: only alphanumeric characters be allowed
    if LENGTH($stream_id) != 32 OR 
       substring($stream_id, 1, 2) != 'st' {
        ERROR('Invalid stream_id format. Must start with "st" followed by 30 lowercase alphanumeric characters: ' || $stream_id);
    }
    
    -- Check if stream already exists
    for $row in SELECT 1 FROM streams WHERE data_provider = $data_provider AND stream_id = $stream_id LIMIT 1 {
        ERROR('Stream already exists: ' || $stream_id);
    }
    
    -- Create the stream
    INSERT INTO streams (data_provider, stream_id, stream_type)
    VALUES ($data_provider, $stream_id, $stream_type);
    
    -- Add required metadata
    $current_block INT := @height;
    $current_uuid UUID := uuid_generate_v5('41fea9f0-179f-11ef-8838-325096b39f47'::UUID, @txid);
    
    -- Add type metadata
    $current_uuid := uuid_generate_v5($current_uuid, @txid);
    INSERT INTO metadata (row_id, data_provider, stream_id, metadata_key, value_s, created_at)
        VALUES ($current_uuid, $data_provider, $stream_id, 'type', $stream_type, $current_block);
    
    -- Add stream_owner metadata
    $current_uuid := uuid_generate_v5($current_uuid, @txid);
    INSERT INTO metadata (row_id, data_provider, stream_id, metadata_key, value_ref, created_at)
        VALUES ($current_uuid, $data_provider, $stream_id, 'stream_owner', LOWER($data_provider), $current_block);
    
    -- Add read visibility (public by default)
    $current_uuid := uuid_generate_v5($current_uuid, @txid);
    INSERT INTO metadata (row_id, data_provider, stream_id, metadata_key, value_i, created_at)
        VALUES ($current_uuid, $data_provider, $stream_id, 'read_visibility', 0, $current_block);
        
    -- Mark readonly keys
    $readonly_keys TEXT[] := ['stream_owner', 'readonly_key', 'taxonomy_version'];
    
    for $key IN ARRAY $readonly_keys {
        $current_uuid := uuid_generate_v5($current_uuid, @txid);
        INSERT INTO metadata (row_id, data_provider, stream_id, metadata_key, value_s, created_at)
            VALUES ($current_uuid, $data_provider, $stream_id, 'readonly_key', $key, $current_block);
    }
};

CREATE OR REPLACE ACTION delete_stream(
    $stream_id TEXT
) PUBLIC {
    -- Get caller's address (data provider) first
    $data_provider TEXT := @caller;

    -- Check if caller is a valid ethereum address
    -- TODO: really check if it's a valid address
    if LENGTH($data_provider) != 42
        OR substring($data_provider, 1, 2) != '0x' {
        ERROR('Invalid data provider address. Must be a valid Ethereum address: ' || $data_provider);
    }

    -- Check if stream_id has valid format (st followed by 30 lowercase alphanumeric chars)
    -- TODO: only alphanumeric characters be allowed
    if LENGTH($stream_id) != 32 OR
       substring($stream_id, 1, 2) != 'st' {
        ERROR('Invalid stream_id format. Must start with "st" followed by 30 lowercase alphanumeric characters: ' || $stream_id);
    }

    DELETE FROM streams WHERE data_provider = $data_provider AND stream_id = $stream_id;
};

-- Helper function to check if a stream is primitive or composed
CREATE OR REPLACE ACTION is_primitive_stream(
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns (is_primitive BOOL) {
    for $row in SELECT stream_type FROM streams 
        WHERE data_provider = $data_provider AND stream_id = $stream_id LIMIT 1 {
        return $row.stream_type = 'primitive';
    }
    
    ERROR('Stream not found: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
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
    if $limit IS NULL {
        $limit := 100;
    }
    if $offset IS NULL {
        $offset := 0;
    }
    if $order_by IS NULL {
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

CREATE OR REPLACE ACTION is_wallet_allowed_to_read_substreams(
    $data_provider TEXT,
    $stream_id TEXT,
    $wallet_address TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns (is_allowed BOOL) {
    return with all_substreams as (
            WITH RECURSIVE latest_taxonomies AS (
            SELECT DISTINCT ON (t.data_provider, t.stream_id, t.child_data_provider, t.child_stream_id, t.start_time)
                t.child_data_provider,
                t.child_stream_id,
                t.start_time
            FROM taxonomies t
            WHERE t.start_time <= $active_to
            AND (t.disabled_at IS NULL OR t.disabled_at > $active_from)
            AND t.data_provider = $data_provider
            AND t.stream_id = $stream_id
            ORDER BY t.data_provider, t.stream_id, t.child_data_provider, t.child_stream_id, 
                    t.start_time DESC, t.version DESC
        ),
        active_versions AS (
            SELECT DISTINCT ON (child_data_provider, child_stream_id)
                child_data_provider,
                child_stream_id
            FROM latest_taxonomies
            ORDER BY child_data_provider, child_stream_id, start_time DESC
        ),
        recursive_substreams AS (
            -- Base case: direct children with valid versions
            SELECT 
                av.child_data_provider AS data_provider, 
                av.child_stream_id AS stream_id
            FROM active_versions av
            
            UNION ALL
            
            -- Recursive case: children of children with their valid versions
            SELECT 
                t.child_data_provider, 
                t.child_stream_id
            FROM taxonomies t
            JOIN recursive_substreams rs ON t.data_provider = rs.data_provider AND t.stream_id = rs.stream_id
            WHERE t.start_time <= $active_to
            AND (t.disabled_at IS NULL OR t.disabled_at > $active_from)
        )
        -- Return individual rows instead of arrays
        SELECT DISTINCT data_provider, stream_id
    ),
    inexisting_substreams as (
        select data_provider, stream_id from all_substreams that are not in (select data_provider, stream_id from streams)
    ),
    private_substreams as (
        select data_provider, stream_id from all_substreams where metadata about vibility is 1
    ),
    streams_without_permissions as (
        select data_provider, stream_id from private_substreams where data_provider, stream_id doesnt have read wallet metadata for the wallet address
    )
    select count about inexistent streams, and streams without permissions;
    
    if one of the counts is greater than 0, return false;
    otherwise, return true;
};

CREATE OR REPLACE ACTION get_substreams(
    $data_provider TEXT,
    $stream_id TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns table(data_provider TEXT, stream_id TEXT) {
    -- Get latest active taxonomies first with version precedence
    WITH RECURSIVE latest_taxonomies AS (
        SELECT DISTINCT ON (t.data_provider, t.stream_id, t.child_data_provider, t.child_stream_id, t.start_time)
            t.child_data_provider,
            t.child_stream_id,
            t.start_time
        FROM taxonomies t
        WHERE t.start_time <= $active_to
          AND (t.disabled_at IS NULL OR t.disabled_at > $active_from)
          AND t.data_provider = $data_provider
          AND t.stream_id = $stream_id
        ORDER BY t.data_provider, t.stream_id, t.child_data_provider, t.child_stream_id, 
                 t.start_time DESC, t.version DESC
    ),
    active_versions AS (
        SELECT DISTINCT ON (child_data_provider, child_stream_id)
            child_data_provider,
            child_stream_id
        FROM latest_taxonomies
        ORDER BY child_data_provider, child_stream_id, start_time DESC
    ),
    recursive_substreams AS (
        -- Base case: direct children with valid versions
        SELECT 
            av.child_data_provider AS data_provider, 
            av.child_stream_id AS stream_id
        FROM active_versions av
        
        UNION ALL
        
        -- Recursive case: children of children with their valid versions
        SELECT 
            t.child_data_provider, 
            t.child_stream_id
        FROM taxonomies t
        JOIN recursive_substreams rs ON t.data_provider = rs.data_provider AND t.stream_id = rs.stream_id
        WHERE t.start_time <= $active_to
          AND (t.disabled_at IS NULL OR t.disabled_at > $active_from)
    )
    -- Return individual rows instead of arrays
    SELECT DISTINCT data_provider, stream_id
    FROM recursive_substreams;
};
