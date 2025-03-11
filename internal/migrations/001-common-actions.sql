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

CREATE OR REPLACE ACTION is_allowed_to_read_all(
    $data_provider TEXT,
    $stream_id TEXT,
    $wallet_address TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns (is_allowed BOOL) {
    
    $data_providers TEXT[];
    $stream_ids TEXT[];

    -- Get all data providers and stream ids for the given stream
    for $row in get_category_streams($data_provider, $stream_id, $active_from, $active_to) {
        $data_providers := array_append($data_providers, $row.data_provider);
        $stream_ids := array_append($stream_ids, $row.stream_id);
    }


    
    for $row in with all_substreams as (
       SELECT unnest($data_providers) as data_provider, 
              unnest($stream_ids) as stream_id
    ),
    inexisting_substreams as (
        SELECT a.data_provider, a.stream_id 
        FROM all_substreams a
        LEFT JOIN streams s 
            ON a.data_provider = s.data_provider 
            AND a.stream_id = s.stream_id
        WHERE s.data_provider IS NULL
    ),
    private_substreams as (
        SELECT a.data_provider, a.stream_id 
        FROM all_substreams a
        WHERE (
            SELECT value_i
            FROM metadata m
            WHERE m.data_provider = a.data_provider
                AND m.stream_id = a.stream_id
                AND m.metadata_key = 'read_visibility'
                AND m.disabled_at IS NULL
            ORDER BY m.created_at DESC
            LIMIT 1
        ) = 1  -- 1 indicates private visibility
    ),
    streams_without_permissions as (
        SELECT p.data_provider, p.stream_id 
        FROM private_substreams p
        WHERE NOT EXISTS (
            SELECT 1
            FROM metadata m
            WHERE m.data_provider = p.data_provider
                AND m.stream_id = p.stream_id
                AND m.metadata_key = 'allow_read_wallet'
                AND LOWER(m.value_ref) = LOWER($wallet_address)
                AND m.disabled_at IS NULL
            ORDER BY m.created_at DESC
            LIMIT 1
        )
    )
    SELECT 
        (SELECT COUNT(*) FROM inexisting_substreams) AS missing_count,
        (SELECT COUNT(*) FROM streams_without_permissions) AS unauthorized_count
    {
        return $missing_count > 0 OR $unauthorized_count > 0;
    }
};

CREATE OR REPLACE ACTION get_category_streams(
    $data_provider TEXT,
    $stream_id TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns table(data_provider TEXT, stream_id TEXT) {
    -- Get all substreams with proper recursive traversal, including the root stream itself
    return WITH RECURSIVE 
        -- effective_taxonomies holds, for every parent-child link that is active,
        -- the rows that are considered effective given the time window.
        effective_taxonomies AS (
        SELECT 
            t.data_provider,
            t.stream_id,
            t.child_data_provider,
            t.child_stream_id,
            t.start_time
        FROM taxonomies t
        WHERE t.disabled_at IS NULL
            AND ($active_to IS NULL OR t.start_time <= $active_to)
            AND (
            -- (A) For rows before (or at) $active_from: only include the one with the maximum start_time.
            ($active_from IS NOT NULL 
                AND t.start_time <= $active_from 
                AND t.start_time = (
                    SELECT max(t2.start_time)
                    FROM taxonomies t2
                    WHERE t2.data_provider = t.data_provider
                        AND t2.stream_id = t.stream_id
                        AND t2.disabled_at IS NULL
                        AND ($active_to IS NULL OR t2.start_time <= $active_to)
                        AND t2.start_time <= $active_from
                )
            )
            -- (B) Also include any rows with start_time greater than $active_from.
            OR ($active_from IS NULL OR t.start_time > $active_from)
            )
        ),
        -- Now recursively gather substreams using the effective taxonomy links.
        recursive_substreams AS (
            -- Start with the root stream itself
            SELECT $data_provider AS data_provider, 
                   $stream_id AS stream_id
                UNION
                -- Then add all child streams
                SELECT et.child_data_provider,
                       et.child_stream_id
                FROM effective_taxonomies et
                JOIN recursive_substreams rs
                    ON et.data_provider = rs.data_provider
                    AND et.stream_id = rs.stream_id
            )
        SELECT DISTINCT data_provider, stream_id
        FROM recursive_substreams;
};
