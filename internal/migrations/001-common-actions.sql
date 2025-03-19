/**
 * create_stream: Creates a new stream with required metadata.
 * Validates stream_id format, data provider address, and stream type.
 * Sets default metadata including type, owner, visibility, and readonly keys.
 */
CREATE OR REPLACE ACTION create_stream(
    $stream_id TEXT,
    $stream_type TEXT
) PUBLIC {
    -- Get caller's address (data provider) first
    $data_provider TEXT := @caller;
    $current_block INT := @height;
    
    -- Check if caller is a valid ethereum address
    if NOT check_ethereum_address($data_provider) {
        ERROR('Invalid data provider address. Must be a valid Ethereum address: ' || $data_provider);
    }

    -- Check if stream_type is valid
    if $stream_type != 'primitive' AND $stream_type != 'composed' {
        ERROR('Invalid stream type. Must be "primitive" or "composed": ' || $stream_type);
    }
    
    -- Check if stream_id has valid format (st followed by 30 lowercase alphanumeric chars)
    if NOT check_stream_id_format($stream_id) {
        ERROR('Invalid stream_id format. Must start with "st" followed by 30 lowercase alphanumeric characters: ' || $stream_id);
    }
    
    -- Check if stream already exists
    for $row in SELECT 1 FROM streams WHERE data_provider = $data_provider AND stream_id = $stream_id LIMIT 1 {
        ERROR('Stream already exists: ' || $stream_id);
    }
    
    -- Create the stream
    INSERT INTO streams (data_provider, stream_id, stream_type, created_at)
    VALUES ($data_provider, $stream_id, $stream_type, $current_block);
    
    -- Add required metadata
    $current_block INT := @height;
    $current_uuid UUID := uuid_generate_kwil('create_stream_' || @txid || $stream_id);
    
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

/**
 * insert_metadata: Adds metadata to a stream.
 * Validates caller is stream owner and handles different value types.
 * Prevents modification of readonly keys.
 */
CREATE OR REPLACE ACTION insert_metadata(
    -- not necessarily the caller is the original deployer of the stream
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT,
    $value TEXT,
    $val_type TEXT
) PUBLIC {
    -- Initialize value variables
    $value_i INT;
    $value_s TEXT;
    $value_f DECIMAL(36,18);
    $value_b BOOL;
    $value_ref TEXT;
    
    -- Check if caller is the stream owner
    if !is_stream_owner($data_provider, $stream_id, @caller) {
        ERROR('Only stream owner can insert metadata');
    }
    
    -- Set the appropriate value based on type
    if $val_type = 'int' {
        $value_i := $value::INT;
    } elseif $val_type = 'string' {
        $value_s := $value;
    } elseif $val_type = 'bool' {
        $value_b := $value::BOOL;
    } elseif $val_type = 'ref' {
        $value_ref := $value;
    } elseif $val_type = 'float' {
        $value_f := $value::DECIMAL(36,18);
    } else {
        ERROR(FORMAT('Unknown type used "%s". Valid types = "float" | "bool" | "int" | "ref" | "string"', $val_type));
    }
    
    -- Check if the key is read-only
    $is_readonly BOOL := false;
    for $row in SELECT * FROM metadata 
        WHERE data_provider = $data_provider 
        AND stream_id = $stream_id 
        AND metadata_key = 'readonly_key' 
        AND value_s = $key LIMIT 1 {
        $is_readonly := true;
    }
    
    if $is_readonly = true {
        ERROR('Cannot insert metadata for read-only key');
    }
    
    -- Create deterministic UUID for the metadata record
    $uuid_key TEXT := @txid || $key || $value;
    $uuid UUID := uuid_generate_kwil($uuid_key);
    $current_block INT := @height;
    
    -- Insert the metadata
    INSERT INTO metadata (
        row_id, 
        data_provider, 
        stream_id, 
        metadata_key, 
        value_i, 
        value_f, 
        value_s, 
        value_b, 
        value_ref, 
        created_at
    ) VALUES (
        $uuid, 
        $data_provider, 
        $stream_id, 
        $key, 
        $value_i, 
        $value_f, 
        $value_s, 
        $value_b, 
        LOWER($value_ref), 
        $current_block
    );
};

/**
 * disable_metadata: Marks a metadata record as disabled.
 * Validates caller is stream owner and prevents disabling readonly keys.
 */
CREATE OR REPLACE ACTION disable_metadata(
    -- not necessarily the caller is the original deployer of the stream
    $data_provider TEXT,
    $stream_id TEXT,
    $row_id UUID
) PUBLIC {
    -- Check if caller is the stream owner
    if !is_stream_owner($data_provider, $stream_id, @caller) {
        ERROR('Only stream owner can disable metadata');
    }
    
    $current_block INT := @height;
    $found BOOL := false;
    $metadata_key TEXT;
    
    -- Get the metadata key first to avoid nested queries
    for $metadata_row in SELECT metadata_key
        FROM metadata
        WHERE row_id = $row_id 
        AND data_provider = $data_provider
        AND stream_id = $stream_id
        AND disabled_at IS NULL
        LIMIT 1 {
        
        $found := true;
        $metadata_key := $metadata_row.metadata_key;
    }
    
    if $found = false {
        ERROR('Metadata record not found');
    }
    
    -- In a separate step, check if the key is read-only
    $is_readonly BOOL := false;
    for $readonly_row in SELECT * FROM metadata 
        WHERE data_provider = $data_provider 
        AND stream_id = $stream_id 
        AND metadata_key = 'readonly_key' 
        AND value_s = $metadata_key LIMIT 1 {
        $is_readonly := true;
    }
    
    if $is_readonly = true {
        ERROR('Cannot disable read-only metadata');
    }
    
    -- Update the metadata to mark it as disabled
    UPDATE metadata SET disabled_at = $current_block
    WHERE row_id = $row_id
    AND data_provider = $data_provider
    AND stream_id = $stream_id;
};

/**
 * check_stream_id_format: Validates stream ID format (st + 30 alphanumeric chars).
 */
CREATE OR REPLACE ACTION check_stream_id_format(
    $stream_id TEXT
) PUBLIC view returns (result BOOL) {
    -- Check that the stream_id is exactly 32 characters and starts with "st"
    if LENGTH($stream_id) != 32 OR substring($stream_id, 1, 2) != 'st' {
        return false;
    }

    -- Iterate through each character after the "st" prefix.
    for $i in 3..32 {
        $c TEXT := substring($stream_id, $i, 1);
        if NOT (
            ($c >= '0' AND $c <= '9')
            OR ($c >= 'a' AND $c <= 'z')
        ) {
            return false;
        }
    }

    return true;
};

/**
 * check_ethereum_address: Validates Ethereum address format.
 */
CREATE OR REPLACE ACTION check_ethereum_address(
    $data_provider TEXT
) PUBLIC view returns (result BOOL) {
    -- Verify the address is exactly 42 characters and starts with "0x"
    if LENGTH($data_provider) != 42 OR substring($data_provider, 1, 2) != '0x' {
        return false;
    }

    -- Iterate through each character after the "0x" prefix.
    for $i in 3..42 {
        $c TEXT := substring($data_provider, $i, 1);
        if NOT (
            ($c >= '0' AND $c <= '9')
            OR ($c >= 'a' AND $c <= 'f')
            OR ($c >= 'A' AND $c <= 'F')
        ) {
            return false;
        }
    }

    return true;
};

/**
 * delete_stream: Removes a stream and all associated data.
 * Only stream owner can perform this action.
 */
CREATE OR REPLACE ACTION delete_stream(
    -- not necessarily the caller is the original deployer of the stream
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC {
     if !is_stream_owner($data_provider, $stream_id, @caller) {
        ERROR('Only stream owner can delete the stream');
    }

    DELETE FROM streams WHERE data_provider = $data_provider AND stream_id = $stream_id;
};

/**
 * is_stream_owner: Checks if caller is the owner of a stream.
 * Uses stream_owner metadata to determine ownership.
 */
CREATE OR REPLACE ACTION is_stream_owner(
    $data_provider TEXT,
    $stream_id TEXT,
    $caller TEXT
) PUBLIC view returns (is_owner BOOL) {
    $lower_caller := LOWER($caller);
    $result BOOL := false;
    for $row in get_metadata(
        $data_provider,
        $stream_id,
        'stream_owner',
        $lower_caller,
        1,
        0,
        'created_at DESC'
    ) {
        $result := true;
    }
    return $result;
};

/**
 * is_primitive_stream: Determines if a stream is primitive or composed.
 */
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

/**
 * get_metadata: Retrieves metadata for a stream with pagination and filtering.
 * Supports ordering by creation time and filtering by key and reference.
 */
CREATE OR REPLACE ACTION get_metadata(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT,
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

/**
 * get_category_streams: Retrieves all streams in a category (composed stream).
 * For primitive streams, returns just the stream itself.
 * For composed streams, recursively traverses taxonomy to find all substreams.
 * It doesn't check for the existence of the substreams, it just returns them.
 */
CREATE OR REPLACE ACTION get_category_streams(
    $data_provider TEXT,
    $stream_id TEXT,
    $active_from INT,
    $active_to INT
) PUBLIC view returns table(data_provider TEXT, stream_id TEXT) {
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    if is_primitive_stream($data_provider, $stream_id) == true  {
        return SELECT $data_provider as data_provider, $stream_id as stream_id;
    }

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

/**
 * stream_exists: Simple check if a stream exists in the database.
 */
CREATE OR REPLACE ACTION stream_exists(
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns (result BOOL) {
    for $row in SELECT 1 FROM streams WHERE data_provider = $data_provider AND stream_id = $stream_id {
        return true;
    }
    return false;
};

CREATE OR REPLACE ACTION transfer_stream_ownership(
    $data_provider TEXT,
    $stream_id TEXT,
    $new_owner TEXT
) PUBLIC {
    if !is_stream_owner($data_provider, $stream_id, @caller) {
        ERROR('Only stream owner can transfer ownership');
    }

    -- Check if new owner is a valid ethereum address
    if NOT check_ethereum_address($new_owner) {
        ERROR('Invalid new owner address. Must be a valid Ethereum address: ' || $new_owner);
    }

    -- Update the stream_owner metadata
    UPDATE metadata SET value_ref = LOWER($new_owner)
    WHERE metadata_key = 'stream_owner'
    AND data_provider = $data_provider
    AND stream_id = $stream_id;
};