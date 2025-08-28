/**
 * create_data_provider: Register new data provider
 */
CREATE OR REPLACE ACTION create_data_provider(
    $address TEXT
) PUBLIC {
    $lower_caller TEXT := LOWER(@caller);
    -- Permission Check: Ensure caller has the 'system:network_writer' role.
    $has_permission BOOL := false;
    for $row in are_members_of('system', 'network_writer', ARRAY[$lower_caller]) {
        if $row.wallet = $lower_caller AND $row.is_member {
            $has_permission := true;
            break;
        }
    }
    if NOT $has_permission {
        ERROR('Caller does not have the required system:network_writer role to create data provider.');
    }

    $lower_address TEXT := LOWER($address);

    -- Check if address provided is a valid ethereum address
    if NOT check_ethereum_address($lower_address) {
        ERROR('Invalid data provider address. Must be a valid Ethereum address: ' || $lower_address);
    }

    INSERT INTO data_providers (id, address, created_at) 
    SELECT 
        COALESCE(MAX(id), 0) + 1,
        $lower_address,
        @height
    FROM data_providers
    ON CONFLICT DO NOTHING;
};

/**
 * create_stream: Creates a new stream with required metadata.
 * Validates stream_id format, data provider address, and stream type.
 * Sets default metadata including type, owner, visibility, and readonly keys.
 */
CREATE OR REPLACE ACTION create_stream(
    $stream_id TEXT,
    $stream_type TEXT
) PUBLIC {
    -- Delegate to batch implementation for single-stream consistency.
    create_streams( ARRAY[$stream_id], ARRAY[$stream_type] );
};

/**
 * create_streams: Creates multiple streams at once.
 * Validates stream_id format, data provider address, and stream type.
 * Sets default metadata including type, owner, visibility, and readonly keys.
 */
CREATE OR REPLACE ACTION create_streams(
    $stream_ids TEXT[],
    $stream_types TEXT[]
) PUBLIC {
    $lower_caller TEXT := LOWER(@caller);
    -- Permission Check: Ensure caller has the 'system:network_writer' role.
    $has_permission BOOL := false;
    for $row in are_members_of('system', 'network_writer', ARRAY[$lower_caller]) {
        if $row.wallet = $lower_caller AND $row.is_member {
            $has_permission := true;
            break;
        }
    }
    if NOT $has_permission {
        ERROR('Caller does not have the required system:network_writer role to create streams.');
    }

    -- Get caller's address (data provider) first
    $data_provider TEXT := $lower_caller;

    -- Check if caller is a valid ethereum address
    if NOT check_ethereum_address($data_provider) {
        ERROR('Invalid data provider address. Must be a valid Ethereum address: ' || $data_provider);
    }

    -- Check if stream_ids and stream_types arrays have the same length
    if array_length($stream_ids) != array_length($stream_types) {
        ERROR('Stream IDs and stream types arrays must have the same length');
    }

    -- Validate stream IDs
    for $validation_result in validate_stream_ids_format_batch($stream_ids) {
        if NOT $validation_result.is_valid {
            ERROR('Invalid stream_id format: ' || $validation_result.stream_id || ' - ' ||
                  $validation_result.error_reason);
        }
    }

    -- Validate stream types using dedicated private function
    for $validation_result in validate_stream_types_batch($stream_types) {
        ERROR('Invalid stream type at position ' || $validation_result.position || ': ' ||
              $validation_result.stream_type || ' - ' || $validation_result.error_reason);
    }

    $base_uuid := uuid_generate_kwil('create_streams_' || @txid);

    -- Get the data provider id
    $data_provider_id INT;
    $dp_found BOOL := false;
    for $data_provider_row in SELECT id
        FROM data_providers
        WHERE address = $data_provider
        LIMIT 1 {
        $dp_found := true;
        $data_provider_id := $data_provider_row.id;
    }

    if $dp_found = false {
        ERROR('Data provider not found: ' || $data_provider);
    }
    
    -- Create the streams using UNNEST for optimal performance
    INSERT INTO streams (id, data_provider_id, data_provider, stream_id, stream_type, created_at)
    SELECT
        ROW_NUMBER() OVER (ORDER BY t.stream_id) + COALESCE((SELECT MAX(id) FROM streams), 0) AS id,
        $data_provider_id,
        $data_provider,
        t.stream_id,
        t.stream_type,
        @height
    FROM UNNEST($stream_ids, $stream_types) AS t(stream_id, stream_type);
 
    -- Create metadata for the streams using UNNEST for optimal performance
    -- Insert stream_owner metadata
    INSERT INTO metadata (
        row_id,
        metadata_key,
        value_i,
        value_f,
        value_b,
        value_s,
        value_ref,
        created_at,
        disabled_at,
        stream_ref
    )
    SELECT
        uuid_generate_v5($base_uuid, 'metadata' || $data_provider || t.stream_id || 'stream_owner' || '1')::UUID,
        'stream_owner'::TEXT,
        NULL::INT,
        NULL::NUMERIC(36,18),
        NULL::BOOL,
        NULL::TEXT,
        LOWER($data_provider)::TEXT,
        @height,
        NULL::INT,
        s.id
    FROM UNNEST($stream_ids, $stream_types) AS t(stream_id, stream_type)
    JOIN data_providers dp ON dp.address = $data_provider
    JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id;

    -- Insert read_visibility metadata
    INSERT INTO metadata (
        row_id,
        metadata_key,
        value_i,
        value_f,
        value_b,
        value_s,
        value_ref,
        created_at,
        disabled_at,
        stream_ref
    )
    SELECT
        uuid_generate_v5($base_uuid, 'metadata' || $data_provider || t.stream_id || 'read_visibility' || '2')::UUID,
        'read_visibility'::TEXT,
        0::INT,
        NULL::NUMERIC(36,18),
        NULL::BOOL,
        NULL::TEXT,
        NULL::TEXT,
        @height,
        NULL::INT,
        s.id
    FROM UNNEST($stream_ids, $stream_types) AS t(stream_id, stream_type)
    JOIN data_providers dp ON dp.address = $data_provider
    JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id;

    -- Insert readonly_key metadata (stream_owner)
    INSERT INTO metadata (
        row_id,
        metadata_key,
        value_i,
        value_f,
        value_b,
        value_s,
        value_ref,
        created_at,
        disabled_at,
        stream_ref
    )
    SELECT
        uuid_generate_v5($base_uuid, 'metadata' || $data_provider || t.stream_id || 'readonly_key' || '3')::UUID,
        'readonly_key'::TEXT,
        NULL::INT,
        NULL::NUMERIC(36,18),
        NULL::BOOL,
        'stream_owner'::TEXT,
        NULL::TEXT,
        @height,
        NULL::INT,
        s.id
    FROM UNNEST($stream_ids, $stream_types) AS t(stream_id, stream_type)
    JOIN data_providers dp ON dp.address = $data_provider
    JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id;

    -- Insert readonly_key metadata (readonly_key)
    INSERT INTO metadata (
        row_id,
        metadata_key,
        value_i,
        value_f,
        value_b,
        value_s,
        value_ref,
        created_at,
        disabled_at,
        stream_ref
    )
    SELECT
        uuid_generate_v5($base_uuid, 'metadata' || $data_provider || t.stream_id || 'readonly_key' || '4')::UUID,
        'readonly_key'::TEXT,
        NULL::INT,
        NULL::NUMERIC(36,18),
        NULL::BOOL,
        'readonly_key'::TEXT,
        NULL::TEXT,
        @height,
        NULL::INT,
        s.id
    FROM UNNEST($stream_ids, $stream_types) AS t(stream_id, stream_type)
    JOIN data_providers dp ON dp.address = $data_provider
    JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id;

    -- Insert type metadata
    INSERT INTO metadata (
        row_id,
        metadata_key,
        value_i,
        value_f,
        value_b,
        value_s,
        value_ref,
        created_at,
        disabled_at,
        stream_ref
    )
    SELECT
        uuid_generate_v5($base_uuid, 'metadata' || $data_provider || t.stream_id || 'type' || '5')::UUID,
        'type'::TEXT,
        NULL::INT,
        NULL::NUMERIC(36,18),
        NULL::BOOL,
        t.stream_type,
        NULL::TEXT,
        @height,
        NULL::INT,
        s.id
    FROM UNNEST($stream_ids, $stream_types) AS t(stream_id, stream_type)
    JOIN data_providers dp ON dp.address = $data_provider
    JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id;

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
    $data_provider := LOWER($data_provider);
    $lower_caller := LOWER(@caller);
    
    -- Check if caller is the stream owner
    if !is_stream_owner($data_provider, $stream_id, $lower_caller) {
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

    $stream_ref := get_stream_id($data_provider, $stream_id);
    
    -- Check if the key is read-only
    $is_readonly BOOL := false;
    for $row in SELECT * FROM metadata 
        WHERE stream_ref = $stream_ref
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
    $stream_ref INT := get_stream_id($data_provider, $stream_id);
    
    -- Insert the metadata
    INSERT INTO metadata (
        row_id, 
        metadata_key, 
        value_i, 
        value_f, 
        value_s, 
        value_b, 
        value_ref, 
        created_at,
        stream_ref
    ) VALUES (
        $uuid, 
        $key, 
        $value_i, 
        $value_f, 
        $value_s, 
        $value_b, 
        LOWER($value_ref), 
        $current_block,
        $stream_ref
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
    $data_provider := LOWER($data_provider);
    $lower_caller := LOWER(@caller);
    -- Check if caller is the stream owner
    if !is_stream_owner($data_provider, $stream_id, $lower_caller) {
        ERROR('Only stream owner can disable metadata');
    }
    
    $current_block INT := @height;
    $found BOOL := false;
    $metadata_key TEXT;
    $stream_ref INT := get_stream_id($data_provider, $stream_id);
    
    -- Get the metadata key first to avoid nested queries
    for $metadata_row in SELECT metadata_key
        FROM metadata
        WHERE row_id = $row_id 
        AND stream_ref = $stream_ref
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
        WHERE stream_ref = $stream_ref
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
    AND stream_ref = $stream_ref;
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
 * validate_stream_ids_format_batch: Validates multiple stream ID formats efficiently.
 * Returns all stream IDs with their validation status and error details.
 */
CREATE OR REPLACE ACTION validate_stream_ids_format_batch(
    $stream_ids TEXT[]
) PUBLIC view returns table(
    stream_id TEXT,
    is_valid BOOL,
    error_reason TEXT
) {
    -- Pure SQL validation using supported functions only
    RETURN SELECT
        t.stream_id,
        CASE
            WHEN LENGTH(t.stream_id) != 32 THEN false
            WHEN substring(t.stream_id, 1, 2) != 'st' THEN false
            -- Check characters 3-32 are lowercase alphanumeric using basic string functions
            WHEN LENGTH(trim(lower(substring(t.stream_id, 3, 30)), '0123456789abcdefghijklmnopqrstuvwxyz')) != 0 THEN false
            ELSE true
        END AS is_valid,
        CASE
            WHEN LENGTH(t.stream_id) != 32 THEN 'Invalid length (must be 32 characters)'
            WHEN substring(t.stream_id, 1, 2) != 'st' THEN 'Must start with "st"'
            -- Check characters 3-32 are lowercase alphanumeric using basic string functions
            WHEN LENGTH(trim(lower(substring(t.stream_id, 3, 30)), '0123456789abcdefghijklmnopqrstuvwxyz')) != 0 THEN 'Characters 3-32 must be lowercase alphanumeric'
            ELSE ''
        END AS error_reason
    FROM UNNEST($stream_ids) AS t(stream_id);
};

/**
 * validate_stream_types_batch: Validates multiple stream types efficiently.
 * Returns invalid stream types with their positions and error details.
 */
CREATE OR REPLACE ACTION validate_stream_types_batch(
    $stream_types TEXT[]
) PRIVATE view returns table(
    position INT,
    stream_type TEXT,
    error_reason TEXT
) {
    -- Use CTE with row_number() since WITH ORDINALITY is not supported in Kuneiform
    RETURN WITH indexed_types AS (
        SELECT
            row_number() OVER () as idx,
            stream_type
        FROM UNNEST($stream_types) AS t(stream_type)
    )
    SELECT
        idx as position,
        stream_type,
        CASE
            WHEN stream_type NOT IN ('primitive', 'composed') THEN
                'Stream type must be "primitive" or "composed"'
            ELSE ''
        END AS error_reason
    FROM indexed_types
    WHERE stream_type NOT IN ('primitive', 'composed');
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
    $data_provider := LOWER($data_provider);
    $lower_caller := LOWER(@caller);

    if !is_stream_owner($data_provider, $stream_id, $lower_caller) {
        ERROR('Only stream owner can delete the stream');
    }

    $stream_ref := get_stream_id($data_provider, $stream_id);

    DELETE FROM streams WHERE id = $stream_ref;
};

/**
 * is_stream_owner: Checks if caller is the owner of a stream.
 * Uses stream_owner metadata to determine ownership.
 */
CREATE OR REPLACE ACTION is_stream_owner_core(
    $stream_ref INT,
    $caller TEXT
) PRIVATE view returns (is_owner BOOL) {
    -- Check if the caller is the owner by looking at the latest stream_owner metadata
    for $row in SELECT COALESCE(
        -- do not use LOWER here, or it will break the index lookup
        (SELECT m.value_ref = LOWER($caller)
         FROM metadata m
         WHERE m.stream_ref = $stream_ref
           AND m.metadata_key = 'stream_owner'
           AND m.disabled_at IS NULL
         ORDER BY m.created_at DESC
         LIMIT 1), false
    ) as result {
        return $row.result;
    }
    return false;
};

CREATE OR REPLACE ACTION is_stream_owner(
    $data_provider TEXT,
    $stream_id TEXT,
    $caller TEXT
) PUBLIC view returns (is_owner BOOL) {
    $data_provider := LOWER($data_provider);
    $lower_caller := LOWER($caller);

    -- Check if the stream exists (get_stream_id returns NULL if stream doesn't exist)
    $stream_ref := get_stream_id($data_provider, $stream_id);
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }
    return is_stream_owner_core($stream_ref, $lower_caller);
};

/**
 * is_stream_owner_batch: Checks if a wallet is the owner of multiple streams.
 * Processes arrays of data providers and stream IDs efficiently.
 * Returns a table indicating ownership status for each stream.
 */
CREATE OR REPLACE ACTION is_stream_owner_batch(
    $data_providers TEXT[],
    $stream_ids TEXT[],
    $wallet TEXT
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT,
    is_owner BOOL
) {
    -- Lowercase data providers directly using UNNEST for efficiency

    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }

    -- Check if the wallet is the owner of each stream
    for $row in stream_exists_batch($data_providers, $stream_ids) {
        if !$row.stream_exists {
            ERROR('stream does not exist: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }
    $lowercase_wallet TEXT := LOWER($wallet);

    -- Use UNNEST for optimal performance with direct LOWER operations
    SELECT
        t.data_provider,
        t.stream_id,
        CASE WHEN m.value_ref IS NOT NULL AND m.value_ref = $wallet THEN true ELSE false END AS is_owner
    FROM UNNEST($data_providers, $stream_ids) AS t(data_provider, stream_id)
    LEFT JOIN (
        SELECT dp.address as data_provider, s.stream_id, md.value_ref
        FROM metadata md
        JOIN streams s ON md.stream_ref = s.id
        JOIN data_providers dp ON s.data_provider_id = dp.id
        WHERE md.metadata_key = 'stream_owner'
          AND md.disabled_at IS NULL
        ORDER BY md.created_at DESC
    ) m ON LOWER(t.data_provider) = m.data_provider AND t.stream_id = m.stream_id;
};

/**
 * is_primitive_stream: Determines if a stream is primitive or composed.
 */
CREATE OR REPLACE ACTION is_primitive_stream(
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns (is_primitive BOOL) {
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);
    for $row in SELECT stream_type FROM streams
        WHERE id = $stream_ref LIMIT 1 {
        return $row.stream_type = 'primitive';
    }
    
    ERROR('Stream not found: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
};

/**
 * is_primitive_stream_batch: Checks if multiple streams are primitive in a single query.
 * Returns a table with primitive status for each stream.
 * Only checks streams that exist - does not error on non-existent streams.
 */
CREATE OR REPLACE ACTION is_primitive_stream_batch(
    $data_providers TEXT[],
    $stream_ids TEXT[]
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT,
    is_primitive BOOL
) {
    -- Lowercase data providers directly using UNNEST for efficiency

    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }

    -- Use UNNEST for optimal performance with direct LOWER operations
    SELECT
        t.data_provider,
        t.stream_id,
        COALESCE(s.stream_type = 'primitive', false) AS is_primitive
    FROM UNNEST($data_providers, $stream_ids) AS t(data_provider, stream_id)
    LEFT JOIN data_providers dp ON dp.address = LOWER(t.data_provider)
    LEFT JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id;
};

/**
 * get_metadata: Retrieves metadata for a stream with pagination and filtering.
 * Supports ordering by creation time and filtering by key and reference.
 */
CREATE OR REPLACE ACTION get_metadata_core(
    $stream_ref INT,
    $key TEXT,
    $ref TEXT,
    $limit INT,
    $offset INT,
    $order_by TEXT
) PRIVATE view returns table(
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
            -- do not use LOWER on value_ref, or it will break the index lookup
            AND ($ref IS NULL OR value_ref = LOWER($ref))
            AND stream_ref = $stream_ref
       ORDER BY
               CASE WHEN $order_by = 'created_at DESC' THEN created_at END DESC,
               CASE WHEN $order_by = 'created_at ASC' THEN created_at END ASC
       LIMIT $limit OFFSET $offset;
};

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
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);
    for $row in get_metadata_core($stream_ref, $key, $ref, $limit, $offset, $order_by) {
        RETURN NEXT $row.row_id, $row.value_i, $row.value_f, $row.value_b, $row.value_s, $row.value_ref, $row.created_at;
    }
};

-- Compatibility wrapper that returns single value from get_metadata_core (for functions expecting single value)
CREATE OR REPLACE ACTION get_metadata_priv_single(
    $stream_ref INT,
    $key TEXT,
    $ref TEXT
) PRIVATE view returns (value TEXT) {
    $result TEXT := NULL;
    for $row in get_metadata_core($stream_ref, $key, $ref, 1, 0, 'created_at DESC') {
        $result := $row.value_ref;
    }
    RETURN $result;
};

/**
 * get_latest_metadata: Retrieves the latest metadata for a stream.
 */
CREATE OR REPLACE ACTION get_latest_metadata_core(
    $stream_ref INT,
    $key TEXT,
    $ref TEXT
) PRIVATE view returns table(
    value_i INT,
    value_f NUMERIC(36,18),
    value_b BOOL,
    value_s TEXT,
    value_ref TEXT
) {
    for $row in get_metadata_core($stream_ref, $key, $ref, 1, 0, 'created_at DESC') {
        RETURN NEXT $row.value_i, $row.value_f, $row.value_b, $row.value_s, $row.value_ref;
    }
};

CREATE OR REPLACE ACTION get_latest_metadata(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT,
    $ref TEXT
) PUBLIC view returns table(
    value_i INT,
    value_f NUMERIC(36,18),
    value_b BOOL,
    value_s TEXT,
    value_ref TEXT
) {
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    for $row in get_latest_metadata_core($stream_ref, $key, $ref) {
        RETURN NEXT $row.value_i, $row.value_f, $row.value_b, $row.value_s, $row.value_ref;
    }
};

/**
 * get_latest_metadata_int: Retrieves the latest metadata value for a stream.
 */
CREATE OR REPLACE ACTION get_latest_metadata_int_core(
    $stream_ref INT,
    $key TEXT
) PRIVATE view returns (value INT) {
    $result INT := NULL;
    for $row in get_latest_metadata_core($stream_ref, $key, NULL) {
        $result := $row.value_i;
    }
    RETURN $result;
};

CREATE OR REPLACE ACTION get_latest_metadata_int(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT
) PUBLIC view returns (value INT) {
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);
    return get_latest_metadata_int_core($stream_ref, $key);
};

/**
 * get_latest_metadata_ref: Retrieves the latest metadata value for a stream.
 */
CREATE OR REPLACE ACTION get_latest_metadata_ref_core(
    $stream_ref INT,
    $key TEXT,
    $ref TEXT
) PRIVATE view returns (value TEXT) {
    $result TEXT := NULL;
    for $row in get_latest_metadata_core($stream_ref, $key, $ref) {
        $result := $row.value_ref;
    }
    RETURN $result;
};

CREATE OR REPLACE ACTION get_latest_metadata_ref(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT,
    $ref TEXT
) PUBLIC view returns (value TEXT) {
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);
    return get_latest_metadata_ref_core($stream_ref, $key, $ref);
};

/**
 * get_latest_metadata_bool: Retrieves the latest metadata value for a stream.
 */
CREATE OR REPLACE ACTION get_latest_metadata_bool_core(
    $stream_ref INT,
    $key TEXT
) PRIVATE view returns (value BOOL) {
    $result BOOL := NULL;
    for $row in get_latest_metadata_core($stream_ref, $key, NULL) {
        $result := $row.value_b;
    }
    RETURN $result;
};

CREATE OR REPLACE ACTION get_latest_metadata_bool(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT
) PUBLIC view returns (value BOOL) {
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);
    return get_latest_metadata_bool_core($stream_ref, $key);
};

/**
 * get_latest_metadata_string: Retrieves the latest metadata value for a stream.
 */
CREATE OR REPLACE ACTION get_latest_metadata_string_core(
    $stream_ref INT,
    $key TEXT
) PRIVATE view returns (value TEXT) {
    $result TEXT := NULL;
    for $row in get_latest_metadata_core($stream_ref, $key, NULL) {
        $result := $row.value_s;
    }
    RETURN $result;
};

CREATE OR REPLACE ACTION get_latest_metadata_string(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT
) PUBLIC view returns (value TEXT) {
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);
    return get_latest_metadata_string_core($stream_ref, $key);
};

/**
 * get_category_streams: Retrieves all streams in a category (composed stream).
 * For primitive streams, returns just the stream itself.
 * For composed streams, recursively traverses taxonomy to find all substreams.
 * It doesn't check for the existence of the substreams, it just returns them.
 */
CREATE OR REPLACE ACTION get_category_streams(
    $data_provider TEXT,
    $stream_id     TEXT,
    $active_from   INT,
    $active_to     INT
) PUBLIC view returns table(data_provider TEXT, stream_id TEXT) {
    $data_provider := LOWER($data_provider);

    -- Check if stream exists (get_stream_id returns NULL if stream doesn't exist)
    $stream_ref := get_stream_id($data_provider, $stream_id);
    IF $stream_ref IS NULL {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

    -- Always return itself first
    RETURN NEXT $data_provider, $stream_id;

    -- For primitive streams, just return the stream itself
    if is_primitive_stream($data_provider, $stream_id) == true {
        RETURN;
    }

    -- Set boundaries for time intervals
    $max_int8 INT := 9223372036854775000;
    $effective_active_from INT := COALESCE($active_from, 0);
    $effective_active_to INT := COALESCE($active_to, $max_int8);

    -- Get all substreams with proper recursive traversal
    return WITH RECURSIVE substreams AS (
        /*------------------------------------------------------------------
         * (1) Base Case: overshadow logic for ($data_provider, $stream_id).
         *     - For each distinct start_time, pick the row with the max group_sequence.
         *     - next_start is used to define [group_sequence_start, group_sequence_end].
         *------------------------------------------------------------------*/
        SELECT
            base.stream_ref,
            base.child_stream_ref,
            
            -- The interval during which this row is active:
            base.start_time            AS group_sequence_start,
            COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
        FROM (
            -- Find rows with maximum group_sequence for each start_time
            SELECT
                t.stream_ref,
                t.child_stream_ref,
                t.start_time,
                t.group_sequence,
                MAX(t.group_sequence) OVER (
                    PARTITION BY t.stream_ref, t.start_time
                ) AS max_group_sequence
            FROM taxonomies t
            WHERE t.stream_ref   = $stream_ref
              AND t.disabled_at  IS NULL
              AND t.start_time   <= $effective_active_to
              AND t.start_time   >= COALESCE((
                    -- Find the most recent taxonomy at or before effective_active_from
                    SELECT t2.start_time
                    FROM taxonomies t2
                    WHERE t2.stream_ref = t.stream_ref
                      AND t2.disabled_at   IS NULL
                      AND t2.start_time   <= $effective_active_from
                    ORDER BY t2.start_time DESC, t2.group_sequence DESC
                    LIMIT 1
                  ), 0
              )
        ) base
        JOIN (
            /* Distinct start_times for top-level (dp, sid), used for LEAD() */
            SELECT
                dt.stream_ref,
                dt.start_time,
                LEAD(dt.start_time) OVER (
                    PARTITION BY dt.stream_ref
                    ORDER BY dt.start_time
                ) AS next_start
            FROM (
                SELECT DISTINCT
                    t.stream_ref,
                    t.start_time
                FROM taxonomies t
                WHERE t.stream_ref = $stream_ref
                  AND t.disabled_at   IS NULL
                  AND t.start_time   <= $effective_active_to
                  AND t.start_time   >= COALESCE((
                        SELECT t2.start_time
                        FROM taxonomies t2
                        WHERE t2.stream_ref = t.stream_ref
                          AND t2.disabled_at   IS NULL
                          AND t2.start_time   <= $effective_active_from
                        ORDER BY t2.start_time DESC, t2.group_sequence DESC
                        LIMIT 1
                      ), 0
                  )
            ) dt
        ) ot
          ON base.stream_ref = ot.stream_ref
         AND base.start_time    = ot.start_time
        WHERE base.group_sequence = base.max_group_sequence

        UNION

        /*------------------------------------------------------------------
         * (2) Recursive Child-Level Overshadow:
         *     For each discovered child, gather overshadow rows for that child
         *     and produce intervals that overlap the parent's own active interval.
         *------------------------------------------------------------------*/
        SELECT
            parent.stream_ref,
            child.child_stream_ref,

            -- Intersection of parent's active interval and child's:
            GREATEST(parent.group_sequence_start, child.start_time)    AS group_sequence_start,
            LEAST(parent.group_sequence_end, child.group_sequence_end) AS group_sequence_end
        FROM substreams parent
        JOIN (
            /* Child overshadow logic, same pattern as above but for child dp/sid. */
            SELECT
                base.stream_ref,
                base.child_stream_ref,
                base.start_time,
                COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
            FROM (
                SELECT
                    t.stream_ref,
                    t.child_stream_ref,
                    t.start_time,
                    t.group_sequence,
                    MAX(t.group_sequence) OVER (
                        PARTITION BY t.stream_ref, t.start_time
                    ) AS max_group_sequence
                FROM taxonomies t
                WHERE t.disabled_at IS NULL
                  AND t.start_time <= $effective_active_to
                  AND t.start_time >= COALESCE((
                        -- Most recent taxonomy at or before effective_from
                        SELECT t2.start_time
                        FROM taxonomies t2
                        WHERE t2.stream_ref = t.stream_ref
                          AND t2.disabled_at   IS NULL
                          AND t2.start_time   <= $effective_active_from
                        ORDER BY t2.start_time DESC, t2.group_sequence DESC
                        LIMIT 1
                      ), 0
                  )
            ) base
            JOIN (
                /* Distinct start_times at child level */
                SELECT
                    dt.stream_ref,
                    dt.start_time,
                    LEAD(dt.start_time) OVER (
                        PARTITION BY dt.stream_ref
                        ORDER BY dt.start_time
                    ) AS next_start
                FROM (
                    SELECT DISTINCT
                        t.stream_ref,
                        t.start_time
                    FROM taxonomies t
                    WHERE t.disabled_at   IS NULL
                      AND t.start_time   <= $effective_active_to
                      AND t.start_time   >= COALESCE((
                            SELECT t2.start_time
                            FROM taxonomies t2
                            WHERE t2.stream_ref = t.stream_ref
                              AND t2.disabled_at   IS NULL
                              AND t2.start_time   <= $effective_active_from
                            ORDER BY t2.start_time DESC, t2.group_sequence DESC
                            LIMIT 1
                          ), 0
                      )
                ) dt
            ) ot
              ON base.stream_ref = ot.stream_ref
             AND base.start_time    = ot.start_time
            WHERE base.group_sequence = base.max_group_sequence
        ) child
          ON child.stream_ref = parent.child_stream_ref
        
        /* Overlap check: child's interval must intersect parent's */
        WHERE child.start_time         <= parent.group_sequence_end
          AND child.group_sequence_end >= parent.group_sequence_start
    )
    SELECT DISTINCT 
        child_dp.address as child_data_provider, 
        child_s.stream_id as child_stream_id
    FROM substreams sub
    JOIN streams child_s ON sub.child_stream_ref = child_s.id
    JOIN data_providers child_dp ON child_s.data_provider_id = child_dp.id;
};

/**
 * stream_exists: Simple check if a stream exists in the database.
 */
CREATE OR REPLACE ACTION stream_exists(
    $data_provider TEXT,
    $stream_id TEXT
) PUBLIC view returns (result BOOL) {
    $data_provider := LOWER($data_provider);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    for $row in SELECT 1 FROM streams WHERE id = $stream_ref {
        return true;
    }
    return false;
};

/**
 * stream_exists_batch_core: Private version that uses stream refs directly.
 * Checks if multiple streams exist using their stream references.
 * Returns false if any stream refs are null (indicating non-existent streams).
 * Returns true only if all streams exist and no stream refs are null.
 */
CREATE OR REPLACE ACTION stream_exists_batch_core(
    $stream_refs INT[]
) PRIVATE VIEW RETURNS (result BOOL) {
    -- Use UNNEST for efficient batch processing
    for $row in SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM UNNEST($stream_refs) AS t(stream_ref) WHERE t.stream_ref IS NULL
        ) THEN false
        ELSE NOT EXISTS (
            SELECT 1
            FROM UNNEST($stream_refs) AS t(stream_ref)
            LEFT JOIN streams s ON s.id = t.stream_ref
            WHERE s.id IS NULL
              AND t.stream_ref IS NOT NULL
        )
    END AS result
    FROM (SELECT 1) dummy {
        return $row.result;
    }
    return false;
};

/**
 * is_primitive_stream_batch_core: Private version that uses stream refs directly.
 * Checks if multiple streams are primitive using their stream references.
 * Returns false if any stream refs are null (indicating non-existent streams).
 * Returns true only if all streams exist and are primitive.
 */
CREATE OR REPLACE ACTION is_primitive_stream_batch_core(
    $stream_refs INT[]
) PRIVATE VIEW RETURNS (result BOOL) {
    -- Use UNNEST for optimal performance - direct array processing without recursion
    for $row in SELECT CASE
        WHEN EXISTS (
            SELECT 1
            FROM UNNEST($stream_refs) AS t(stream_ref)
            WHERE t.stream_ref IS NULL
        ) THEN false
        ELSE NOT EXISTS (
            SELECT 1
            FROM UNNEST($stream_refs) AS t(stream_ref)
            JOIN streams s ON s.id = t.stream_ref
            WHERE s.stream_type != 'primitive'
              AND t.stream_ref IS NOT NULL
        )
    END AS result
    FROM (SELECT 1) dummy {
        return $row.result;
    }
    return false;
};

/**
 * stream_exists_batch: Checks existence of multiple streams in a single query.
 * Returns a table with existence status for each stream.
 */
CREATE OR REPLACE ACTION stream_exists_batch(
    $data_providers TEXT[],
    $stream_ids TEXT[]
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT,
    stream_exists BOOL
) {
    -- Lowercase data providers directly using UNNEST for efficiency

    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }

    -- Use UNNEST for optimal performance with direct LOWER operations
    RETURN SELECT
        t.data_provider,
        t.stream_id,
        CASE WHEN s.data_provider IS NOT NULL THEN true ELSE false END AS stream_exists
    FROM UNNEST($data_providers, $stream_ids) AS t(data_provider, stream_id)
    LEFT JOIN data_providers dp ON dp.address = LOWER(t.data_provider)
    LEFT JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id;
};

CREATE OR REPLACE ACTION transfer_stream_ownership(
    $data_provider TEXT,
    $stream_id TEXT,
    $new_owner TEXT
) PUBLIC {
    $data_provider := LOWER($data_provider);
    $new_owner := LOWER($new_owner);
    $lower_caller := LOWER(@caller);
    $stream_ref := get_stream_id($data_provider, $stream_id);

    if !is_stream_owner($data_provider, $stream_id, $lower_caller) {
        ERROR('Only stream owner can transfer ownership');
    }

    -- Check if new owner is a valid ethereum address
    if NOT check_ethereum_address($new_owner) {
        ERROR('Invalid new owner address. Must be a valid Ethereum address: ' || $new_owner);
    }

    -- Update the stream_owner metadata
    UPDATE metadata SET value_ref = LOWER($new_owner)
    WHERE metadata_key = 'stream_owner'
    AND stream_ref = $stream_ref;
};

/**
 * filter_streams_by_existence: Filters streams based on existence.
 * Can return either existing or non-existing streams based on existing_only flag.
 * Takes arrays of data providers and stream IDs as input.
 * Uses efficient WITH RECURSIVE pattern for batch processing.
 */
CREATE OR REPLACE ACTION filter_streams_by_existence(
    $data_providers TEXT[],
    $stream_ids TEXT[],
    $existing_only BOOL
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT
) {
    -- Lowercase data providers directly using UNNEST for efficiency

    -- default to return existing streams
    if $existing_only IS NULL {
        $existing_only := true;
    }
    
    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }
    
    -- Use UNNEST for efficient batch processing
    RETURN SELECT
        t.data_provider,
        t.stream_id
    FROM UNNEST($data_providers, $stream_ids) AS t(data_provider, stream_id)
    LEFT JOIN data_providers dp ON dp.address = LOWER(t.data_provider)
    LEFT JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = t.stream_id
    WHERE (CASE WHEN s.id IS NOT NULL THEN true ELSE false END) = $existing_only;
};

CREATE OR REPLACE ACTION list_streams(
    $data_provider TEXT,
    $limit INT,
    $offset INT,
    $order_by TEXT,
    $block_height INT
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT,
    stream_type TEXT,
    created_at INT8
) {
    $data_provider := LOWER($data_provider);

    if $limit > 5000 {
        ERROR('Limit exceeds maximum allowed value of 5000');
    }
    if $limit IS NULL {
        $limit := 5000;
    }
    if $limit == 0 {
        $limit := 5000;
    }
    if $offset IS NULL {
        $offset := 0;
    }
    if $order_by IS NULL {
        $order_by := 'created_at DESC';
    }
    if $order_by == '' {
        $order_by := 'created_at DESC';
    }

    RETURN  SELECT 
              dp.address as data_provider,
              s.stream_id,
              s.stream_type,
              s.created_at
            FROM streams s
            JOIN data_providers dp ON s.data_provider_id = dp.id
            -- do not use LOWER on dp.address, or it will break the index lookup
            WHERE ($data_provider IS NULL OR $data_provider = '' OR dp.address = LOWER($data_provider))
            AND s.created_at > $block_height
            ORDER BY
               CASE WHEN $order_by = 'created_at DESC' THEN s.created_at END DESC,
               CASE WHEN $order_by = 'created_at ASC' THEN s.created_at END ASC,
               CASE WHEN $order_by = 'stream_id ASC' THEN stream_id END ASC,
               CASE WHEN $order_by = 'stream_id DESC' THEN stream_id END DESC,
               CASE WHEN $order_by = 'stream_type ASC' THEN stream_type END ASC,
               CASE WHEN $order_by = 'stream_type DESC' THEN stream_type END DESC
               LIMIT $limit OFFSET $offset;
};