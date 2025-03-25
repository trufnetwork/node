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
    $base_uuid := uuid_generate_kwil('create_stream_' || @txid || $stream_id);
    
    -- Add type metadata
    $type_uuid := uuid_generate_v5($base_uuid, 'type');
    $stream_owner_uuid := uuid_generate_v5($base_uuid, 'stream_owner');
    $read_visibility_uuid := uuid_generate_v5($base_uuid, 'read_visibility');
    $readonly_key_stream_owner_uuid := uuid_generate_v5($base_uuid, 'readonly_key:stream_owner');
    $readonly_key_readonly_key_uuid := uuid_generate_v5($base_uuid, 'readonly_key:readonly_key');

    INSERT INTO metadata (row_id, data_provider, stream_id, metadata_key, value_s, value_i, value_ref, created_at) VALUES
        ($type_uuid,                      $data_provider, $stream_id, 'type',                       $stream_type,   NULL, NULL,                  $current_block),
        ($stream_owner_uuid,              $data_provider, $stream_id, 'stream_owner',               NULL,           NULL, LOWER($data_provider), $current_block),
        ($read_visibility_uuid,           $data_provider, $stream_id, 'read_visibility',            NULL,           0,    NULL,                  $current_block),
        ($readonly_key_stream_owner_uuid, $data_provider, $stream_id, 'readonly_key',  'stream_owner', NULL, NULL,                  $current_block),
        ($readonly_key_readonly_key_uuid, $data_provider, $stream_id, 'readonly_key',  'readonly_key', NULL, NULL,                  $current_block);
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
    -- Get caller's address (data provider) first
    $data_provider TEXT := @caller;

    -- Check if caller is a valid ethereum address
    if NOT check_ethereum_address($data_provider) {
        ERROR('Invalid data provider address. Must be a valid Ethereum address: ' || $data_provider);
    }

    -- Check if stream_ids and stream_types arrays have the same length
    if array_length($stream_ids) != array_length($stream_types) {
        ERROR('Stream IDs and stream types arrays must have the same length');
    }

    -- Iterate through each stream ID and type, checking if they have a valid format
    for $i in 1..array_length($stream_ids) {
        $stream_id := $stream_ids[$i];
        $stream_type := $stream_types[$i];
        if NOT check_stream_id_format($stream_id) {
            ERROR('Invalid stream_id format. Must start with "st" followed by 30 lowercase alphanumeric characters: ' || $stream_id);
        }
        if $stream_type != 'primitive' AND $stream_type != 'composed' {
            ERROR('Invalid stream type. Must be "primitive" or "composed": ' || $stream_type);
        }
    }

    $base_uuid := uuid_generate_kwil('create_streams_' || @txid);

    -- Create the streams
    WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($stream_ids)
    ),
    stream_arrays AS (
        SELECT 
            $stream_ids AS stream_ids,
            $stream_types AS stream_types
    ),
    arguments AS (
        SELECT 
            idx,
            stream_arrays.stream_ids[idx] AS stream_id,
            stream_arrays.stream_types[idx] AS stream_type
        FROM indexes
        JOIN stream_arrays ON 1=1
    )
    INSERT INTO streams (data_provider, stream_id, stream_type, created_at)
    SELECT 
        $data_provider, 
        stream_id, 
        stream_type, 
        @height
    FROM arguments;
 
    -- Create metadata for the streams
    WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($stream_ids)
    ),
    stream_arrays AS (
        SELECT 
            $stream_ids AS stream_ids,
            $stream_types AS stream_types
    ),
    stream_metadata AS (
        SELECT 
            stream_arrays.stream_ids[idx] AS stream_id,
            stream_arrays.stream_types[idx] AS stream_type
        FROM indexes
        JOIN stream_arrays ON 1=1
    ),
    metadata_arguments AS (
        -- Don't add type metadata here, we will add it when we join both tables
        SELECT
            'stream_owner' AS metadata_key,
            NULL::TEXT AS value_s,
            NULL::INT AS value_i,
            LOWER($data_provider) AS value_ref
        UNION ALL
        SELECT
            'read_visibility' AS metadata_key,
            NULL::TEXT AS value_s,    
            0::INT AS value_i, -- 0 = public, 1 = private
            NULL::TEXT AS value_ref
        UNION ALL
        SELECT
            'readonly_key:stream_owner' AS metadata_key,
            'stream_owner' AS value_s,
            NULL::INT AS value_i,
            NULL::TEXT AS value_ref
        UNION ALL
        SELECT
            'readonly_key:readonly_key' AS metadata_key,
            'readonly_key' AS value_s,
            NULL::INT AS value_i,
            NULL::TEXT AS value_ref
    ),
    -- Cross join the stream_metadata and metadata_arguments
    all_arguments AS (
        SELECT 
            sm.stream_id,
            sm.stream_type,
            ma.metadata_key,
            ma.value_s,
            ma.value_i,
            ma.value_ref
        FROM stream_metadata sm
        JOIN metadata_arguments ma ON 1=1

        UNION ALL

        SELECT
            stream_metadata.stream_id,
            stream_metadata.stream_type,
            'type' AS metadata_key,
            stream_metadata.stream_type AS value_s,
            NULL::INT AS value_i,
            NULL::TEXT AS value_ref
        FROM stream_metadata
    ),
    -- Add row number to be able to create deterministic UUIDs
    args_with_row_number AS (
        SELECT all_arguments.*, row_number() OVER () AS row_number
        FROM all_arguments
    ),
    args AS (
        SELECT 
            uuid_generate_v5($base_uuid, 'metadata' || arg.row_number::TEXT)::UUID as row_id,
            $data_provider AS data_provider,
            arg.stream_id,
            arg.metadata_key,
            arg.value_s,
            arg.value_i,
            arg.value_ref,
            @height AS created_at
        FROM args_with_row_number arg
    )
    -- catched a bug where it's expected to have the same order of columns
    -- as the table definition
    INSERT INTO metadata (
        row_id,
        data_provider,
        stream_id,
        metadata_key,
        value_i,
        value_f,
        value_b,
        value_s,
        value_ref,
        created_at,
        disabled_at
    )
    SELECT 
        row_id::UUID,
        data_provider,
        stream_id,
        metadata_key,
        value_i,
        NULL::NUMERIC(36,18),
        NULL::BOOLEAN,
        value_s,
        value_ref,
        created_at,
        NULL::INT8
    FROM args;
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

    -- Check if the stream exists
    if !stream_exists($data_provider, $stream_id) {
        ERROR('Stream does not exist: data_provider=' || $data_provider || ' stream_id=' || $stream_id);
    }

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

    -- Use WITH RECURSIVE to process each stream efficiently
    WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($data_providers)
    ),
    stream_arrays AS (
        SELECT 
            $data_providers AS data_providers,
            $stream_ids AS stream_ids
    ),
    arguments AS (
        SELECT 
            stream_arrays.data_providers[idx] AS data_provider,
            stream_arrays.stream_ids[idx] AS stream_id
        FROM indexes
        JOIN stream_arrays ON 1=1
    ),
    -- Check which streams are owned by the wallet
    ownership_check AS (
        SELECT 
            a.data_provider,
            a.stream_id,
            CASE WHEN m.value_ref IS NOT NULL AND LOWER(m.value_ref) = $lowercase_wallet THEN true ELSE false END AS is_owner
        FROM arguments a
        LEFT JOIN (
            SELECT data_provider, stream_id, value_ref
            FROM metadata
            WHERE metadata_key = 'stream_owner'
              AND disabled_at IS NULL
            ORDER BY created_at DESC
        ) m ON a.data_provider = m.data_provider AND a.stream_id = m.stream_id
    )
    -- Combine results
    SELECT 
        o.data_provider,
        o.stream_id,
        o.is_owner
    FROM ownership_check o;
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
    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }

    -- Use WITH RECURSIVE to process each stream efficiently
    WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($data_providers)
    ),
    stream_arrays AS (
        SELECT 
            $data_providers AS data_providers,
            $stream_ids AS stream_ids
    ),
    arguments AS (
        SELECT 
            stream_arrays.data_providers[idx] AS data_provider,
            stream_arrays.stream_ids[idx] AS stream_id
        FROM indexes
        JOIN stream_arrays ON 1=1
    )
    -- Check stream type for each stream
    SELECT 
        a.data_provider,
        a.stream_id,
        COALESCE(s.stream_type = 'primitive', false) AS is_primitive
    FROM arguments a
    LEFT JOIN streams s ON a.data_provider = s.data_provider AND a.stream_id = s.stream_id;
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
 * get_latest_metadata: Retrieves the latest metadata for a stream.
 */
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
    for $row in get_metadata($data_provider, $stream_id, $key, $ref, 1, 0, 'created_at DESC') {
        RETURN NEXT $row.value_i, $row.value_f, $row.value_b, $row.value_s, $row.value_ref;
    }
};

/**
 * get_latest_metadata_int: Retrieves the latest metadata value for a stream.
 */
CREATE OR REPLACE ACTION get_latest_metadata_int(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT
) PUBLIC view returns (value INT) {
    $result INT;
    for $row in get_latest_metadata($data_provider, $stream_id, $key, NULL) {
        $result := $row.value_i;
    }
    RETURN $result;
};

/**
 * get_latest_metadata_ref: Retrieves the latest metadata value for a stream.
 */
CREATE OR REPLACE ACTION get_latest_metadata_ref(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT,
    $ref TEXT
) PUBLIC view returns (value TEXT) {
    $result TEXT;
    for $row in get_latest_metadata($data_provider, $stream_id, $key, $ref) {
        $result := $row.value_ref;
    }
    RETURN $result;
};

/**
 * get_latest_metadata_bool: Retrieves the latest metadata value for a stream.
 */
CREATE OR REPLACE ACTION get_latest_metadata_bool(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT
) PUBLIC view returns (value BOOL) {
    $result BOOL;
    for $row in get_latest_metadata($data_provider, $stream_id, $key, NULL) {
        $result := $row.value_b;
    }
    RETURN $result;
};

/**
 * get_latest_metadata_string: Retrieves the latest metadata value for a stream.
 */
CREATE OR REPLACE ACTION get_latest_metadata_string(
    $data_provider TEXT,
    $stream_id TEXT,
    $key TEXT
) PUBLIC view returns (value TEXT) {
    $result TEXT;
    for $row in get_latest_metadata($data_provider, $stream_id, $key, NULL) {
        $result := $row.value_s;
    }
    RETURN $result;
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
    -- Check if stream exists
    if !stream_exists($data_provider, $stream_id) {
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
            base.data_provider         AS parent_data_provider,
            base.stream_id             AS parent_stream_id,
            base.child_data_provider,
            base.child_stream_id,
            
            -- The interval during which this row is active:
            base.start_time            AS group_sequence_start,
            COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
        FROM (
            -- Find rows with maximum group_sequence for each start_time
            SELECT
                t.data_provider,
                t.stream_id,
                t.child_data_provider,
                t.child_stream_id,
                t.start_time,
                t.group_sequence,
                MAX(t.group_sequence) OVER (
                    PARTITION BY t.data_provider, t.stream_id, t.start_time
                ) AS max_group_sequence
            FROM taxonomies t
            WHERE t.data_provider = $data_provider
              AND t.stream_id     = $stream_id
              AND t.disabled_at   IS NULL
              AND t.start_time   <= $effective_active_to
              AND t.start_time   >= COALESCE((
                    -- Find the most recent taxonomy at or before effective_active_from
                    SELECT t2.start_time
                    FROM taxonomies t2
                    WHERE t2.data_provider = t.data_provider
                      AND t2.stream_id     = t.stream_id
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
                dt.data_provider,
                dt.stream_id,
                dt.start_time,
                LEAD(dt.start_time) OVER (
                    PARTITION BY dt.data_provider, dt.stream_id
                    ORDER BY dt.start_time
                ) AS next_start
            FROM (
                SELECT DISTINCT
                    t.data_provider,
                    t.stream_id,
                    t.start_time
                FROM taxonomies t
                WHERE t.data_provider = $data_provider
                  AND t.stream_id     = $stream_id
                  AND t.disabled_at   IS NULL
                  AND t.start_time   <= $effective_active_to
                  AND t.start_time   >= COALESCE((
                        SELECT t2.start_time
                        FROM taxonomies t2
                        WHERE t2.data_provider = t.data_provider
                          AND t2.stream_id     = t.stream_id
                          AND t2.disabled_at   IS NULL
                          AND t2.start_time   <= $effective_active_from
                        ORDER BY t2.start_time DESC, t2.group_sequence DESC
                        LIMIT 1
                      ), 0
                  )
            ) dt
        ) ot
          ON base.data_provider = ot.data_provider
         AND base.stream_id     = ot.stream_id
         AND base.start_time    = ot.start_time
        WHERE base.group_sequence = base.max_group_sequence

        UNION

        /*------------------------------------------------------------------
         * (2) Recursive Child-Level Overshadow:
         *     For each discovered child, gather overshadow rows for that child
         *     and produce intervals that overlap the parent's own active interval.
         *------------------------------------------------------------------*/
        SELECT
            parent.parent_data_provider,
            parent.parent_stream_id,
            child.child_data_provider,
            child.child_stream_id,

            -- Intersection of parent's active interval and child's:
            GREATEST(parent.group_sequence_start, child.start_time)    AS group_sequence_start,
            LEAST(parent.group_sequence_end, child.group_sequence_end) AS group_sequence_end
        FROM substreams parent
        JOIN (
            /* Child overshadow logic, same pattern as above but for child dp/sid. */
            SELECT
                base.data_provider,
                base.stream_id,
                base.child_data_provider,
                base.child_stream_id,
                base.start_time,
                COALESCE(ot.next_start, $max_int8) - 1 AS group_sequence_end
            FROM (
                SELECT
                    t.data_provider,
                    t.stream_id,
                    t.child_data_provider,
                    t.child_stream_id,
                    t.start_time,
                    t.group_sequence,
                    MAX(t.group_sequence) OVER (
                        PARTITION BY t.data_provider, t.stream_id, t.start_time
                    ) AS max_group_sequence
                FROM taxonomies t
                WHERE t.disabled_at IS NULL
                  AND t.start_time <= $effective_active_to
                  AND t.start_time >= COALESCE((
                        -- Most recent taxonomy at or before effective_from
                        SELECT t2.start_time
                        FROM taxonomies t2
                        WHERE t2.data_provider = t.data_provider
                          AND t2.stream_id     = t.stream_id
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
                    dt.data_provider,
                    dt.stream_id,
                    dt.start_time,
                    LEAD(dt.start_time) OVER (
                        PARTITION BY dt.data_provider, dt.stream_id
                        ORDER BY dt.start_time
                    ) AS next_start
                FROM (
                    SELECT DISTINCT
                        t.data_provider,
                        t.stream_id,
                        t.start_time
                    FROM taxonomies t
                    WHERE t.disabled_at   IS NULL
                      AND t.start_time   <= $effective_active_to
                      AND t.start_time   >= COALESCE((
                            SELECT t2.start_time
                            FROM taxonomies t2
                            WHERE t2.data_provider = t.data_provider
                              AND t2.stream_id     = t.stream_id
                              AND t2.disabled_at   IS NULL
                              AND t2.start_time   <= $effective_active_from
                            ORDER BY t2.start_time DESC, t2.group_sequence DESC
                            LIMIT 1
                          ), 0
                      )
                ) dt
            ) ot
              ON base.data_provider = ot.data_provider
             AND base.stream_id     = ot.stream_id
             AND base.start_time    = ot.start_time
            WHERE base.group_sequence = base.max_group_sequence
        ) child
          ON child.data_provider = parent.child_data_provider
         AND child.stream_id     = parent.child_stream_id
        
        /* Overlap check: child's interval must intersect parent's */
        WHERE child.start_time         <= parent.group_sequence_end
          AND child.group_sequence_end >= parent.group_sequence_start
    )
    SELECT DISTINCT 
        substreams.child_data_provider, 
        substreams.child_stream_id
    FROM substreams;
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
    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }

    -- Use WITH RECURSIVE to process each stream efficiently
    WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($data_providers)
    ),
    stream_arrays AS (
        SELECT 
            $data_providers AS data_providers,
            $stream_ids AS stream_ids
    ),
    arguments AS (
        SELECT 
            stream_arrays.data_providers[idx] AS data_provider,
            stream_arrays.stream_ids[idx] AS stream_id
        FROM indexes
        JOIN stream_arrays ON 1=1
    )
    -- Check existence for each stream
    SELECT 
        a.data_provider,
        a.stream_id,
        CASE WHEN s.data_provider IS NOT NULL THEN true ELSE false END AS stream_exists
    FROM arguments a
    LEFT JOIN streams s ON a.data_provider = s.data_provider AND a.stream_id = s.stream_id;
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

/**
 * filter_streams_by_existence: Filters streams based on existence.
 * Can return either existing or non-existing streams based on return_existing flag.
 * Takes arrays of data providers and stream IDs as input.
 */
CREATE OR REPLACE ACTION filter_streams_by_existence(
    $data_providers TEXT[],
    $stream_ids TEXT[],
    $existing_only BOOL
) PUBLIC view returns table(
    data_provider TEXT,
    stream_id TEXT
) {
    $filtered_dp TEXT[];
    $filtered_sid TEXT[];

    -- default to return existing streams
    if $existing_only IS NULL {
        $existing_only := true;
    }
    
    -- Check that arrays have the same length
    if array_length($data_providers) != array_length($stream_ids) {
        ERROR('Data providers and stream IDs arrays must have the same length');
    }
    
    -- Iterate through each stream locator
    for $i in 1..array_length($data_providers) {
        $dp := $data_providers[$i];
        $sid := $stream_ids[$i];
        
        -- Check if stream exists
        $exists := false;
        for $row in SELECT 1 FROM streams 
            WHERE LOWER(data_provider) = LOWER($dp) 
            AND stream_id = $sid {
            $exists := true;
        }
        
        -- Filter based on return_existing flag
        if ($exists = $existing_only) {
            $filtered_dp := array_append($filtered_dp, $dp);
            $filtered_sid := array_append($filtered_sid, $sid);
        }
    }
    
    -- Return results as a table
    for $i in 1..array_length($filtered_dp) {
        RETURN NEXT $filtered_dp[$i], $filtered_sid[$i];
    }
};