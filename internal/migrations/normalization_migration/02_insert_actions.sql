-- Migration-compatible actions that handle both old and new schema during normalization migration
-- These actions are designed to work during the transition period when both old and new columns may exist

/**
 * get_stream_id: Migration-compatible version that handles transition period
 */
CREATE OR REPLACE ACTION get_stream_id(
  $data_provider_address TEXT,
  $stream_id TEXT
) PRIVATE returns (id INT) {
  $id INT;
  FOR $stream_row IN SELECT s.id
      FROM streams s
      JOIN data_providers dp ON s.data_provider_id = dp.id
      WHERE s.stream_id = $stream_id 
      AND dp.address = $data_provider_address
      AND s.id IS NOT NULL  -- Only return streams that have been migrated
      LIMIT 1 {
      $id := $stream_row.id;
  }

  return $id;
};

/**
 * get_stream_ids: Migration-compatible version for batch operations
 */
CREATE OR REPLACE ACTION get_stream_ids(
    $data_providers TEXT[],
    $stream_ids TEXT[]
) PRIVATE VIEW RETURNS (stream_ids INT[]) {
    -- Use WITH RECURSIVE to process stream ids lookup in single SQL operation
    -- This avoids the expensive for-loop roundtrips
    for $row in WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < array_length($data_providers)
    ),
    input_arrays AS (
        SELECT 
            $data_providers AS data_providers,
            $stream_ids AS stream_ids
    ),
    stream_lookups AS (
        SELECT
            s.id AS stream_ref
        FROM indexes
        JOIN input_arrays ON 1=1
        JOIN data_providers dp ON dp.address = input_arrays.data_providers[idx]
        JOIN streams s ON s.data_provider_id = dp.id 
                      AND s.stream_id = input_arrays.stream_ids[idx]
                      AND s.id IS NOT NULL  -- Only include migrated streams
    )
    SELECT ARRAY_AGG(stream_ref) AS stream_refs
    FROM stream_lookups {
      return $row.stream_refs;
    }
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
 * create_streams: Migration-compatible version that handles new INT schema
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
    
    -- Create the streams with proper INT IDs
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
    ),
    sequential_ids AS (
        SELECT 
            idx,
            stream_id,
            stream_type,
            ROW_NUMBER() OVER (ORDER BY idx) + COALESCE((SELECT MAX(id) FROM streams), 0) AS id
        FROM arguments
    )
    INSERT INTO streams (id, data_provider_id, data_provider, stream_id, stream_type, created_at)
    SELECT
        id,
        $data_provider_id,
        $data_provider,
        stream_id, 
        stream_type, 
        @height
    FROM sequential_ids;
 
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
            'readonly_key' AS metadata_key,
            'stream_owner' AS value_s,
            NULL::INT AS value_i,
            NULL::TEXT AS value_ref
        UNION ALL
        SELECT
            'readonly_key' AS metadata_key,
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
            uuid_generate_v5($base_uuid, 'metadata' || $data_provider || arg.stream_id || arg.metadata_key || arg.row_number::TEXT)::UUID as row_id,
            $data_provider AS data_provider,
            arg.stream_id,
            arg.metadata_key,
            arg.value_s,
            arg.value_i,
            arg.value_ref,
            @height AS created_at,
            s.id AS stream_ref
        FROM args_with_row_number arg
        JOIN data_providers dp ON dp.address = $data_provider
        JOIN streams s ON s.data_provider_id = dp.id AND s.stream_id = arg.stream_id
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
        disabled_at,
        stream_ref
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
        NULL::INT8,
        stream_ref
    FROM args;
};

/**
 * insert_record: Migration-compatible version for primitive events
 */
CREATE OR REPLACE ACTION insert_record(
    $data_provider TEXT,
    $stream_id TEXT,
    $event_time INT8,
    $value NUMERIC(36,18)
) PUBLIC {
    $data_provider TEXT := LOWER($data_provider);
    $lower_caller TEXT := LOWER(@caller);
    -- Ensure the wallet is allowed to write
    if !is_wallet_allowed_to_write($data_provider, $stream_id, $lower_caller) {
        ERROR('wallet not allowed to write');
    }

    -- Ensure that the stream/contract is existent
    if !stream_exists($data_provider, $stream_id) {
        ERROR('stream does not exist');
    }

    -- Ensure that the stream is a primitive stream
    if is_primitive_stream($data_provider, $stream_id) == false {
        ERROR('stream is not a primitive stream');
    }

    -- Skip insertion if value is 0
    if $value == 0::NUMERIC(36,18) {
        RETURN;
    }

    $current_block INT := @height;
    $stream_ref INT := get_stream_id($data_provider, $stream_id);

    -- Insert the new record into the primitive_events table
    -- During migration, stream_ref might be NULL for some records, that's expected
    INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at, stream_ref)
    VALUES ($stream_id, $data_provider, $event_time, $value, $current_block, $stream_ref);
};

/**
 * insert_records: Migration-compatible version for batch primitive events
 */
CREATE OR REPLACE ACTION insert_records(
    $data_provider TEXT[],
    $stream_id TEXT[],
    $event_time INT8[],
    $value NUMERIC(36,18)[]
) PUBLIC {
    -- Use helper function to avoid expensive for-loop roundtrips
    $data_provider := helper_lowercase_array($data_provider);
    $lower_caller TEXT := LOWER(@caller);

    $num_records INT := array_length($data_provider);
    if $num_records != array_length($stream_id) or $num_records != array_length($event_time) or $num_records != array_length($value) {
        ERROR('array lengths mismatch');
    }

    $current_block INT := @height;

    -- Check stream existence in batch
    for $row in stream_exists_batch($data_provider, $stream_id) {
        if !$row.stream_exists {
            ERROR('stream does not exist: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Check if streams are primitive in batch
    for $row in is_primitive_stream_batch($data_provider, $stream_id) {
        if !$row.is_primitive {
            ERROR('stream is not a primitive stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Validate that the wallet is allowed to write to each stream
    for $row in is_wallet_allowed_to_write_batch($data_provider, $stream_id, $lower_caller) {
        if !$row.is_allowed {
            ERROR('wallet not allowed to write to stream: data_provider=' || $row.data_provider || ', stream_id=' || $row.stream_id);
        }
    }

    -- Get stream reference for all streams (may contain NULLs during migration)
    $stream_refs := get_stream_ids($data_provider, $stream_id);

    -- Insert all records using WITH RECURSIVE pattern to avoid round trips
    WITH RECURSIVE 
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes
        WHERE idx < $num_records
    ),
    record_arrays AS (
        SELECT 
            $stream_id AS stream_ids,
            $data_provider AS data_providers,
            $event_time AS event_times,
            $value AS values_array,
            $stream_refs AS stream_refs_array
    ),
    arguments AS (
        SELECT 
            record_arrays.stream_ids[idx] AS stream_id,
            record_arrays.data_providers[idx] AS data_provider,
            record_arrays.event_times[idx] AS event_time,
            record_arrays.values_array[idx] AS value,
            record_arrays.stream_refs_array[idx] AS stream_ref
        FROM indexes
        JOIN record_arrays ON 1=1
        WHERE record_arrays.values_array[idx] != 0::NUMERIC(36,18)
    )
    INSERT INTO primitive_events (stream_id, data_provider, event_time, value, created_at, truflation_created_at, stream_ref)
    SELECT 
        stream_id, 
        data_provider, 
        event_time, 
        value, 
        $current_block,
        NULL,
        stream_ref  -- May be NULL during migration, will be populated later
    FROM arguments;
};

/**
 * insert_taxonomy: Migration-compatible version for composed streams
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
    
    $stream_ref := get_stream_id($data_provider, $stream_id);

    FOR $i IN 1..$num_children {
        $child_data_provider_value := $child_data_providers[$i];
        $child_stream_id_value := $child_stream_ids[$i];
        $child_stream_ref := get_stream_id($child_data_provider_value, $child_stream_id_value);
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
            start_time,
            stream_ref,
            child_stream_ref
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
            $start_date,          -- Start date of the taxonomy.
            $stream_ref,         -- May be NULL during migration
            $child_stream_ref    -- May be NULL during migration
        );
    }
};

/**
 * insert_metadata: Migration-compatible version for metadata insertion
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
    
    -- Check if the key is read-only (only if stream_ref is available)
    $is_readonly BOOL := false;
    if $stream_ref IS NOT NULL {
        for $row in SELECT * FROM metadata 
            WHERE stream_ref = $stream_ref
            AND metadata_key = 'readonly_key' 
            AND value_s = $key LIMIT 1 {
            $is_readonly := true;
        }
    }
    
    if $is_readonly = true {
        ERROR('Cannot insert metadata for read-only key');
    }
    
    -- Create deterministic UUID for the metadata record
    $uuid_key TEXT := @txid || $key || $value;
    $uuid UUID := uuid_generate_kwil($uuid_key);
    $current_block INT := @height;
    $stream_ref INT := get_stream_id($data_provider, $stream_id);
    
    -- Insert the metadata (stream_ref may be NULL during migration)
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
        created_at,
        stream_ref
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
        $current_block,
        $stream_ref  -- May be NULL during migration
    );
};

/**
 * create_data_provider: Migration-compatible version for data provider creation
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

    -- Insert with proper INT ID generation
    INSERT INTO data_providers (id, address, created_at) 
    SELECT 
        COALESCE(MAX(id), 0) + 1,
        $lower_address,
        @height
    FROM data_providers
    ON CONFLICT DO NOTHING;
};

/**
 * disable_metadata: Migration-compatible version for metadata disabling
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
    -- Only proceed if stream_ref is available (stream has been migrated)
    if $stream_ref IS NOT NULL {
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
    } else {
        ERROR('Cannot disable metadata during migration - stream not yet migrated');
    }
};

/**
 * disable_taxonomy: Migration-compatible version for taxonomy disabling
 */
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

    -- Only proceed if stream has been migrated
    if $stream_ref IS NOT NULL {
        UPDATE taxonomies
        SET disabled_at = @height
        WHERE stream_ref = $stream_ref
        AND group_sequence = $group_sequence;
    } else {
        ERROR('Cannot disable taxonomy during migration - stream not yet migrated');
    }
};

/**
 * transfer_stream_ownership: Migration-compatible version for ownership transfer
 */
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

    -- Only proceed if stream has been migrated
    if $stream_ref IS NOT NULL {
        -- Update the stream_owner metadata
        UPDATE metadata SET value_ref = LOWER($new_owner)
        WHERE metadata_key = 'stream_owner'
        AND stream_ref = $stream_ref;
    } else {
        ERROR('Cannot transfer ownership during migration - stream not yet migrated');
    }
};
