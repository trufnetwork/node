/**
 * TEST-ONLY OVERRIDE: TRANSACTION FEE COLLECTION - STREAM CREATION
 * This file overrides the production create_streams and create_stream actions
 * to use sepolia_bridge instead of ethereum_bridge for test environments.
 *
 * This is an exact copy of 026-transaction-fees.sql with only the bridge namespace changed.
 */

CREATE OR REPLACE ACTION create_streams(
    $stream_ids TEXT[],
    $stream_types TEXT[]
) PUBLIC {
    -- ===== FEE COLLECTION WITH ROLE EXEMPTION =====
    $lower_caller TEXT := LOWER(@caller);

    -- Check if caller is exempt (has system:network_writer role)
    $is_exempt BOOL := FALSE;
    FOR $row IN are_members_of('system', 'network_writer', ARRAY[$lower_caller]) {
        IF $row.wallet = $lower_caller AND $row.is_member {
            $is_exempt := TRUE;
            BREAK;
        }
    }

    -- Collect fee only from non-exempt wallets (TEST: using sepolia_bridge, 2 TRUF per stream)
    IF NOT $is_exempt {
        $fee_per_stream := 2000000000000000000::NUMERIC(78, 0); -- 2 TRUF with 18 decimals
        $num_streams := array_length($stream_ids);
        $total_fee := $fee_per_stream * $num_streams::NUMERIC(78, 0);

        $caller_balance := sepolia_bridge.balance(@caller);

        IF $caller_balance < $total_fee {
            ERROR('Insufficient balance for stream creation. Required: ' || ($num_streams * 2)::TEXT || ' TRUF for ' || $num_streams::TEXT || ' stream(s)');
        }

        $leader_addr TEXT := encode(@leader_sender, 'hex')::TEXT;
        sepolia_bridge.transfer($leader_addr, $total_fee);
    }
    -- ===== END FEE COLLECTION =====

    -- ===== ORIGINAL STREAM CREATION LOGIC =====
    -- Get caller's address (data provider)
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
        IF $validation_result.error_reason != '' {
            ERROR('Invalid stream type at position ' || $validation_result.position || ': ' ||
                  $validation_result.stream_type || ' - ' || $validation_result.error_reason);
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

CREATE OR REPLACE ACTION create_stream(
    $stream_id TEXT,
    $stream_type TEXT
) PUBLIC {
    -- Delegate to batch implementation (fee collection happens in create_streams)
    create_streams( ARRAY[$stream_id], ARRAY[$stream_type] );
};
