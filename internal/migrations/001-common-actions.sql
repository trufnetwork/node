CREATE ACTION create_stream(
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
}