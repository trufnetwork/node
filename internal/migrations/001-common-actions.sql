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
    $readonly_keys TEXT[] := ['stream_owner', 'readonly_key'];
    
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