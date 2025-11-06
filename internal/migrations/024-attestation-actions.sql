-- =============================================================================
-- CORE ATTESTATION ACTIONS
-- =============================================================================

/**
 * request_attestation: Request signed attestation of query results
 *
 * Permissions:
 * - Only wallets with the 'system:network_writer' role can request attestations.
 *
 * Validates action is allowed, executes query deterministically, calculates
 * attestation hash, stores unsigned attestation, and queues for signing.
 */
CREATE OR REPLACE ACTION request_attestation(
    $data_provider TEXT,
    $stream_id TEXT,
    $action_name TEXT,
    $args_bytes BYTEA,
    $encrypt_sig BOOLEAN,
    $max_fee NUMERIC(78, 0)
) PUBLIC RETURNS (request_tx_id TEXT, attestation_hash BYTEA) {
    -- Capture transaction ID for primary key
    $request_tx_id := @txid;
    
    -- Permission Check: Ensure caller has the 'system:network_writer' role.
    $lower_caller TEXT := LOWER(@caller);
    $has_permission BOOL := false;
    for $row in are_members_of('system', 'network_writer', ARRAY[$lower_caller]) {
        if $row.wallet = $lower_caller AND $row.is_member {
            $has_permission := true;
            break;
        }
    }
    if NOT $has_permission {
        ERROR('Caller does not have the required system:network_writer role to request attestation.');
    }
    
    -- Validate encryption flag (must be false in MVP)
    if $encrypt_sig = true {
        ERROR('Encryption not implemented');
    }
    
    -- Validate action is in allowlist
    $action_id := 0;
    for $row in SELECT action_id FROM attestation_actions WHERE action_name = $action_name {
        $action_id := $row.action_id;
    }
    if $action_id = 0 {
        ERROR('Action not allowed for attestation: ' || $action_name);
    }

    -- ===== FEE COLLECTION =====
    -- Collect 40 TRUF flat fee for attestation request
    $attestation_fee := '40000000000000000000'::NUMERIC(78, 0); -- 40 TRUF with 18 decimals

    -- Validate max_fee if provided
    IF $max_fee IS NOT NULL AND $max_fee > 0::NUMERIC(78, 0) {
        IF $attestation_fee > $max_fee {
            ERROR('Attestation fee (40 TRUF) exceeds caller max_fee limit: ' || ($max_fee / 1000000000000000000::NUMERIC(78, 0))::TEXT || ' TRUF');
        }
    }

    $caller_balance := ethereum_bridge.balance(@caller);

    IF $caller_balance < $attestation_fee {
        ERROR('Insufficient balance for attestation. Required: 40 TRUF');
    }

    -- Verify leader address is available
    IF @leader_sender IS NULL {
        ERROR('Leader address not available for fee transfer');
    }

    $leader_addr TEXT := encode(@leader_sender, 'hex')::TEXT;
    ethereum_bridge.transfer($leader_addr, $attestation_fee);
    -- ===== END FEE COLLECTION =====
    
    -- Get current block height
    $created_height := @height;
    
    -- Normalize caller address to bytes for storage
    $caller_hex := LOWER(substring(@caller, 3, 40));
    $caller_bytes := decode($caller_hex, 'hex');
    
    -- Normalize provider input and enforce length
    $provider_lower := LOWER($data_provider);
    if char_length($provider_lower) != 42 {
        ERROR('data_provider must be 0x-prefixed 40 hex characters');
    }
    if substring($provider_lower, 1, 2) != '0x' {
        ERROR('data_provider must be 0x-prefixed 40 hex characters');
    }
    $data_provider_bytes := decode(substring($provider_lower, 3, 40), 'hex');

    if length($stream_id) != 32 {
        ERROR('stream_id must be 32 characters');
    }
    $stream_bytes := $stream_id::BYTEA;

    -- Force deterministic execution by overriding non-deterministic parameters.
    -- Query actions (IDs 1-5) all have use_cache as their last parameter.
    -- Force use_cache=false to ensure all validators compute identical results
    -- regardless of cache state.
    if $action_id >= 1 AND $action_id <= 5 {
        $args_bytes := tn_utils.force_last_arg_false($args_bytes);
    }

    -- Execute target query deterministically using tn_utils.call_dispatch precompile
    $query_result := tn_utils.call_dispatch($action_name, $args_bytes);
    $result_payload := tn_utils.canonical_to_datapoints_abi($query_result);
    
    $version := 1;
    $algo := 0; -- secp256k1
    -- Serialize canonical payload (version through result) using tn_utils helpers
    $version_bytes := tn_utils.encode_uint8($version::INT);
    $algo_bytes := tn_utils.encode_uint8($algo::INT);
    $height_bytes := tn_utils.encode_uint64($created_height::INT);
    $action_id_bytes := tn_utils.encode_uint16($action_id::INT);

    -- Canonical payload mirrors Go helpers: each field length-prefixed so the
    -- validator can recover every component without ambiguity.
    $result_canonical := tn_utils.bytea_join(ARRAY[
        $version_bytes,
        $algo_bytes,
        $height_bytes,
        tn_utils.bytea_length_prefix($data_provider_bytes),
        tn_utils.bytea_length_prefix($stream_bytes),
        $action_id_bytes,
        tn_utils.bytea_length_prefix($args_bytes),
        tn_utils.bytea_length_prefix($result_payload)
    ], NULL);
    
    -- Build hash material in canonical order using caller-provided inputs only.
    -- This keeps the hash deterministic for clients (excludes block height and result).
    $hash_input := tn_utils.bytea_join(ARRAY[
        $version_bytes,
        $algo_bytes,
        tn_utils.bytea_length_prefix($data_provider_bytes),
        tn_utils.bytea_length_prefix($stream_bytes),
        $action_id_bytes,
        tn_utils.bytea_length_prefix($args_bytes)
    ], NULL);
    $attestation_hash := digest($hash_input, 'sha256');
    
    -- Store unsigned attestation
    INSERT INTO attestations (
        request_tx_id, attestation_hash, requester, result_canonical, encrypt_sig, 
        created_height, signature, validator_pubkey, signed_height
    ) VALUES (
        $request_tx_id, $attestation_hash, $caller_bytes, $result_canonical, $encrypt_sig, 
        $created_height, NULL, NULL, NULL
    );
    
    -- Queue for signing (no-op on non-leader validators; handled by precompile)
    tn_attestation.queue_for_signing(encode($attestation_hash, 'hex'));

    record_transaction_event(
        6,
        $attestation_fee,
        '0x' || $leader_addr,
        NULL
    );

RETURN $request_tx_id, $attestation_hash;
};

-- -----------------------------------------------------------------------------
-- Leader-only action for recording validator signatures on attestations.
CREATE OR REPLACE ACTION sign_attestation(
    $request_tx_id TEXT,
    $signature BYTEA
) PUBLIC {
    -- Only the current leader may submit signatures on-chain.
    IF @leader_sender IS NULL OR @signer IS NULL OR @leader_sender != @signer {
        $leader_hex TEXT := 'unknown';
        $signer_hex TEXT := 'unknown';
        IF @leader_sender IS NOT NULL {
            $leader_hex := encode(@leader_sender, 'hex')::TEXT;
        }
        IF @signer IS NOT NULL {
            $signer_hex := encode(@signer, 'hex')::TEXT;
        }
        ERROR('Only the current block leader may sign attestations. leader=' || $leader_hex || ' signer=' || $signer_hex);
    }

    IF $request_tx_id IS NULL {
        ERROR('Request transaction ID is required');
    }
    IF $signature IS NULL {
        ERROR('Signature is required');
    }

    -- Ensure attestation exists and has not been signed yet.
    $found BOOL := FALSE;
    FOR $row IN
        SELECT signature
        FROM attestations
        WHERE request_tx_id = $request_tx_id
        LIMIT 1
    {
        $found := TRUE;
        IF $row.signature IS NOT NULL {
            ERROR('Attestation already signed: ' || $request_tx_id);
        }
    }
    IF NOT $found {
        ERROR('Attestation not found: ' || $request_tx_id);
    }

    -- Record signature, validator identity, and the height at which it was signed.
    UPDATE attestations
       SET signature = $signature,
           validator_pubkey = @signer,
           signed_height = @height
     WHERE request_tx_id = $request_tx_id
       AND signature IS NULL;
};

-- -----------------------------------------------------------------------------
/**
 * get_signed_attestation: Retrieve complete signed attestation payload
 *
 * Returns the concatenated canonical payload + signature as a single BYTEA.
 * 
 * NOTE: This action does NOT verify the signature or hash integrity.
 * Verification is the responsibility of the client/consumer because:
 * 1. Clients should never blindly trust data from any source
 * 2. Verification requires cryptographic operations that are expensive to run on every retrieval
 * 3. The canonical payload contains the validator's public key, allowing client-side verification
 * 4. Different clients may have different trust models (some may trust specific validators)
 * 
 * Clients SHOULD verify signatures using the validator_pubkey in the payload before using the data.
 */
CREATE OR REPLACE ACTION get_signed_attestation(
    $request_tx_id TEXT
) PUBLIC VIEW RETURNS (payload BYTEA) {
    -- Validate attestation exists
    $found BOOL := FALSE;
    $canonical BYTEA;
    $sig BYTEA;
    
    FOR $row IN 
        SELECT result_canonical, signature
        FROM attestations
        WHERE request_tx_id = $request_tx_id
        LIMIT 1
    {
        $found := TRUE;
        $canonical := $row.result_canonical;
        $sig := $row.signature;
    }
    
    IF NOT $found {
        ERROR('Attestation not found: ' || $request_tx_id);
    }
    
    -- Validate signature is not NULL
    IF $sig IS NULL {
        ERROR('Attestation not yet signed: ' || $request_tx_id);
    }
    
    -- Concatenate canonical + signature for 9-item payload
    RETURN tn_utils.bytea_join(ARRAY[$canonical, $sig], NULL);
};

-- -----------------------------------------------------------------------------
/**
 * list_attestations: List attestations with optional filtering
 *
 * Returns metadata for attestations, optionally filtered by requester.
 * Supports pagination with limit and offset.
 */
CREATE OR REPLACE ACTION list_attestations(
    $requester BYTEA,
    $limit INT,
    $offset INT,
    $order_by TEXT
) PUBLIC VIEW RETURNS TABLE(
    request_tx_id TEXT,
    attestation_hash BYTEA,
    requester BYTEA,
    created_height INT8,
    signed_height INT8,
    encrypt_sig BOOLEAN
) {
    -- Validate and apply defaults
    IF $limit IS NULL OR $limit > 5000 {
        $limit := 5000;
    }
    IF $offset IS NULL {
        $offset := 0;
    }
    
    -- Parse and sanitise order_by input (only created_height supported)
    $order_desc BOOL := TRUE;
    IF $order_by IS NOT NULL {
        $normalized TEXT := LOWER(TRIM($order_by));
        IF $normalized = 'created_height asc' OR $normalized = 'created_height' {
            $order_desc := FALSE;
        } ELSEIF $normalized = 'created_height desc' {
            $order_desc := TRUE;
        }
    }
    
    -- Build query with optional requester filter
    IF $requester IS NULL {
        -- Show all attestations (analytics/auditing)
        IF $order_desc {
            RETURN SELECT request_tx_id, attestation_hash, requester, 
                          created_height, signed_height, encrypt_sig
                   FROM attestations
                   ORDER BY created_height DESC, request_tx_id ASC
                   LIMIT $limit OFFSET $offset;
        } ELSE {
            RETURN SELECT request_tx_id, attestation_hash, requester, 
                          created_height, signed_height, encrypt_sig
                   FROM attestations
                   ORDER BY created_height ASC, request_tx_id ASC
                   LIMIT $limit OFFSET $offset;
        }
    } ELSE {
        -- Filter by requester
        IF $order_desc {
            RETURN SELECT request_tx_id, attestation_hash, requester,
                          created_height, signed_height, encrypt_sig
                   FROM attestations
                   WHERE requester = $requester
                   ORDER BY created_height DESC, request_tx_id ASC
                   LIMIT $limit OFFSET $offset;
        } ELSE {
            RETURN SELECT request_tx_id, attestation_hash, requester,
                          created_height, signed_height, encrypt_sig
                   FROM attestations
                   WHERE requester = $requester
                   ORDER BY created_height ASC, request_tx_id ASC
                   LIMIT $limit OFFSET $offset;
        }
    }
};
