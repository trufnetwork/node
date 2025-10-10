-- =============================================================================
-- CORE ATTESTATION ACTIONS
-- =============================================================================

/**
 * request_attestation: Request signed attestation of query results
 *
 * Validates action is allowed, executes query deterministically, calculates
 * attestation hash, stores unsigned attestation, and queues for signing.
 */
CREATE OR REPLACE ACTION request_attestation(
    $data_provider BYTEA,
    $stream_id BYTEA,
    $action_name TEXT,
    $args_bytes BYTEA,
    $encrypt_sig BOOLEAN,
$max_fee INT8
) PUBLIC RETURNS (attestation_hash BYTEA) {
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
    
    -- Get current block height
    $created_height := @height;
    
    -- Normalize caller address to bytes for storage
    $caller_hex := LOWER(substring(@caller, 3, 40));
    $caller_bytes := decode($caller_hex, 'hex');
    
    -- Execute target query deterministically using tn_utils.call_dispatch precompile
    -- TODO: some arguments are not deterministic, such as `use_cache`
    -- we should aim at filtering these out before we release attestations.
    -- One idea is to also store a force_args in the whitelisted actions. Then this should help us force 
    -- some args per action
    $query_result := tn_utils.call_dispatch($action_name, $args_bytes);
    
    -- Calculate attestation hash from (version|algo|data_provider|stream_id|action_id|args)
    $version := 1;
    $algo := 1; -- secp256k1
    -- Serialize canonical payload (version through result) using tn_utils helpers
    $version_bytes := tn_utils.encode_uint8($version::INT);
    $algo_bytes := tn_utils.encode_uint8($algo::INT);
    $height_bytes := tn_utils.encode_uint64($created_height::INT);
    $action_id_bytes := tn_utils.encode_uint16($action_id::INT);

    -- Build hash material in canonical order (no length prefixes) to match
    -- the engine-side hashing utilities used by the signing service.
    $hash_input := tn_utils.bytea_join(ARRAY[
        $version_bytes,
        $algo_bytes,
        $data_provider,
        $stream_id,
        $action_id_bytes,
        $args_bytes
    ], NULL);
    $attestation_hash := digest($hash_input, 'sha256');

    -- Canonical payload mirrors Go helpers: each field length-prefixed so the
    -- validator can recover every component without ambiguity.
    $result_canonical := tn_utils.bytea_join(ARRAY[
        $version_bytes,
        $algo_bytes,
        $height_bytes,
        tn_utils.bytea_length_prefix($data_provider),
        tn_utils.bytea_length_prefix($stream_id),
        $action_id_bytes,
        tn_utils.bytea_length_prefix($args_bytes),
        tn_utils.bytea_length_prefix($query_result)
    ], NULL);
    
    -- Store unsigned attestation
    INSERT INTO attestations (
        attestation_hash, requester, result_canonical, encrypt_sig, 
        created_height, signature, validator_pubkey, signed_height
    ) VALUES (
        $attestation_hash, $caller_bytes, $result_canonical, $encrypt_sig, 
        $created_height, NULL, NULL, NULL
    );
    
-- Queue for signing (no-op on non-leader validators; handled by precompile)
tn_attestation.queue_for_signing(encode($attestation_hash, 'hex'));

RETURN $attestation_hash;
};

-- -----------------------------------------------------------------------------
-- Leader-only action for recording validator signatures on attestations.
CREATE OR REPLACE ACTION sign_attestation(
    $attestation_hash BYTEA,
    $requester BYTEA,
    $created_height INT8,
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

    IF $attestation_hash IS NULL OR length($attestation_hash) = 0 {
        ERROR('Attestation hash is required');
    }
    IF $requester IS NULL OR length($requester) = 0 {
        ERROR('Requester is required');
    }
    IF $created_height IS NULL {
        ERROR('Created height is required');
    }
    IF $signature IS NULL OR length($signature) = 0 {
        ERROR('Signature is required');
    }

    -- Ensure attestation exists and has not been signed yet.
    $found BOOL := FALSE;
    FOR $row IN
        SELECT signature
        FROM attestations
        WHERE attestation_hash = $attestation_hash
          AND requester = $requester
          AND created_height = $created_height
        LIMIT 1
    {
        $found := TRUE;
        IF $row.signature IS NOT NULL {
            ERROR('Attestation already signed for requester at height ' || $created_height::TEXT);
        }
    }
    IF NOT $found {
        ERROR('Attestation not found for requester at height ' || $created_height::TEXT);
    }

    -- Record signature, validator identity, and the height at which it was signed.
    UPDATE attestations
       SET signature = $signature,
           validator_pubkey = @signer,
           signed_height = @height
     WHERE attestation_hash = $attestation_hash
       AND requester = $requester
       AND created_height = $created_height;
};

-- TODO: get_signed_attestation / list_attestations
-- CREATE OR REPLACE ACTION get_signed_attestation(...) { ... };
-- CREATE OR REPLACE ACTION list_attestations(...) RETURNS TABLE(...) { ... };
