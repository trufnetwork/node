-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/024-attestation-actions.sql
-- Script : scripts/generate_prod_migrations.py
--
-- Manual-apply mainnet override. The embedded migration loader skips
-- *.prod.sql, so apply via:
--
--     kwil-cli exec-sql --file <this file> --sync \
--         --private-key $PRIVATE_KEY --provider $PROVIDER
--
-- Prerequisite: erc20-bridge/000-extension.prod.sql must be applied
-- FIRST so the eth_truf and eth_usdc bridge instances exist.
-- =============================================================================

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

    -- ===== FEE COLLECTION WITH ROLE EXEMPTION =====
    -- Declare variables in outer scope
    $attestation_fee NUMERIC(78, 0);
    $caller_balance NUMERIC(78, 0);
    $leader_addr TEXT;

    -- Normalizing caller and leader safely using precompiles
    $caller_bytes BYTEA := tn_utils.get_caller_bytes();
    $lower_caller TEXT := tn_utils.get_caller_hex();

    -- Check if caller is exempt (has system:network_writer role)
    $is_exempt BOOL := FALSE;
    FOR $row IN are_members_of('system', 'network_writer', ARRAY[$lower_caller]) {
        IF $row.wallet = $lower_caller AND $row.is_member {
            $is_exempt := TRUE;
            BREAK;
        }
    }

    -- Collect fee only from non-exempt wallets (40 TRUF flat fee)
    IF NOT $is_exempt {
        $attestation_fee := '40000000000000000000'::NUMERIC(78, 0); -- 40 TRUF with 18 decimals

        -- Validate max_fee if provided
        IF $max_fee IS NOT NULL AND $max_fee > 0::NUMERIC(78, 0) {
            IF $attestation_fee > $max_fee {
                ERROR('Attestation fee (40 TRUF) exceeds caller max_fee limit: ' || ($max_fee / 1000000000000000000::NUMERIC(78, 0))::TEXT || ' TRUF');
            }
        }

        $caller_balance := eth_truf.balance(@caller);

        IF $caller_balance < $attestation_fee {
            ERROR('Insufficient balance for attestation. Required: 40 TRUF');
        }

        -- Safe leader address conversion
        $leader_addr := tn_utils.get_leader_hex();
        IF $leader_addr = '' {
            ERROR('Leader address not available for fee transfer');
        }

        eth_truf.transfer($leader_addr, $attestation_fee);
    }
    -- ===== END FEE COLLECTION =====
    
    -- Get current block height
    $created_height := @height;
    
    -- Normalize caller address to bytes for storage (re-use safe normalization)
    $caller_bytes := $caller_bytes; -- Already normalized above
    
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

    -- Validate date range for range-based attestation actions (IDs 1-3) BEFORE
    -- executing the query. This prevents unbounded queries from scanning the entire
    -- primitive_events table during block execution. Max range: 90 days.
    -- This check runs before call_dispatch to reject expensive queries early,
    -- before kwil-db buffers all result rows into memory.
    tn_utils.validate_attestation_date_range($action_id, $args_bytes);

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
        request_tx_id, attestation_hash, requester, data_provider, stream_id,
        result_canonical, encrypt_sig, created_height, signature, validator_pubkey, signed_height
    ) VALUES (
        $request_tx_id, $attestation_hash, $caller_bytes, $data_provider, $stream_id,
        $result_canonical, $encrypt_sig, $created_height, NULL, NULL, NULL
    );
    
    -- Queue for signing (no-op on non-leader validators; handled by precompile)
    tn_attestation.queue_for_signing(encode($attestation_hash, 'hex'));

    record_transaction_event(
        6,
        $attestation_fee,
        $leader_addr,
        NULL
    );

RETURN $request_tx_id, $attestation_hash;
};
