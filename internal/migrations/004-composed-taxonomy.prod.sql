-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/004-composed-taxonomy.sql
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
    $fee_total NUMERIC(78, 0) := 0::NUMERIC(78, 0);
    $fee_recipient TEXT := NULL;
    $leader_hex TEXT := NULL;

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
    if $num_children != array_length($child_data_providers) OR $num_children != array_length($weights) {
        ERROR('All child arrays must be of the same length');
    }
    -- ensure there is at least 1 child, otherwise we might have silent bugs
    if $num_children == 0 {
        ERROR('There must be at least 1 child');
    }

    -- ===== FEE COLLECTION WITH ROLE EXEMPTION =====

    -- Check if caller is exempt (has system:network_writer role)
    $is_exempt BOOL := FALSE;
    FOR $row IN are_members_of('system', 'network_writer', ARRAY[$lower_caller]) {
        IF $row.wallet = $lower_caller AND $row.is_member {
            $is_exempt := TRUE;
            BREAK;
        }
    }

    -- Collect fee only from non-exempt wallets (6 TRUF per stream)
    IF NOT $is_exempt {
        $fee_per_stream := 6000000000000000000::NUMERIC(78, 0); -- 6 TRUF with 18 decimals
        $total_fee := $fee_per_stream * $num_children::NUMERIC(78, 0);

        IF @leader_sender IS NULL {
            ERROR('Leader address not available for fee transfer');
        }
        $leader_hex := encode(@leader_sender, 'hex')::TEXT;

        $caller_balance := eth_truf.balance(@caller);

        IF $caller_balance < $total_fee {
            -- Derive human-readable fee from $total_fee
            ERROR('Insufficient balance for taxonomies creation. Required: ' || ($total_fee / 1000000000000000000::NUMERIC(78, 0))::TEXT || ' TRUF for ' || $num_children::TEXT || ' child stream(s)');
        }

        eth_truf.transfer($leader_hex, $total_fee);
        $fee_total := $total_fee;
        $fee_recipient := '0x' || $leader_hex;
    }
    -- ===== END FEE COLLECTION =====

    -- Default start time to 0 if not provided
    if $start_date IS NULL {
        $start_date := 0;
    }

    -- Retrieve the current group_sequence for this parent and increment it by 1.
    $new_group_sequence := get_current_group_sequence($data_provider, $stream_id, true) + 1;
    
    $stream_ref := get_stream_id($data_provider, $stream_id);
    if $stream_ref IS NULL {
        ERROR('parent stream does not exist: ' || $data_provider || ':' || $stream_id);
    }

    FOR $i IN 1..$num_children {
        $child_data_provider_value := $child_data_providers[$i];
        $child_stream_id_value := $child_stream_ids[$i];
        $child_stream_ref := get_stream_id($child_data_provider_value, $child_stream_id_value);
        $weight_value := $weights[$i];

        if $child_stream_ref IS NULL {
            ERROR('child stream does not exist: ' || $child_data_provider_value || ':' || $child_stream_id_value);
        }

        $taxonomy_id := uuid_generate_kwil(@txid||$data_provider||$stream_id||$child_data_provider_value||$child_stream_id_value||$i::TEXT);

        INSERT INTO taxonomies (
            taxonomy_id,
            weight,
            created_at,
            disabled_at,
            group_sequence,
            start_time,
            stream_ref,
            child_stream_ref,
            tx_id
        ) VALUES (
            $taxonomy_id,
            $weight_value,
            @height,             -- Use the current block height for created_at.
            NULL,               -- New record is active.
            $new_group_sequence,          -- Use the new group_sequence for all child records.
            $start_date,          -- Start date of the taxonomy.
            $stream_ref,
            $child_stream_ref,
            @txid
        );
    }

    record_transaction_event(
        3,
        $fee_total,
        $fee_recipient,
        NULL
    );
};
