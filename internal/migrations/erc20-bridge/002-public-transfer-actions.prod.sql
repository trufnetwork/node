-- =============================================================================
-- HAND-WRITTEN MAINNET OVERRIDE — TRUF + USDC peer-to-peer transfers
-- =============================================================================
-- Source pattern: internal/migrations/erc20-bridge/002-public-transfer-actions.sql
--
-- The original file's `sepolia_transfer` and `ethereum_transfer` actions
-- target bridge instances that do not exist on mainnet (sepolia_bridge
-- and ethereum_bridge). This override adds two new actions targeting the
-- production bridges:
--
--   eth_truf_transfer  — TRUF token p2p transfer + 1 TRUF fee (18 decimals)
--   eth_usdc_transfer  — USDC token p2p transfer + 1 USDC fee (6 decimals)
--
-- Both follow the same pattern as the original ethereum_transfer:
-- fee is paid in the SAME bridge as the transfer (not always in TRUF).
-- This avoids forcing USDC senders to also hold TRUF.
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

-- TRUF p2p transfer (replaces ethereum_transfer)
-- Fee: 1 TRUF (10^18 wei, since TRUF has 18 decimals)
CREATE OR REPLACE ACTION eth_truf_transfer($to_address TEXT, $amount TEXT) PUBLIC {
  $recipient_lower TEXT := LOWER($to_address);

  -- Validate Ethereum address format
  if NOT check_ethereum_address($recipient_lower) {
    ERROR('Invalid Ethereum address format. Must be a valid Ethereum address: ' || $to_address);
  }

  -- Validate amount is positive
  if $amount::NUMERIC(78, 0) <= 0::NUMERIC(78, 0) {
    ERROR('Transfer amount must be positive');
  }

  -- Fee
  $fee := 1000000000000000000::NUMERIC(78, 0); -- 1 TRUF with 18 decimals
  $caller_balance := COALESCE(eth_truf.balance(@caller), 0::NUMERIC(78, 0));

  IF @leader_sender IS NULL {
    ERROR('Leader address not available for fee transfer');
  }
  $leader_hex TEXT := encode(@leader_sender, 'hex')::TEXT;

  IF ($caller_balance < ($amount::NUMERIC(78, 0) + $fee)) {
    ERROR('Insufficient balance for transfer. Requires an extra 1 TRUF fee on top of the transfer amount');
  }

  eth_truf.transfer($leader_hex, $fee);

  -- Execute transfer using the bridge extension
  eth_truf.transfer($to_address, $amount::NUMERIC(78, 0));

  record_transaction_event(
    4,
    $fee,
    '0x' || $leader_hex,
    NULL
  );
};


-- USDC p2p transfer
-- Fee: 1 USDC (10^6 wei, since USDC has 6 decimals)
CREATE OR REPLACE ACTION eth_usdc_transfer($to_address TEXT, $amount TEXT) PUBLIC {
  $recipient_lower TEXT := LOWER($to_address);

  -- Validate Ethereum address format
  if NOT check_ethereum_address($recipient_lower) {
    ERROR('Invalid Ethereum address format. Must be a valid Ethereum address: ' || $to_address);
  }

  -- Validate amount is positive
  if $amount::NUMERIC(78, 0) <= 0::NUMERIC(78, 0) {
    ERROR('Transfer amount must be positive');
  }

  -- Fee
  $fee := 1000000::NUMERIC(78, 0); -- 1 USDC with 6 decimals
  $caller_balance := COALESCE(eth_usdc.balance(@caller), 0::NUMERIC(78, 0));

  IF @leader_sender IS NULL {
    ERROR('Leader address not available for fee transfer');
  }
  $leader_hex TEXT := encode(@leader_sender, 'hex')::TEXT;

  IF ($caller_balance < ($amount::NUMERIC(78, 0) + $fee)) {
    ERROR('Insufficient balance for transfer. Requires an extra 1 USDC fee on top of the transfer amount');
  }

  eth_usdc.transfer($leader_hex, $fee);

  -- Execute transfer using the bridge extension
  eth_usdc.transfer($to_address, $amount::NUMERIC(78, 0));

  record_transaction_event(
    4,
    $fee,
    '0x' || $leader_hex,
    NULL
  );
};
