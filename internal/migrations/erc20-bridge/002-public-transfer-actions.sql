-- Public Transfer Actions for TRUF Token Operations
-- Enables users to transfer TRUF tokens between addresses on TN

-- SEPOLIA TESTNET TRANSFERS
CREATE OR REPLACE ACTION sepolia_transfer($to_address TEXT, $amount TEXT) PUBLIC {
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
  $caller_balance := COALESCE(sepolia_bridge.balance(@caller), 0::NUMERIC(78, 0));

  IF @leader_sender IS NULL {
    ERROR('Leader address not available for fee transfer');
  }
  $leader_hex TEXT := encode(@leader_sender, 'hex')::TEXT;

  IF ($caller_balance < ($amount::NUMERIC(78, 0) + $fee)) {
    ERROR('Insufficient balance for transfer. Requires an extra 1 TRUF fee on top of the transfer amount');
  }

  sepolia_bridge.transfer($leader_hex, $fee);

  -- Execute transfer using the bridge extension
  sepolia_bridge.transfer($to_address, $amount::NUMERIC(78, 0));

  record_transaction_event(
    4,
    $fee,
    '0x' || $leader_hex,
    NULL
  );
};


-- MAINNET TRANSFERS
CREATE OR REPLACE ACTION ethereum_transfer($to_address TEXT, $amount TEXT) PUBLIC {
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
  $caller_balance := COALESCE(ethereum_bridge.balance(@caller), 0::NUMERIC(78, 0));

  IF @leader_sender IS NULL {
    ERROR('Leader address not available for fee transfer');
  }
  $leader_hex TEXT := encode(@leader_sender, 'hex')::TEXT;

  IF ($caller_balance < ($amount::NUMERIC(78, 0) + $fee)) {
    ERROR('Insufficient balance for transfer. Requires an extra 1 TRUF fee on top of the transfer amount');
  }

  ethereum_bridge.transfer($leader_hex, $fee);

  -- Execute transfer using the bridge extension
  ethereum_bridge.transfer($to_address, $amount::NUMERIC(78, 0));

  record_transaction_event(
    4,
    $fee,
    '0x' || $leader_hex,
    NULL
  );
};
