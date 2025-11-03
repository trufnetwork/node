-- Public Transfer Actions for TRUF Token Operations
-- Enables users to transfer TRUF tokens between addresses on TN

-- SEPOLIA TESTNET TRANSFERS
CREATE OR REPLACE ACTION sepolia_transfer($to_address TEXT, $amount TEXT) PUBLIC {
  -- Validate Ethereum address format
  if NOT check_ethereum_address($to_address) {
    ERROR('Invalid Ethereum address format. Must be a valid Ethereum address: ' || $to_address);
  }

  -- Validate amount is positive
  if $amount::NUMERIC(78, 0) <= 0::NUMERIC(78, 0) {
    ERROR('Transfer amount must be positive');
  }

  $fee := 1000000000000000000::NUMERIC(78, 0); -- 1 TRUF with 18 decimals
  $caller_balance := ethereum_bridge.balance(@caller);

  IF ($caller_balance - $amount::NUMERIC(78, 0)) < $fee {
    ERROR('Insufficient balance for transfer. Requires an extra 1 TRUF fee on top of the transfer amount');
  }

  $leader_addr TEXT := encode(@leader_sender, 'hex')::TEXT;
  sepolia_bridge.transfer($leader_addr, $total_fee);

  -- Execute transfer using the bridge extension
  sepolia_bridge.transfer($to_address, $amount::NUMERIC(78, 0));
};


-- MAINNET TRANSFERS
CREATE OR REPLACE ACTION ethereum_transfer($to_address TEXT, $amount TEXT) PUBLIC {
  -- Validate Ethereum address format
  if NOT check_ethereum_address($to_address) {
    ERROR('Invalid Ethereum address format. Must be a valid Ethereum address: ' || $to_address);
  }

  -- Validate amount is positive
  if $amount::NUMERIC(78, 0) <= 0::NUMERIC(78, 0) {
    ERROR('Transfer amount must be positive');
  }

  -- Execute transfer using the bridge extension
  ethereum_bridge.transfer($to_address, $amount::NUMERIC(78, 0));
};

