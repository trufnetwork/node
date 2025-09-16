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

  -- Execute transfer using the bridge extension
  sepolia_bridge.transfer($to_address, $amount::NUMERIC(78, 0));
};


-- MAINNET TRANSFERS
CREATE OR REPLACE ACTION mainnet_transfer($to_address TEXT, $amount TEXT) PUBLIC {
  -- Validate Ethereum address format
  if NOT check_ethereum_address($to_address) {
    ERROR('Invalid Ethereum address format. Must be a valid Ethereum address: ' || $to_address);
  }

  -- Validate amount is positive
  if $amount::NUMERIC(78, 0) <= 0::NUMERIC(78, 0) {
    ERROR('Transfer amount must be positive');
  }

  -- Execute transfer using the bridge extension
  mainnet_bridge.transfer($to_address, $amount::NUMERIC(78, 0));
};

