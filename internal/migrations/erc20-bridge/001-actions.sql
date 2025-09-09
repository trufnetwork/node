-- TESTNET
CREATE OR REPLACE ACTION sepolia_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := sepolia_bridge.balance($wallet_address);
  return $balance;
};

CREATE OR REPLACE ACTION sepolia_admin_lock_tokens($wallet_address TEXT, $amount TEXT) PUBLIC {
  sepolia_bridge.lock_admin($wallet_address, $amount::NUMERIC(78, 0));
};

CREATE OR REPLACE ACTION sepolia_admin_unlock_tokens($to_address TEXT, $amount TEXT) PUBLIC {
  sepolia_bridge.unlock($to_address, $amount::NUMERIC(78, 0));
};

-- MAINNET
CREATE OR REPLACE ACTION mainnet_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := mainnet_bridge.balance($wallet_address);
  return $balance;
};

CREATE OR REPLACE ACTION mainnet_admin_lock_tokens($wallet_address TEXT, $amount TEXT) PUBLIC {
  mainnet_bridge.lock_admin($wallet_address, $amount::NUMERIC(78, 0));
};

CREATE OR REPLACE ACTION mainnet_admin_unlock_tokens($to_address TEXT, $amount TEXT) PUBLIC {
  mainnet_bridge.unlock($to_address, $amount::NUMERIC(78, 0));
};