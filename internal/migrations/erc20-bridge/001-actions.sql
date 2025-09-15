-- TESTNET
CREATE OR REPLACE ACTION sepolia_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := sepolia_bridge.balance($wallet_address);
  return $balance;
};

CREATE OR REPLACE ACTION sepolia_admin_bridge_tokens($amount TEXT) PUBLIC {
  -- Calculate 1% fee and lock it in our treasury
  $numAmount := $amount::NUMERIC(78, 0);
  $fee := $numAmount * 0.01;
  sepolia_bridge.lock($fee);

  -- Bridge the rest to users
  sepolia_bridge.bridge($numAmount - $fee);
};

-- MAINNET
CREATE OR REPLACE ACTION mainnet_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := mainnet_bridge.balance($wallet_address);
  return $balance;
};

CREATE OR REPLACE ACTION mainnet_admin_bridge_tokens($amount TEXT) PUBLIC {
  -- Calculate 1% fee and lock it in our treasury
  $numAmount := $amount::NUMERIC(78, 0);
  $fee := $numAmount * 0.01;
  mainnet_bridge.lock($fee);

  -- Bridge the rest to users
  mainnet_bridge.bridge($numAmount - $fee);
};