CREATE OR REPLACE ACTION get_erc20_bridge_info() 
PUBLIC VIEW RETURNS (
  chain TEXT,
  escrow TEXT,
  epoch_period TEXT,
  erc20 TEXT,
  decimals INT,
  balance NUMERIC(78, 0),
  synced BOOLEAN,
  synced_at INT8,
  enabled BOOLEAN
) {
  FOR $row IN sepolia_bridge.info() {
    RETURN $row.chain, $row.escrow, $row.epoch_period, $row.erc20, $row.decimals, $row.balance, $row.synced, $row.synced_at, $row.enabled;
  }
};

-- TESTNET
CREATE OR REPLACE ACTION sepolia_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := sepolia_bridge.balance($wallet_address);
  RETURN $balance;
};

CREATE OR REPLACE ACTION sepolia_admin_bridge_tokens($amount TEXT) PUBLIC {
  -- Calculate 1% fee and lock it in our treasury
  $num_amount := $amount::NUMERIC(78, 0);
  $fee := $num_amount * 0.01;
  sepolia_bridge.lock($fee);

  $treasury_address := '0xDe5B2aBce299eBdC3567895B1B4b02Ca2c33C94A';
  sepolia_bridge.unlock($treasury_address, $fee);

  -- Bridge the rest to users
  sepolia_bridge.bridge($num_amount - $fee);
};

-- MAINNET
CREATE OR REPLACE ACTION mainnet_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := mainnet_bridge.balance($wallet_address);
  RETURN $balance;
};

CREATE OR REPLACE ACTION mainnet_admin_bridge_tokens($amount TEXT) PUBLIC {
  -- Calculate 1% fee and lock it in our treasury
  $numAmount := $amount::NUMERIC(78, 0);
  $fee := $numAmount * 0.01;
  mainnet_bridge.lock($fee);

  -- TODO: update when we have treasury address on mainnet
  -- $treasury_address := ''
  -- sepolia_bridge.unlock($treasury_address, $fee);

  -- Bridge the rest to users
  mainnet_bridge.bridge($numAmount - $fee);
};