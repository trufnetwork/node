-- TESTNET
CREATE OR REPLACE ACTION sepolia_get_erc20_bridge_info() 
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

CREATE OR REPLACE ACTION sepolia_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := sepolia_bridge.balance($wallet_address);
  RETURN $balance;
};

CREATE OR REPLACE ACTION sepolia_bridge_tokens($recipient TEXT DEFAULT NULL, $amount TEXT) PUBLIC {
  sepolia_bridge.bridge(COALESCE($recipient, @caller), $amount::NUMERIC(78, 0));
};

-- MAINNET
CREATE OR REPLACE ACTION ethereum_get_erc20_bridge_info() 
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
  FOR $row IN ethereum_bridge.info() {
    RETURN $row.chain, $row.escrow, $row.epoch_period, $row.erc20, $row.decimals, $row.balance, $row.synced, $row.synced_at, $row.enabled;
  }
};

CREATE OR REPLACE ACTION ethereum_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := ethereum_bridge.balance($wallet_address);
  RETURN $balance;
};

CREATE OR REPLACE ACTION ethereum_bridge_tokens($recipient TEXT DEFAULT NULL, $amount TEXT) PUBLIC {
  ethereum_bridge.bridge(COALESCE($recipient, @caller), $amount::NUMERIC(78, 0));
};
