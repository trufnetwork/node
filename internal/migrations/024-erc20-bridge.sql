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
  $num_amount := $amount::NUMERIC(78, 0);
  
  -- Get fee configuration for withdraw
  $found := 0
  FOR $config IN SELECT fee_percentage, treasury_address FROM fee_configs WHERE operation_type = 'withdraw' ORDER BY created_at DESC LIMIT 1 {
    $fee := $num_amount * $config.fee_percentage;

    IF $fee > 0::NUMERIC(78, 0) {
      sepolia_bridge.lock($fee);
      sepolia_bridge.unlock($config.treasury_address, $fee);
    }
    
    -- Bridge the rest to users
    sepolia_bridge.bridge($num_amount - $fee);
    $found := 1;
  }

  IF $found = 0 { ERROR('No fee_configs rows for operation_type=withdraw'); }
};

-- MAINNET
CREATE OR REPLACE ACTION mainnet_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := mainnet_bridge.balance($wallet_address);
  RETURN $balance;
};

CREATE OR REPLACE ACTION sepolia_admin_bridge_tokens($amount TEXT) PUBLIC {
  $num_amount := $amount::NUMERIC(78, 0);
  
  -- Get fee configuration for withdraw
  $found := 0
  FOR $config IN SELECT fee_percentage, treasury_address FROM fee_configs WHERE operation_type = 'withdraw' ORDER BY created_at DESC LIMIT 1 {
    $fee := $num_amount * $config.fee_percentage;

    IF $fee > 0::NUMERIC(78, 0) {
      mainnet_bridge.lock($fee);
      mainnet_bridge.unlock($config.treasury_address, $fee);
    }
    
    -- Bridge the rest to users
    mainnet_bridge.bridge($num_amount - $fee);
    $found := 1;
  }

  IF $found = 0 { ERROR('No fee_configs rows for operation_type=withdraw'); }
};