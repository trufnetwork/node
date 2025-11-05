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
  -- ===== FEE COLLECTION (NO EXEMPTION - USER-FACING OPERATION) =====
  $withdrawal_fee := '40000000000000000000'::NUMERIC(78, 0); -- 40 TRUF with 18 decimals
  $withdrawal_amount := $amount::NUMERIC(78, 0);
  $total_required := $withdrawal_amount + $withdrawal_fee;

  $caller_balance := COALESCE(ethereum_bridge.balance(@caller), 0::NUMERIC(78, 0));

  IF $caller_balance < $total_required {
    ERROR('Insufficient balance for withdrawal. Required: ' ||
          ($total_required / '1000000000000000000'::NUMERIC(78, 0))::TEXT ||
          ' TRUF (' || $withdrawal_amount::TEXT || ' wei withdrawal + ' ||
          ($withdrawal_fee / '1000000000000000000'::NUMERIC(78, 0))::TEXT || ' TRUF fee)');
  }

  $leader_addr TEXT := encode(@leader_sender, 'hex')::TEXT;
  ethereum_bridge.transfer($leader_addr, $withdrawal_fee);
  -- ===== END FEE COLLECTION =====

  -- Execute withdrawal using the bridge extension
  sepolia_bridge.bridge(COALESCE($recipient, @caller), $withdrawal_amount);
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
  -- ===== FEE COLLECTION (NO EXEMPTION - USER-FACING OPERATION) =====
  $withdrawal_fee := '40000000000000000000'::NUMERIC(78, 0); -- 40 TRUF with 18 decimals
  $withdrawal_amount := $amount::NUMERIC(78, 0);
  $total_required := $withdrawal_amount + $withdrawal_fee;

  $caller_balance := COALESCE(ethereum_bridge.balance(@caller), 0::NUMERIC(78, 0));

  IF $caller_balance < $total_required {
    ERROR('Insufficient balance for withdrawal. Required: ' ||
          ($total_required / '1000000000000000000'::NUMERIC(78, 0))::TEXT ||
          ' TRUF (' || $withdrawal_amount::TEXT || ' wei withdrawal + ' ||
          ($withdrawal_fee / '1000000000000000000'::NUMERIC(78, 0))::TEXT || ' TRUF fee)');
  }

  $leader_addr TEXT := encode(@leader_sender, 'hex')::TEXT;
  ethereum_bridge.transfer($leader_addr, $withdrawal_fee);
  -- ===== END FEE COLLECTION =====

  -- Execute withdrawal using the bridge extension
  ethereum_bridge.bridge(COALESCE($recipient, @caller), $withdrawal_amount);
};
