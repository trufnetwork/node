-- HOODI TESTNET
CREATE OR REPLACE ACTION hoodi_get_erc20_bridge_info()
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
  FOR $row IN hoodi_bridge.info() {
    RETURN $row.chain, $row.escrow, $row.epoch_period, $row.erc20, $row.decimals, $row.balance, $row.synced, $row.synced_at, $row.enabled;
  }
};

CREATE OR REPLACE ACTION hoodi_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := hoodi_bridge.balance($wallet_address);
  RETURN $balance;
};

CREATE OR REPLACE ACTION hoodi_bridge_tokens($recipient TEXT DEFAULT NULL, $amount TEXT) PUBLIC {
  -- ===== FEE COLLECTION (NO EXEMPTION - USER-FACING OPERATION) =====
  $withdrawal_fee := '40000000000000000000'::NUMERIC(78, 0); -- 40 TRUF with 18 decimals
  $withdrawal_amount := $amount::NUMERIC(78, 0);
  $total_required := $withdrawal_amount + $withdrawal_fee;

  $caller_balance := COALESCE(hoodi_bridge.balance(@caller), 0::NUMERIC(78, 0));

  IF $caller_balance < $total_required {
    ERROR('Insufficient balance for withdrawal. Required: ' ||
          ($total_required / '1000000000000000000'::NUMERIC(78, 0))::TEXT ||
          ' TRUF (' || $withdrawal_amount::TEXT || ' wei withdrawal + ' ||
          ($withdrawal_fee / '1000000000000000000'::NUMERIC(78, 0))::TEXT || ' TRUF fee)');
  }

  IF @leader_sender IS NULL {
    ERROR('Leader address not available for fee transfer');
  }
  $leader_hex TEXT := encode(@leader_sender, 'hex')::TEXT;
  hoodi_bridge.transfer($leader_hex, $withdrawal_fee);
  -- ===== END FEE COLLECTION =====

  $bridge_recipient TEXT := LOWER(COALESCE($recipient, @caller));

  -- Execute withdrawal using the bridge extension
  hoodi_bridge.bridge($bridge_recipient, $withdrawal_amount);

  record_transaction_event(
    5,
    $withdrawal_fee,
    '0x' || $leader_hex,
    NULL
  );
};

-- SEPOLIA TESTNET (kept for reference)
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

  IF @leader_sender IS NULL {
    ERROR('Leader address not available for fee transfer');
  }
  $leader_hex TEXT := encode(@leader_sender, 'hex')::TEXT;
  ethereum_bridge.transfer($leader_hex, $withdrawal_fee);
  -- ===== END FEE COLLECTION =====

  $bridge_recipient TEXT := LOWER(COALESCE($recipient, @caller));

  -- Execute withdrawal using the bridge extension
  sepolia_bridge.bridge($bridge_recipient, $withdrawal_amount);

  record_transaction_event(
    5,
    $withdrawal_fee,
    '0x' || $leader_hex,
    NULL
  );
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

  IF @leader_sender IS NULL {
    ERROR('Leader address not available for fee transfer');
  }
  $leader_hex TEXT := encode(@leader_sender, 'hex')::TEXT;
  ethereum_bridge.transfer($leader_hex, $withdrawal_fee);
  -- ===== END FEE COLLECTION =====

  $bridge_recipient TEXT := LOWER(COALESCE($recipient, @caller));

  -- Execute withdrawal using the bridge extension
  ethereum_bridge.bridge($bridge_recipient, $withdrawal_amount);

  record_transaction_event(
    5,
    $withdrawal_fee,
    '0x' || $leader_hex,
    NULL
  );
};
