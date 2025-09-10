-- TESTNET
CREATE OR REPLACE ACTION sepolia_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := sepolia_bridge.balance($wallet_address);
  return $balance;
};

CREATE OR REPLACE ACTION sepolia_admin_lock_tokens($wallet_address TEXT, $amount TEXT) PUBLIC {
  $lower_caller TEXT := LOWER(@caller);

  -- Permission Check: Ensure caller has the 'system:erc20_bridge_writer' role.
  $has_permission BOOL := false;
  for $row in are_members_of('system', 'erc20_bridge_writer', ARRAY[$lower_caller]) {
      if $row.wallet = $lower_caller AND $row.is_member {
          $has_permission := true;
          break;
      }
  }
  if NOT $has_permission {
      ERROR('Caller does not have the required system:erc20_bridge_writer role to lock tokens.');
  }

  sepolia_bridge.lock_admin($wallet_address, $amount::NUMERIC(78, 0));
};

CREATE OR REPLACE ACTION sepolia_admin_unlock_tokens($to_address TEXT, $amount TEXT) PUBLIC {
  $lower_caller TEXT := LOWER(@caller);

  -- Permission Check: Ensure caller has the 'system:erc20_bridge_writer' role.
  $has_permission BOOL := false;
  for $row in are_members_of('system', 'erc20_bridge_writer', ARRAY[$lower_caller]) {
      if $row.wallet = $lower_caller AND $row.is_member {
          $has_permission := true;
          break;
      }
  }
  if NOT $has_permission {
      ERROR('Caller does not have the required system:erc20_bridge_writer role to unlock tokens.');
  }

  sepolia_bridge.unlock($to_address, $amount::NUMERIC(78, 0));
};

CREATE OR REPLACE ACTION sepolia_admin_issue_tokens($to_address TEXT, $amount TEXT) PUBLIC {
  $lower_caller TEXT := LOWER(@caller);

  -- Permission Check: Ensure caller has the 'system:erc20_bridge_writer' role.
  $has_permission BOOL := false;
  for $row in are_members_of('system', 'erc20_bridge_writer', ARRAY[$lower_caller]) {
      if $row.wallet = $lower_caller AND $row.is_member {
          $has_permission := true;
          break;
      }
  }
  if NOT $has_permission {
      ERROR('Caller does not have the required system:erc20_bridge_writer role to issue tokens.');
  }

  sepolia_bridge.issue($to_address, $amount::NUMERIC(78, 0));
};

-- MAINNET
CREATE OR REPLACE ACTION mainnet_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := mainnet_bridge.balance($wallet_address);
  return $balance;
};

CREATE OR REPLACE ACTION mainnet_admin_lock_tokens($wallet_address TEXT, $amount TEXT) PUBLIC {
  $lower_caller TEXT := LOWER(@caller);

  -- Permission Check: Ensure caller has the 'system:erc20_bridge_writer' role.
  $has_permission BOOL := false;
  for $row in are_members_of('system', 'erc20_bridge_writer', ARRAY[$lower_caller]) {
      if $row.wallet = $lower_caller AND $row.is_member {
          $has_permission := true;
          break;
      }
  }
  if NOT $has_permission {
      ERROR('Caller does not have the required system:erc20_bridge_writer role to lock tokens.');
  }

  mainnet_bridge.lock_admin($wallet_address, $amount::NUMERIC(78, 0));
};

CREATE OR REPLACE ACTION mainnet_admin_unlock_tokens($to_address TEXT, $amount TEXT) PUBLIC {
  $lower_caller TEXT := LOWER(@caller);

  -- Permission Check: Ensure caller has the 'system:erc20_bridge_writer' role.
  $has_permission BOOL := false;
  for $row in are_members_of('system', 'erc20_bridge_writer', ARRAY[$lower_caller]) {
      if $row.wallet = $lower_caller AND $row.is_member {
          $has_permission := true;
          break;
      }
  }
  if NOT $has_permission {
      ERROR('Caller does not have the required system:erc20_bridge_writer role to unlock tokens.');
  }

  mainnet_bridge.unlock($to_address, $amount::NUMERIC(78, 0));
};

CREATE OR REPLACE ACTION mainnet_admin_issue_tokens($to_address TEXT, $amount TEXT) PUBLIC {
  $lower_caller TEXT := LOWER(@caller);

  -- Permission Check: Ensure caller has the 'system:erc20_bridge_writer' role.
  $has_permission BOOL := false;
  for $row in are_members_of('system', 'erc20_bridge_writer', ARRAY[$lower_caller]) {
      if $row.wallet = $lower_caller AND $row.is_member {
          $has_permission := true;
          break;
      }
  }
  if NOT $has_permission {
      ERROR('Caller does not have the required system:erc20_bridge_writer role to issue tokens.');
  }

  mainnet_bridge.issue($to_address, $amount::NUMERIC(78, 0));
};