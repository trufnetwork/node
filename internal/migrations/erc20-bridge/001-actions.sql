/**
    Bridge Mechanism:
    1. Call [chain]_admin_lock_tokens(), to lock x amount of tokens from user.
    2. Call [chain]_admin_issue_tokens(), to issue the locked tokens to layer-1 (escrow on ethereum).
    3. Frontend will call `list_wallet_rewards --namespace [chain]_bridge` to get inclusion proofs.
    4. Frontend will call claimReward function on escrow contract using the inclusion proofs given.
    5. Receiver wallet will get the tokens after claim is done.
*/

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

CREATE OR REPLACE ACTION sepolia_admin_issue_tokens($to_address TEXT, $amount TEXT) PUBLIC {
  sepolia_bridge.issue($to_address, $amount::NUMERIC(78, 0));
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

CREATE OR REPLACE ACTION mainnet_admin_issue_tokens($to_address TEXT, $amount TEXT) PUBLIC {
  mainnet_bridge.issue($to_address, $amount::NUMERIC(78, 0));
};