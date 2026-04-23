-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/erc20-bridge/001-actions.sql
-- Script : scripts/generate_prod_migrations.py
--
-- Manual-apply mainnet override. The embedded migration loader skips
-- *.prod.sql, so apply via:
--
--     kwil-cli exec-sql --file <this file> --sync \
--         --private-key $PRIVATE_KEY --provider $PROVIDER
--
-- Prerequisite: erc20-bridge/000-extension.prod.sql must be applied
-- FIRST so the eth_truf and eth_usdc bridge instances exist.
-- =============================================================================

CREATE OR REPLACE ACTION eth_truf_get_erc20_bridge_info()
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
  FOR $row IN eth_truf.info() {
    RETURN $row.chain, $row.escrow, $row.epoch_period, $row.erc20, $row.decimals, $row.balance, $row.synced, $row.synced_at, $row.enabled;
  }
};

CREATE OR REPLACE ACTION eth_truf_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := eth_truf.balance($wallet_address);
  RETURN $balance;
};

CREATE OR REPLACE ACTION eth_truf_bridge_tokens($recipient TEXT DEFAULT NULL, $amount TEXT) PUBLIC {
  $withdrawal_amount := $amount::NUMERIC(78, 0);
  $caller_balance := COALESCE(eth_truf.balance(@caller), 0::NUMERIC(78, 0));

  IF $caller_balance < $withdrawal_amount {
    ERROR('Insufficient balance for withdrawal. Required: ' ||
          ($withdrawal_amount / '1000000000000000000'::NUMERIC(78, 0))::TEXT || ' tokens');
  }

  $bridge_recipient TEXT := LOWER(COALESCE($recipient, @caller));

  -- Execute withdrawal using the bridge extension
  eth_truf.bridge($bridge_recipient, $withdrawal_amount);
};

CREATE OR REPLACE ACTION eth_usdc_get_erc20_bridge_info()
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
  FOR $row IN eth_usdc.info() {
    RETURN $row.chain, $row.escrow, $row.epoch_period, $row.erc20, $row.decimals, $row.balance, $row.synced, $row.synced_at, $row.enabled;
  }
};

CREATE OR REPLACE ACTION eth_usdc_wallet_balance($wallet_address TEXT) PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
  $balance := eth_usdc.balance($wallet_address);
  RETURN $balance;
};

CREATE OR REPLACE ACTION eth_usdc_bridge_tokens($recipient TEXT DEFAULT NULL, $amount TEXT) PUBLIC {
  $withdrawal_amount := $amount::NUMERIC(78, 0);
  $caller_balance := COALESCE(eth_usdc.balance(@caller), 0::NUMERIC(78, 0));

  IF $caller_balance < $withdrawal_amount {
    ERROR('Insufficient balance for withdrawal. Required: ' ||
          ($withdrawal_amount / '1000000000000000000'::NUMERIC(78, 0))::TEXT || ' tokens');
  }

  $bridge_recipient TEXT := LOWER(COALESCE($recipient, @caller));

  -- Execute withdrawal using the bridge extension
  eth_usdc.bridge($bridge_recipient, $withdrawal_amount);
};
