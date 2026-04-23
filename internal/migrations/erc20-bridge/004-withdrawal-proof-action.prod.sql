-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/erc20-bridge/004-withdrawal-proof-action.sql
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

CREATE OR REPLACE ACTION eth_truf_get_withdrawal_proof($wallet_address TEXT)
PUBLIC VIEW RETURNS TABLE (
  chain TEXT,
  chain_id TEXT,
  contract TEXT,
  created_at INT8,
  recipient TEXT,
  amount NUMERIC(78, 0),
  block_hash BYTEA,
  root BYTEA,
  proofs BYTEA[],
  signatures BYTEA[]
) {
  -- with_pending = false means only return confirmed epochs (ready for withdrawal)
  -- Returns ALL confirmed epochs ordered by height DESC (newest first)
  FOR $row IN eth_truf.list_wallet_rewards($wallet_address, false) {
    -- Return each row (don't exit loop!)
    RETURN NEXT $row.chain, $row.chain_id, $row.contract, $row.created_at,
           $row.param_recipient, $row.param_amount, $row.param_block_hash,
           $row.param_root, $row.param_proofs, $row.param_signatures;
  }
};

CREATE OR REPLACE ACTION eth_usdc_get_withdrawal_proof($wallet_address TEXT)
PUBLIC VIEW RETURNS TABLE (
  chain TEXT,
  chain_id TEXT,
  contract TEXT,
  created_at INT8,
  recipient TEXT,
  amount NUMERIC(78, 0),
  block_hash BYTEA,
  root BYTEA,
  proofs BYTEA[],
  signatures BYTEA[]
) {
  -- with_pending = false means only return confirmed epochs (ready for withdrawal)
  -- Returns ALL confirmed epochs ordered by height DESC (newest first)
  FOR $row IN eth_usdc.list_wallet_rewards($wallet_address, false) {
    -- Return each row (don't exit loop!)
    RETURN NEXT $row.chain, $row.chain_id, $row.contract, $row.created_at,
           $row.param_recipient, $row.param_amount, $row.param_block_hash,
           $row.param_root, $row.param_proofs, $row.param_signatures;
  }
};
