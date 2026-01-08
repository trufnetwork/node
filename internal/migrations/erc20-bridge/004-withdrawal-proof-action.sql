-- Withdrawal Proof Action for Hoodi Non-Custodial Bridge
-- This migration exposes the list_wallet_rewards precompile as a public action
-- for users to retrieve merkle proofs and validator signatures for withdrawals.

-- Get withdrawal proof for confirmed epochs on Hoodi
-- Returns merkle proof, signatures, and metadata needed for TrufNetworkBridge.withdraw()
CREATE OR REPLACE ACTION hoodi_get_withdrawal_proof($wallet_address TEXT)
PUBLIC VIEW RETURNS TABLE (
  chain TEXT,
  chain_id TEXT,
  contract TEXT,
  created_at INT8,
  recipient TEXT,
  amount NUMERIC(78, 0),
  block_hash BYTEA,
  root BYTEA,
  proofs BYTEA[]
) {
  -- with_pending = false means only return confirmed epochs (ready for withdrawal)
  FOR $row IN hoodi_bridge.list_wallet_rewards($wallet_address, false) {
    RETURN $row.chain, $row.chain_id, $row.contract, $row.created_at,
           $row.param_recipient, $row.param_amount, $row.param_block_hash,
           $row.param_root, $row.param_proofs;
  }
};
