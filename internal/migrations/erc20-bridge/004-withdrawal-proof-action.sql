-- Withdrawal Proof Action for Hoodi Non-Custodial Bridge
-- This action exposes the list_wallet_rewards precompile as a public action.
-- Returns merkle proofs AND validator signatures - everything needed for withdrawal.
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
  proofs BYTEA[],
  signatures BYTEA[]
) {
  -- with_pending = false means only return confirmed epochs (ready for withdrawal)
  FOR $row IN hoodi_bridge.list_wallet_rewards($wallet_address, false) {
    RETURN $row.chain, $row.chain_id, $row.contract, $row.created_at,
           $row.param_recipient, $row.param_amount, $row.param_block_hash,
           $row.param_root, $row.param_proofs, $row.param_signatures;
  }
};
