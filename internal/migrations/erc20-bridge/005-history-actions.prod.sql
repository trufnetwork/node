-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/erc20-bridge/005-history-actions.sql
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

CREATE OR REPLACE ACTION eth_truf_get_history($wallet_address TEXT, $limit INT, $offset INT)
PUBLIC VIEW RETURNS TABLE (
    type TEXT,
    amount NUMERIC(78, 0),
    from_address BYTEA,
    to_address BYTEA,
    internal_tx_hash BYTEA,
    external_tx_hash BYTEA,
    status TEXT,
    block_height INT8,
    block_timestamp INT8,
    external_block_height INT8
) {
    FOR $row IN eth_truf.get_history($wallet_address, $limit, $offset) {
        RETURN NEXT $row.type, $row.amount, $row.from_address, $row.to_address, 
                    $row.internal_tx_hash, $row.external_tx_hash, $row.status, 
                    $row.block_height, $row.block_timestamp, $row.external_block_height;
    }
};

CREATE OR REPLACE ACTION eth_usdc_get_history($wallet_address TEXT, $limit INT, $offset INT)
PUBLIC VIEW RETURNS TABLE (
    type TEXT,
    amount NUMERIC(78, 0),
    from_address BYTEA,
    to_address BYTEA,
    internal_tx_hash BYTEA,
    external_tx_hash BYTEA,
    status TEXT,
    block_height INT8,
    block_timestamp INT8,
    external_block_height INT8
) {
    FOR $row IN eth_usdc.get_history($wallet_address, $limit, $offset) {
        RETURN NEXT $row.type, $row.amount, $row.from_address, $row.to_address, 
                    $row.internal_tx_hash, $row.external_tx_hash, $row.status, 
                    $row.block_height, $row.block_timestamp, $row.external_block_height;
    }
};
