-- History Actions for ERC20 Bridges
-- These actions expose the unified transaction history for each bridge instance.

-- HOODI TESTNET - Test Token (TT)
CREATE OR REPLACE ACTION hoodi_tt_get_history($wallet_address TEXT, $limit INT, $offset INT)
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
    FOR $row IN hoodi_tt.get_history($wallet_address, $limit, $offset) {
        RETURN NEXT $row.type, $row.amount, $row.from_address, $row.to_address, 
                    $row.internal_tx_hash, $row.external_tx_hash, $row.status, 
                    $row.block_height, $row.block_timestamp, $row.external_block_height;
    }
};

-- HOODI TESTNET - Test Token 2 (TT2)
CREATE OR REPLACE ACTION hoodi_tt2_get_history($wallet_address TEXT, $limit INT, $offset INT)
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
    FOR $row IN hoodi_tt2.get_history($wallet_address, $limit, $offset) {
        RETURN NEXT $row.type, $row.amount, $row.from_address, $row.to_address, 
                    $row.internal_tx_hash, $row.external_tx_hash, $row.status, 
                    $row.block_height, $row.block_timestamp, $row.external_block_height;
    }
};

-- SEPOLIA TESTNET
CREATE OR REPLACE ACTION sepolia_get_history($wallet_address TEXT, $limit INT, $offset INT)
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
    FOR $row IN sepolia_bridge.get_history($wallet_address, $limit, $offset) {
        RETURN NEXT $row.type, $row.amount, $row.from_address, $row.to_address, 
                    $row.internal_tx_hash, $row.external_tx_hash, $row.status, 
                    $row.block_height, $row.block_timestamp, $row.external_block_height;
    }
};

-- MAINNET
CREATE OR REPLACE ACTION ethereum_get_history($wallet_address TEXT, $limit INT, $offset INT)
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
    FOR $row IN ethereum_bridge.get_history($wallet_address, $limit, $offset) {
        RETURN NEXT $row.type, $row.amount, $row.from_address, $row.to_address, 
                    $row.internal_tx_hash, $row.external_tx_hash, $row.status, 
                    $row.block_height, $row.block_timestamp, $row.external_block_height;
    }
};
