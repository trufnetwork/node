-- This is not meant to be run on tests as the contract address is valid for mainnet only.
-- USE erc20 {
--     chain: 'sepolia',
--     escrow: '0xf6009345d772a04f4e52150b5ec91617db79be58',
--     distribution_period: '30m'
-- } AS sepolia_bridge;

-- USE erc20 {
--     chain: 'ethereum',
--     escrow: '0x964dd66477c22BC5726dfeBCf9BA49aa8625978C',
--     distribution_period: '30m'
-- } AS ethereum_bridge;

-- TESTING ONLY ---
-- The following is for test environments where the sepolia bridge is used. Please comment for production and use the above.
USE erc20 {
    chain: 'sepolia',
    escrow: '0x502430eD0BbE0f230215870c9C2853e126eE5Ae3'
} AS sepolia_bridge;

-- The following is for test environments where the hoodi bridge is used. Please comment for production and use the above.
USE erc20 {
    chain: 'hoodi',
    escrow: '0x878d6aaeb6e746033f50b8dc268d54b4631554e7',
    distribution_period: '10m'
} AS hoodi_bridge;