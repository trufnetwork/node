-- This is not meant to be run on tests as the contract address is valid for mainnet only.
-- USE erc20 {
--     chain: 'sepolia',
--     escrow: '0xf6009345d772a04f4e52150b5ec91617db79be58',
--     distribution_period: '10m'
-- } AS sepolia_bridge;

-- USE erc20 {
--     chain: 'ethereum',
--     escrow: '0x964dd66477c22BC5726dfeBCf9BA49aa8625978C',
--     distribution_period: '10m'
-- } AS ethereum_bridge;

-- TESTING ONLY ---
-- The following is for test environments where the sepolia bridge is used. Please comment for production and use the above.
USE erc20 {
    chain: 'sepolia',
    escrow: '0x502430eD0BbE0f230215870c9C2853e126eE5Ae3',
    distribution_period: '10m'
} AS sepolia_bridge;

-- The following is for test environments where the hoodi bridges are used. Please comment for production and use the above.
-- First hoodi bridge: Test Token (TT)
-- Token: 0x263ce78fef26600e4e428cebc91c2a52484b4fbf
-- Proxy: 0x878d6aaeb6e746033f50b8dc268d54b4631554e7
-- Explorer: https://hoodi.etherscan.io/address/0x878d6aaeb6e746033f50b8dc268d54b4631554e7
USE erc20 {
    chain: 'hoodi',
    escrow: '0x878d6aaeb6e746033f50b8dc268d54b4631554e7',
    distribution_period: '10m'
} AS hoodi_tt;

-- Second hoodi bridge: Test Token 2 (TT2)
-- Token: 0x1591DeAa21710E0BA6CC1b15F49620C9F65B2dEd
-- Proxy: 0x9BD843A3ce718FE639e9968860B933b026784687
-- Explorer: https://hoodi.etherscan.io/address/0x9BD843A3ce718FE639e9968860B933b026784687
USE erc20 {
    chain: 'hoodi',
    escrow: '0x9BD843A3ce718FE639e9968860B933b026784687',
    distribution_period: '10m'
} AS hoodi_tt2;