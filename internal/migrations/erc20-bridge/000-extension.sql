-- This is not meant to be run on tests as the contract address is valid for mainnet only.

USE erc20 {
    chain: 'sepolia',
    escrow: '0xf6009345d772a04f4e52150b5ec91617db79be58',
    distribution_period: '30m'
} AS sepolia_bridge;

-- Note: uncomment the following to enable mainnet bridge instance
-- USE erc20 {
--     chain: 'ethereum',
--     escrow: '0x964dd66477c22BC5726dfeBCf9BA49aa8625978C',
--     distribution_period: '30m'
-- } AS ethereum_bridge;