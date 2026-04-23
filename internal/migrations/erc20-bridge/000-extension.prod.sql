-- =============================================================================
-- MAINNET BRIDGE DECLARATIONS — apply BEFORE any other *.prod.sql override
-- =============================================================================
-- Manual-apply mainnet override. The embedded migration loader skips
-- *.prod.sql, so apply via:
--
--     kwil-cli exec-sql --file <this file> --sync \
--         --private-key $PRIVATE_KEY --provider $PROVIDER
--
-- Registers the two production ERC20 bridges that replace the testnet
-- hoodi_tt (TRUF) and hoodi_tt2 (USDC) instances. Once these aliases
-- exist, apply the script-generated *.prod.sql files (031, 032, 033,
-- 037, erc20-bridge/{001,004,005}) which redefine the order-book
-- actions and per-bridge wrappers to target eth_truf / eth_usdc.
--
-- Cleanup of the testnet aliases (UNUSE hoodi_tt; UNUSE hoodi_tt2;
-- UNUSE sepolia_bridge;) is intentionally not bundled here — apply
-- those manually after verifying the new bridges are healthy.
-- =============================================================================

-- TRUF token bridge (proxy: 0x5E4d32D5072A2cF94E4A20CD715E1aa76Cd52e43)
-- Implementation: 0xc6E82D96D2B0204e6467B67752817939Ec6162e9
-- Replaces testnet hoodi_tt — used for the market-creation fee.
USE erc20 {
    chain: 'ethereum',
    escrow: '0x5E4d32D5072A2cF94E4A20CD715E1aa76Cd52e43',
    distribution_period: '10m'
} AS eth_truf;

-- USDC bridge (proxy: 0x67f087bC88E36721919A4531b83C62C5022Ca810)
-- Implementation: 0x618AdFd0bc1802De96c758628Ea5eA6C32F48Bf0
-- Replaces testnet hoodi_tt2 — used for order-book collateral.
USE erc20 {
    chain: 'ethereum',
    escrow: '0x67f087bC88E36721919A4531b83C62C5022Ca810',
    distribution_period: '10m'
} AS eth_usdc;
