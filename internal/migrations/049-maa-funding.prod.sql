-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/049-maa-funding.sql
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

CREATE OR REPLACE ACTION maa_dispatch_balance($bridge TEXT, $addr TEXT)
PRIVATE VIEW RETURNS (balance NUMERIC(78, 0)) {
    if $bridge = 'eth_usdc' {
        RETURN eth_usdc.balance($addr);
    } else if $bridge = 'eth_truf' {
        RETURN eth_truf.balance($addr);
    } else {
        ERROR('Invalid bridge. Supported: eth_usdc, eth_truf');
    }
};

CREATE OR REPLACE ACTION maa_dispatch_transfer($bridge TEXT, $to TEXT, $amount NUMERIC(78, 0))
PRIVATE {
    if $bridge = 'eth_usdc' {
        eth_usdc.transfer($to, $amount);
    } else if $bridge = 'eth_truf' {
        eth_truf.transfer($to, $amount);
    } else {
        ERROR('Invalid bridge. Supported: eth_usdc, eth_truf');
    }
};

CREATE OR REPLACE ACTION maa_dispatch_offramp($bridge TEXT, $recipient TEXT, $amount NUMERIC(78, 0))
PRIVATE {
    if $bridge = 'eth_usdc' {
        eth_usdc.bridge($recipient, $amount);
    } else if $bridge = 'eth_truf' {
        eth_truf.bridge($recipient, $amount);
    } else {
        ERROR('Invalid bridge. Supported: eth_usdc, eth_truf');
    }
};
