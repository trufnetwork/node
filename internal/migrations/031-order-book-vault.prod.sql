-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/031-order-book-vault.sql
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

CREATE OR REPLACE ACTION ob_lock_collateral($bridge TEXT, $amount NUMERIC(78, 0))
PRIVATE {
    -- Validate bridge (will ERROR if invalid)
    -- Note: validate_bridge procedure defined in 032-order-book-actions.sql
    -- Since migrations run in order, that procedure will exist when this is called

    -- Validate amount
    if $amount IS NULL OR $amount <= 0::NUMERIC(78, 0) {
        ERROR('Lock amount must be positive');
    }

    -- Lock collateral using bridge (user -> network ownedBalance)
    if $bridge = 'eth_usdc' {
        eth_usdc.lock($amount);
    } else if $bridge = 'eth_truf' {
        eth_truf.lock($amount);
    } else {
        ERROR('Invalid bridge. Supported: eth_usdc, eth_truf');
    }
};

CREATE OR REPLACE ACTION ob_unlock_collateral($bridge TEXT, $user_address TEXT, $amount NUMERIC(78, 0))
PRIVATE {
    -- Validate inputs (must be 0x-prefixed 40 hex character address)
    if $user_address IS NULL OR length($user_address) != 42 OR substring(LOWER($user_address), 1, 2) != '0x' {
        ERROR('Invalid user address format (expected 0x-prefixed hex, 42 chars)');
    }

    if $amount IS NULL OR $amount <= 0::NUMERIC(78, 0) {
        ERROR('Unlock amount must be positive');
    }

    -- Unlock collateral using bridge (network ownedBalance -> user)
    if $bridge = 'eth_usdc' {
        eth_usdc.unlock($user_address, $amount);
    } else if $bridge = 'eth_truf' {
        eth_truf.unlock($user_address, $amount);
    } else {
        ERROR('Invalid bridge. Supported: eth_usdc, eth_truf');
    }
};
