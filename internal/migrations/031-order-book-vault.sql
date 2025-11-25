/*
 * ORDER BOOK VAULT HELPERS
 *
 * Provides helper actions for vault/collateral operations:
 * - ob_lock_collateral: Lock user's tokens into network vault
 * - ob_unlock_collateral: Return tokens from network to user
 * - ob_get_or_create_participant: Get or create participant record
 *
 * IMPORTANT: The "vault" is the network's ownedBalance in the ERC20 bridge,
 * stored in reward_instances.balance. We use lock()/unlock() methods.
 *
 * TODO: When USDC bridge is deployed (see "Goal: Deposit/withdraw contracts"),
 *       replace ethereum_bridge with usdc_bridge in all collateral operations.
 */

-- =============================================================================
-- ob_lock_collateral: Lock user's collateral into network vault
-- =============================================================================
-- Uses the ERC20 bridge's lock() method which:
-- - Decreases user's balance in kwil_erc20_meta.balances
-- - Increases network's ownedBalance in reward_instances.balance
--
-- TODO: Replace ethereum_bridge with usdc_bridge when USDC bridge is deployed
CREATE OR REPLACE ACTION ob_lock_collateral($amount NUMERIC(78, 0))
PRIVATE {
    -- Validate amount
    if $amount IS NULL OR $amount <= 0::NUMERIC(78, 0) {
        ERROR('Lock amount must be positive');
    }

    -- Lock collateral using bridge (user -> network ownedBalance)
    -- TODO: Change to usdc_bridge.lock($amount) when USDC bridge is available
    ethereum_bridge.lock($amount);
};

-- =============================================================================
-- ob_unlock_collateral: Return collateral from network vault to user
-- =============================================================================
-- Uses the ERC20 bridge's unlock() method which:
-- - Decreases network's ownedBalance
-- - Increases user's balance
--
-- NOTE: This is effectively a SYSTEM action - unlock() requires owner permission
-- TODO: Replace ethereum_bridge with usdc_bridge when USDC bridge is deployed
CREATE OR REPLACE ACTION ob_unlock_collateral($user_address TEXT, $amount NUMERIC(78, 0))
PRIVATE {
    -- Validate inputs (must be 0x-prefixed 40 hex character address)
    if $user_address IS NULL OR length($user_address) != 42 OR substring(LOWER($user_address), 1, 2) != '0x' {
        ERROR('Invalid user address format (expected 0x-prefixed hex, 42 chars)');
    }

    if $amount IS NULL OR $amount <= 0::NUMERIC(78, 0) {
        ERROR('Unlock amount must be positive');
    }

    -- Unlock collateral using bridge (network ownedBalance -> user)
    -- TODO: Change to usdc_bridge.unlock($user_address, $amount) when USDC bridge is available
    ethereum_bridge.unlock($user_address, $amount);
};

-- =============================================================================
-- ob_get_or_create_participant: Get or create participant record for caller
-- =============================================================================
-- Returns the participant_id for the caller, creating a new record if needed.
-- This provides a compact integer ID for efficient FK references in positions table.
CREATE OR REPLACE ACTION ob_get_or_create_participant()
PRIVATE RETURNS (participant_id INT) {
    -- Validate @caller format (must be 0x-prefixed 40 hex character address)
    -- Note: @caller is set by the Kwil engine, so this is mostly defensive
    if @caller IS NULL OR length(@caller) != 42 OR substring(LOWER(@caller), 1, 2) != '0x' {
        ERROR('Invalid caller address format');
    }

    -- Convert @caller (TEXT like '0x...') to BYTEA (20 bytes)
    $caller_bytes BYTEA := decode(substring(LOWER(@caller), 3, 40), 'hex');

    -- Try to find existing participant
    for $row in SELECT id FROM ob_participants WHERE wallet_address = $caller_bytes {
        RETURN $row.id;
    }

    -- Create new participant with MAX(id) + 1 pattern
    INSERT INTO ob_participants (id, wallet_address)
    SELECT COALESCE(MAX(id), 0) + 1, $caller_bytes
    FROM ob_participants;

    -- Get the ID we just inserted
    for $row in SELECT id FROM ob_participants WHERE wallet_address = $caller_bytes {
        RETURN $row.id;
    }

    -- Should never reach here
    ERROR('Failed to create participant');
};

-- =============================================================================
-- ob_get_participant_id: Get participant ID without creating (for lookups)
-- =============================================================================
-- Returns the participant_id for a wallet address, or NULL if not found.
-- Use this for read-only operations where we don't want to create records.
CREATE OR REPLACE ACTION ob_get_participant_id($wallet_address TEXT)
PRIVATE VIEW RETURNS (participant_id INT) {
    -- Validate address format (must be 0x-prefixed 40 hex character address)
    if $wallet_address IS NULL OR length($wallet_address) != 42 OR substring(LOWER($wallet_address), 1, 2) != '0x' {
        RETURN NULL;
    }

    $wallet_bytes BYTEA := decode(substring(LOWER($wallet_address), 3, 40), 'hex');

    for $row in SELECT id FROM ob_participants WHERE wallet_address = $wallet_bytes {
        RETURN $row.id;
    }

    RETURN NULL;
};
