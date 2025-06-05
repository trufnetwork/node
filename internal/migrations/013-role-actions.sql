/**
 * grant_role: Assigns a role to a specific wallet.
 *
 * Permissions:
 * - Only the role owner or a designated manager can execute this action.
 *
 * Validations:
 * - Validates wallet and owner addresses.
 * - Ensures the role exists.
 * - The action is idempotent; granting an existing role membership will not cause an error.
 */
CREATE OR REPLACE ACTION grant_role(
    $owner TEXT,
    $role_name TEXT,
    $wallet TEXT
) PUBLIC {
    $owner := LOWER($owner);
    $role_name := LOWER($role_name);
    $wallet := LOWER($wallet);
    $caller := LOWER(@caller);

    -- Input validation
    IF NOT check_ethereum_address($wallet) {
        ERROR('Invalid wallet address');
    }
    IF $owner != 'system' AND NOT check_ethereum_address($owner) {
        ERROR('Invalid owner address');
    }

    -- Check if role exists
    $role_owner TEXT;
    $found_role BOOL := FALSE;
    FOR $r IN SELECT owner FROM roles WHERE owner = $owner AND role_name = $role_name LIMIT 1 {
        $role_owner := $r.owner;
        $found_role := TRUE;
    }
    IF NOT $found_role {
        ERROR('Role does not exist');
    }

    -- Permission check: caller must be owner or a manager.
    -- For system roles (owner='system'), only a manager can grant them as no wallet can be the owner.
    $is_owner BOOL := $caller = $role_owner;
    $is_manager BOOL := FALSE;
    FOR $m IN SELECT 1 FROM role_managers WHERE owner = $owner AND role_name = $role_name AND manager_wallet = $caller LIMIT 1 {
        $is_manager := TRUE;
    }

    IF NOT $is_owner AND NOT $is_manager {
        ERROR('Only role owner or a manager can grant roles');
    }
    
    -- Idempotency check: if member already exists, do nothing
    FOR $m IN SELECT 1 FROM role_members WHERE owner = $owner AND role_name = $role_name AND wallet = $wallet LIMIT 1 {
        RETURN; -- already a member, successfully do nothing
    }

    -- Insert into role_members
    INSERT INTO role_members (owner, role_name, wallet, granted_at, granted_by)
    VALUES ($owner, $role_name, $wallet, @height, $caller);
};

/**
 * revoke_role: Removes a role from a specific wallet.
 *
 * Permissions:
 * - Only the role owner or a designated manager can execute this action.
 *
 * Validations:
 * - Validates wallet and owner addresses.
 * - Ensures the role exists.
 * - Ensures the wallet is actually a member before attempting revocation.
 */
CREATE OR REPLACE ACTION revoke_role(
    $owner TEXT,
    $role_name TEXT,
    $wallet TEXT
) PUBLIC {
    $owner := LOWER($owner);
    $role_name := LOWER($role_name);
    $wallet := LOWER($wallet);
    $caller := LOWER(@caller);

    -- Input validation
    IF NOT check_ethereum_address($wallet) {
        ERROR('Invalid wallet address');
    }
    IF $owner != 'system' AND NOT check_ethereum_address($owner) {
        ERROR('Invalid owner address');
    }

    -- Check if role exists
    $role_owner TEXT;
    $found_role BOOL := FALSE;
    FOR $r IN SELECT owner FROM roles WHERE owner = $owner AND role_name = $role_name LIMIT 1 {
        $role_owner := $r.owner;
        $found_role := TRUE;
    }
    IF NOT $found_role {
        ERROR('Role does not exist');
    }

    -- Permission check: caller must be owner or a manager.
    $is_owner BOOL := $caller = $role_owner;
    $is_manager BOOL := FALSE;
    FOR $m IN SELECT 1 FROM role_managers WHERE owner = $owner AND role_name = $role_name AND manager_wallet = $caller LIMIT 1 {
        $is_manager := TRUE;
    }

    IF NOT $is_owner AND NOT $is_manager {
        ERROR('Only role owner or a manager can revoke roles');
    }

    -- Check if wallet is actually a member
    $member_exists BOOL := FALSE;
    FOR $m IN SELECT 1 FROM role_members WHERE owner = $owner AND role_name = $role_name AND wallet = $wallet LIMIT 1 {
        $member_exists := TRUE;
    }

    IF NOT $member_exists {
        ERROR('Wallet is not a member of role');
    }

    -- Delete from role_members
    DELETE FROM role_members WHERE owner = $owner AND role_name = $role_name AND wallet = $wallet;
};

/**
 * is_member_of: Checks if a wallet is a member of a specific role.
 * This is a public view action and requires no special permissions.
 */
CREATE OR REPLACE ACTION is_member_of(
    $owner TEXT,
    $role_name TEXT,
    $wallet TEXT
) PUBLIC VIEW RETURNS (is_member BOOL) {
    $owner := LOWER($owner);
    $role_name := LOWER($role_name);
    $wallet := LOWER($wallet);
    
    -- Input validation
    IF NOT check_ethereum_address($wallet) {
        ERROR('Invalid wallet address');
    }
    IF $owner != 'system' AND NOT check_ethereum_address($owner) {
        ERROR('Invalid owner address');
    }

    -- Check if role exists before checking membership
    $role_exists BOOL := FALSE;
    FOR $r IN SELECT 1 FROM roles WHERE owner = $owner AND role_name = $role_name LIMIT 1 {
        $role_exists := TRUE;
    }
    IF NOT $role_exists {
        ERROR('Role does not exist');
    }

    -- Check for membership
    FOR $m IN SELECT 1 FROM role_members WHERE owner = $owner AND role_name = $role_name AND wallet = $wallet LIMIT 1 {
        RETURN TRUE;
    }

    RETURN FALSE;
};