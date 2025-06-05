/**
 * grant_roles: Assigns a role to a list of wallets.
 *
 * Permissions:
 * - Only the role owner or a designated manager can execute this action.
 *
 * Validations:
 * - Validates wallet and owner addresses.
 * - Ensures the role exists.
 * - The action is idempotent; granting an existing role membership will not cause an error.
 */
CREATE OR REPLACE ACTION grant_roles(
    $owner TEXT,
    $role_name TEXT,
    $wallets TEXT[]
) PUBLIC {
    $owner := LOWER($owner);
    $role_name := LOWER($role_name);
    $caller := LOWER(@caller);

    -- Input validation
    IF $owner != 'system' AND NOT check_ethereum_address($owner) {
        ERROR('Invalid owner address');
    }

    FOR $i in 1..array_length($wallets) {
        IF NOT check_ethereum_address($wallets[$i]) {
            ERROR('Invalid wallet address in array at index ' || $i::TEXT);
        }
        $wallets[$i] := LOWER($wallets[$i]);
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

    -- An empty wallet list is a no-op after validation.
    IF array_length($wallets) = 0 {
        RETURN;
    }

    -- Insert into role_members using a batch-friendly approach
    WITH RECURSIVE
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes WHERE idx < array_length($wallets)
    ),
    wallet_arrays AS (
        SELECT $wallets AS wallets
    ),
    wallet_list AS (
        SELECT wallet_arrays.wallets[idx] AS wallet
        FROM indexes
        JOIN wallet_arrays ON 1=1
    )
    INSERT INTO role_members (owner, role_name, wallet, granted_at, granted_by)
    SELECT $owner, $role_name, wallet, @height, $caller
    FROM wallet_list
    ON CONFLICT (owner, role_name, wallet) DO NOTHING;
};

/**
 * revoke_roles: Removes a role from a list of wallets.
 *
 * Permissions:
 * - Only the role owner or a designated manager can execute this action.
 *
 * Validations:
 * - Validates wallet and owner addresses.
 * - Ensures the role exists.
 * - Is idempotent; revoking a non-member does not error.
 */
CREATE OR REPLACE ACTION revoke_roles(
    $owner TEXT,
    $role_name TEXT,
    $wallets TEXT[]
) PUBLIC {
    $owner := LOWER($owner);
    $role_name := LOWER($role_name);
    $caller := LOWER(@caller);

    -- Input validation
    IF $owner != 'system' AND NOT check_ethereum_address($owner) {
        ERROR('Invalid owner address');
    }
    FOR $i in 1..array_length($wallets) {
        IF NOT check_ethereum_address($wallets[$i]) {
            ERROR('Invalid wallet address in array at index ' || $i::TEXT);
        }
        $wallets[$i] := LOWER($wallets[$i]);
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

    -- An empty wallet list is a no-op after validation.
    IF array_length($wallets) = 0 {
        RETURN;
    }

    -- Batch delete from role_members using a recursive CTE to unnest the array
    WITH RECURSIVE
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes WHERE idx < array_length($wallets)
    ),
    wallet_arrays AS (
        SELECT $wallets AS wallets
    ),
    wallets_to_delete AS (
        SELECT wallet_arrays.wallets[idx] as wallet_addr
        FROM indexes
        JOIN wallet_arrays ON 1=1
    )
    DELETE FROM role_members
    WHERE owner = $owner AND role_name = $role_name
    AND wallet IN (SELECT wallet_addr FROM wallets_to_delete);
};

/**
 * are_members_of: Checks if a list of wallets are members of a specific role.
 * This is a public view action and requires no special permissions.
 */
CREATE OR REPLACE ACTION are_members_of(
    $owner TEXT,
    $role_name TEXT,
    $wallets TEXT[]
) PUBLIC VIEW RETURNS TABLE (wallet TEXT, is_member BOOL) {
    $owner := LOWER($owner);
    $role_name := LOWER($role_name);

    -- Input validation
    IF $owner != 'system' AND NOT check_ethereum_address($owner) {
        ERROR('Invalid owner address');
    }
    FOR $i in 1..array_length($wallets) {
        IF NOT check_ethereum_address($wallets[$i]) {
            ERROR('Invalid wallet address in array at index ' || $i::TEXT);
        }
        $wallets[$i] := LOWER($wallets[$i]);
    }

    -- Check if role exists before checking membership
    $role_exists BOOL := FALSE;
    FOR $r IN SELECT 1 FROM roles WHERE owner = $owner AND role_name = $role_name LIMIT 1 {
        $role_exists := TRUE;
    }
    IF NOT $role_exists {
        ERROR('Role does not exist');
    }

    -- An empty wallet list should return an empty result set after validation.
    IF array_length($wallets) = 0 {
        RETURN;
    }

    -- Batch check for membership
    RETURN WITH RECURSIVE
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes WHERE idx < array_length($wallets)
    ),
    wallet_arrays AS (
        SELECT $wallets AS wallets
    ),
    arguments AS (
        SELECT wallet_arrays.wallets[idx] as wallet 
        FROM indexes
        JOIN wallet_arrays ON 1=1
    )
    SELECT
        a.wallet,
        (rm.wallet IS NOT NULL) AS is_member
    FROM arguments a
    LEFT JOIN role_members rm ON rm.owner = $owner
                             AND rm.role_name = $role_name
                             AND rm.wallet = a.wallet;
};