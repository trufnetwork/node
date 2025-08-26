/**
 * =====================================================================================
 *                            ROLE-SPECIFIC HELPER ACTIONS
 * =====================================================================================
 */

/**
 * helper_assert_owner_addr: Validates an owner address for the roles context.
 * It checks if the address is a valid Ethereum address or the special 'system' identifier.
 */
CREATE OR REPLACE ACTION helper_assert_owner_addr($owner TEXT) PRIVATE VIEW {
    IF $owner != 'system' AND NOT check_ethereum_address($owner) {
        ERROR('Invalid owner address: ' || $owner);
    }
};

/**
 * helper_assert_role_exists: Checks if a role exists. Errors if not found.
 */
CREATE OR REPLACE ACTION helper_assert_role_exists($owner TEXT, $role_name TEXT) PRIVATE VIEW {
    $role_exists BOOL := FALSE;
    FOR $r IN SELECT 1 FROM roles WHERE owner = $owner AND role_name = $role_name LIMIT 1 {
        $role_exists := TRUE;
    }
    IF NOT $role_exists {
        ERROR('Role does not exist: ' || $owner || ':' || $role_name);
    }
};

/**
 * helper_assert_is_role_owner: Ensures the caller is the owner of a role.
 */
CREATE OR REPLACE ACTION helper_assert_is_role_owner($owner TEXT, $role_name TEXT) PRIVATE VIEW {
    IF LOWER(@caller) != $owner {
        ERROR('Caller ' || LOWER(@caller) || ' is not the owner of role ' || $owner || ':' || $role_name);
    }
};

/**
 * helper_assert_can_manage_members: Ensures the caller is the role owner or a member of the manager role.
 */
CREATE OR REPLACE ACTION helper_assert_can_manage_members($owner TEXT, $role_name TEXT) PRIVATE VIEW {
    $caller := LOWER(@caller);

    -- First, get the role's owner and manager role details
    $role_owner TEXT;
    $manager_owner TEXT;
    $manager_role_name TEXT;
    $found_role BOOL := FALSE;
    FOR $r IN SELECT owner, manager_owner, manager_role_name FROM roles WHERE owner = $owner AND role_name = $role_name LIMIT 1 {
        $role_owner := $r.owner;
        $manager_owner := $r.manager_owner;
        $manager_role_name := $r.manager_role_name;
        $found_role := TRUE;
    }
    IF NOT $found_role {
        ERROR('Role does not exist: ' || $owner || ':' || $role_name);
    }

    -- Check if the caller is the direct owner of the role
    $is_owner BOOL := $caller = $role_owner;
    IF $is_owner {
        RETURN;
    }

    -- If not the owner, check if they are a member of the manager role
    $is_manager BOOL := FALSE;
    IF $manager_owner IS NOT NULL AND $manager_role_name IS NOT NULL {
        -- Query role_members directly to check for membership
        FOR $r IN SELECT 1 FROM role_members
            WHERE owner = $manager_owner
              AND role_name = $manager_role_name
              AND wallet = $caller
            LIMIT 1
        {
            $is_manager := TRUE;
        }
    }

    -- If neither owner nor manager, then error
    IF NOT $is_manager {
        ERROR('Caller ' || $caller || ' is not the owner or a member of the manager role for ' || $owner || ':' || $role_name);
    }
};


/**
 * =====================================================================================
 *                              PUBLIC ROLE ACTIONS
 * =====================================================================================
 */

/**
 * grant_roles: Assigns a role to a list of wallets.
 *
 * Permissions:
 * - Only the role owner or members of the designated manager role can execute this action.
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

    helper_assert_owner_addr($owner);
    $wallets := helper_sanitize_wallets($wallets);
    helper_assert_can_manage_members($owner, $role_name);

    -- An empty wallet list is a no-op after validation.
    IF array_length($wallets) = 0 {
        RETURN;
    }

    -- Insert into role_members using UNNEST for optimal performance
    INSERT INTO role_members (owner, role_name, wallet, granted_at, granted_by)
    SELECT $owner, $role_name, t.wallet, @height, LOWER(@caller)
    FROM UNNEST($wallets) AS t(wallet)
    ON CONFLICT (owner, role_name, wallet) DO NOTHING;
};

/**
 * revoke_roles: Removes a role from a list of wallets.
 *
 * Permissions:
 * - Only the role owner or members of the designated manager role can execute this action.
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

    helper_assert_owner_addr($owner);
    $wallets := helper_sanitize_wallets($wallets);
    helper_assert_can_manage_members($owner, $role_name);

    -- An empty wallet list is a no-op after validation.
    IF array_length($wallets) = 0 {
        RETURN;
    }

    -- Batch delete from role_members using UNNEST for optimal performance
    DELETE FROM role_members
    WHERE owner = $owner AND role_name = $role_name
    AND wallet IN (SELECT t.wallet FROM UNNEST($wallets) AS t(wallet));
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

    helper_assert_owner_addr($owner);
    $wallets := helper_sanitize_wallets($wallets);
    helper_assert_role_exists($owner, $role_name);

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

CREATE OR REPLACE ACTION list_role_members(
    $owner TEXT,
    $role_name TEXT,
    $limit INT,
    $offset INT
) PUBLIC VIEW RETURNS TABLE (wallet TEXT, granted_at INT8, granted_by TEXT) {
    $owner := LOWER($owner);
    $role_name := LOWER($role_name);

    helper_assert_owner_addr($owner);
    helper_assert_role_exists($owner, $role_name);

    -- Enforce sensible defaults and bounds for pagination
    IF $limit IS NULL OR $limit <= 0 {
        $limit := 100;
    }
    IF $offset IS NULL OR $offset < 0 {
        $offset := 0;
    }

    RETURN SELECT wallet,
                  granted_at,
                  granted_by
           FROM role_members
           WHERE owner = $owner
             AND role_name = $role_name
           ORDER BY granted_at ASC
           LIMIT $limit OFFSET $offset;
};