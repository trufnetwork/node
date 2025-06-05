/**
 * add_role_managers: Assigns multiple managers to a role.
 * Only the role owner or the system can assign managers.
 */
CREATE OR REPLACE ACTION add_role_managers(
    $owner_address TEXT,
    $role_name TEXT,
    $manager_wallets TEXT[]
) PUBLIC {
    $owner_address := LOWER($owner_address);
    $role_name := LOWER($role_name);
    $lower_caller := LOWER(@caller);
    
    -- Validate owner address
    if $owner_address != 'system' AND NOT check_ethereum_address($owner_address) {
        ERROR('Invalid owner address: ' || $owner_address);
    }
    
    -- Validate all manager wallet addresses and check for conflicts
    FOR $i IN 1..array_length($manager_wallets) {
        $manager_wallet := $manager_wallets[$i];
        if NOT check_ethereum_address($manager_wallet) {
            ERROR('Invalid manager wallet address in array: ' || $manager_wallet);
        }
        if LOWER($manager_wallet) = $owner_address {
            ERROR('Role owner cannot be assigned as a manager of their own role');
        }
        $manager_wallets[$i] := LOWER($manager_wallet);
    }
    
    -- Check if role exists
    $role_exists BOOL := false;
    for $row in SELECT 1 FROM roles WHERE owner = $owner_address AND role_name = $role_name LIMIT 1 {
        $role_exists := true;
    }
    if $role_exists = false {
        ERROR('Role does not exist: owner=' || $owner_address || ' role_name=' || $role_name);
    }
    
    -- Check permissions: caller must be the role owner or system
    if $lower_caller != $owner_address AND $lower_caller != 'system' {
        ERROR('Only role owner or system can add managers');
    }

    -- An empty manager list is a no-op after validation.
    IF array_length($manager_wallets) = 0 {
        RETURN;
    }

    
    -- Add the managers, ignoring if they are already managers
    WITH RECURSIVE
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes WHERE idx < array_length($manager_wallets)
    ),
    manager_arrays AS (
        SELECT $manager_wallets AS manager_wallets
    ),
    manager_list AS (
        SELECT manager_arrays.manager_wallets[idx] AS manager_wallet
        FROM indexes
        JOIN manager_arrays ON 1=1
    )
    INSERT INTO role_managers (owner, role_name, manager_wallet, assigned_at, assigned_by)
    SELECT $owner_address, $role_name, manager_wallet, @height, $lower_caller
    FROM manager_list
    ON CONFLICT (owner, role_name, manager_wallet) DO NOTHING;
};

/**
 * remove_role_managers: Removes multiple managers from a role.
 * Only the role owner or the system can remove managers.
 */
CREATE OR REPLACE ACTION remove_role_managers(
    $owner_address TEXT,
    $role_name TEXT,
    $manager_wallets TEXT[]
) PUBLIC {
    $owner_address := LOWER($owner_address);
    $role_name := LOWER($role_name);
    $lower_caller := LOWER(@caller);

    -- Validate owner address
    if $owner_address != 'system' AND NOT check_ethereum_address($owner_address) {
        ERROR('Invalid owner address: ' || $owner_address);
    }

    -- Validate all manager wallet addresses
    FOR $i IN 1..array_length($manager_wallets) {
        if NOT check_ethereum_address($manager_wallets[$i]) {
            ERROR('Invalid manager wallet address in array: ' || $manager_wallets[$i]);
        }
        $manager_wallets[$i] := LOWER($manager_wallets[$i]);
    }

    -- Check permissions: caller must be the role owner or system
    if $lower_caller != $owner_address AND $lower_caller != 'system' {
        ERROR('Only role owner or system can remove managers');
    }

    -- An empty manager list is a no-op after validation.
    IF array_length($manager_wallets) = 0 {
        RETURN;
    }

    -- Remove the managers using a recursive CTE to unnest the array
    WITH RECURSIVE
    indexes AS (
        SELECT 1 AS idx
        UNION ALL
        SELECT idx + 1 FROM indexes WHERE idx < array_length($manager_wallets)
    ),
    manager_arrays AS (
        SELECT $manager_wallets AS manager_wallets
    ),
    managers_to_delete AS (
        SELECT manager_arrays.manager_wallets[idx] as manager_wallet_addr
        FROM indexes
        JOIN manager_arrays ON 1=1
    )
    DELETE FROM role_managers
    WHERE owner = $owner_address
    AND role_name = $role_name
    AND manager_wallet IN (SELECT manager_wallet_addr FROM managers_to_delete);
}; 