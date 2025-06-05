/**
 * add_role_manager: Assigns a manager to a role.
 * Only the role owner or the system can assign managers.
 */
CREATE OR REPLACE ACTION add_role_manager(
    $owner_address TEXT,
    $role_name TEXT,
    $manager_wallet TEXT
) PUBLIC {
    $owner_address := LOWER($owner_address);
    $manager_wallet := LOWER($manager_wallet);
    $lower_caller := LOWER(@caller);
    
    -- Validate wallet addresses
    if NOT check_ethereum_address($manager_wallet) {
        ERROR('Invalid manager wallet address: ' || $manager_wallet);
    }
    if $owner_address != 'system' AND NOT check_ethereum_address($owner_address) {
        ERROR('Invalid owner address: ' || $owner_address);
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
    
    -- Prevent owner from being a manager of their own role
    if $manager_wallet = $owner_address {
        ERROR('Role owner cannot be assigned as a manager of their own role');
    }
    
    -- Add the manager, ignoring if they are already a manager
    INSERT INTO role_managers (owner, role_name, manager_wallet, assigned_at, assigned_by)
    VALUES ($owner_address, $role_name, $manager_wallet, @height, $lower_caller)
    ON CONFLICT (owner, role_name, manager_wallet) DO NOTHING;
};

/**
 * remove_role_manager: Removes a manager from a role.
 * Only the role owner or the system can remove managers.
 */
CREATE OR REPLACE ACTION remove_role_manager(
    $owner_address TEXT,
    $role_name TEXT,
    $manager_wallet TEXT
) PUBLIC {
    $owner_address := LOWER($owner_address);
    $manager_wallet := LOWER($manager_wallet);
    $lower_caller := LOWER(@caller);

    -- Validate wallet addresses
    if NOT check_ethereum_address($manager_wallet) {
        ERROR('Invalid manager wallet address: ' || $manager_wallet);
    }
    if $owner_address != 'system' AND NOT check_ethereum_address($owner_address) {
        ERROR('Invalid owner address: ' || $owner_address);
    }

    -- Check permissions: caller must be the role owner or system
    if $lower_caller != $owner_address AND $lower_caller != 'system' {
        ERROR('Only role owner or system can remove managers');
    }

    -- Remove the manager
    DELETE FROM role_managers
    WHERE owner = $owner_address
    AND role_name = $role_name
    AND manager_wallet = $manager_wallet;
}; 