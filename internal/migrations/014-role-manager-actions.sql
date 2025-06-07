/**
 * set_role_manager: Sets a manager role for a target role.
 * 
 * Permissions:
 * - Only the role owner can execute this action.
 *
 * System roles that need management changes usually bypass this by using direct SQL,
 * as the system owner cannot sign transactions.
 */
CREATE OR REPLACE ACTION set_role_manager(
    $owner TEXT,
    $role_name TEXT,
    $manager_owner TEXT, -- owner of the manager role, or NULL to remove
    $manager_role_name TEXT -- name of the manager role, or NULL to remove
) PUBLIC {
    $owner := LOWER($owner);
    $role_name := LOWER($role_name);
    
    -- Assert that target role exists and caller is its owner
    helper_assert_owner_addr($owner);
    helper_assert_role_exists($owner, $role_name);
    helper_assert_is_role_owner($owner, $role_name);

    -- If a manager role is being set, sanitize and validate its components
    IF $manager_owner IS NOT NULL AND $manager_role_name IS NOT NULL {
        $manager_owner := LOWER($manager_owner);
        $manager_role_name := LOWER($manager_role_name);
        helper_assert_owner_addr($manager_owner);
    } ELSE {
        -- If one is null, both must be null to clear the manager
        IF $manager_owner IS NOT NULL OR $manager_role_name IS NOT NULL {
            ERROR('To set a manager role, both manager_owner and manager_role_name must be provided. To remove a manager, both must be NULL.');
        }
        -- Ensure they are explicitly NULL for the UPDATE
        $manager_owner := NULL;
        $manager_role_name := NULL;
    }

    -- Update the manager role. The FK constraint will validate that the manager role exists.
    UPDATE roles 
    SET manager_owner = $manager_owner,
        manager_role_name = $manager_role_name
    WHERE owner = $owner AND role_name = $role_name;
};
