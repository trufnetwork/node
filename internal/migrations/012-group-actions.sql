-- 1. **New Contract Actions (More or less)**

/**
 * create_group: Creates a new access group with the caller as the owner.
 */
CREATE OR REPLACE ACTION create_group(
    $group_id INT8
) PUBLIC {
    $owner := LOWER(@caller);
    $current_block := @height;

    -- Check if group already exists
    for $row in SELECT 1 FROM access_groups WHERE group_id = $group_id LIMIT 1 {
        ERROR('Group already exists: ' || $group_id);
    }

    INSERT INTO access_groups (group_id, owned_by, created_at)
    VALUES ($group_id, $owner, $current_block);

    -- Add owner as first member
    INSERT INTO access_group_members (group_id, wallet)
    VALUES ($group_id, $owner);
};

/**
 * add_wallet_to_group: Adds a wallet to an access group. Only owner can add.
 */
CREATE OR REPLACE ACTION add_wallet_to_group(
    $group_id INT8,
    $wallet TEXT
) PUBLIC {
    $owner := LOWER(@caller);
    $wallet := LOWER($wallet);

    -- Check if group exists and get owner
    $found := false;
    for $row in SELECT owned_by FROM access_groups WHERE group_id = $group_id LIMIT 1 {
        if $row.owned_by != $owner {
            ERROR('Only group owner can add members');
        }
        $found := true;
    }
    if !$found {
        ERROR('Group does not exist: ' || $group_id);
    }

    -- Check if wallet is already a member
    for $row in SELECT 1 FROM access_group_members WHERE group_id = $group_id AND wallet = $wallet LIMIT 1 {
        ERROR('Wallet is already a member of group: ' || $group_id);
    }

    INSERT INTO access_group_members (group_id, wallet)
    VALUES ($group_id, $wallet);
};

/**
 * remove_wallet_from_group: Removes a wallet from an access group. Only owner can remove.
 */
CREATE OR REPLACE ACTION remove_wallet_from_group(
    $group_id INT8,
    $wallet TEXT
) PUBLIC {
    $owner := LOWER(@caller);
    $wallet := LOWER($wallet);

    -- Check if group exists and get owner
    $found := false;
    for $row in SELECT owned_by FROM access_groups WHERE group_id = $group_id LIMIT 1 {
        if $row.owned_by != $owner {
            ERROR('Only group owner can remove members');
        }
        $found := true;
    }
    if !$found {
        ERROR('Group does not exist: ' || $group_id);
    }

    -- Check if wallet is a member
    $is_member := false;
    for $row in SELECT 1 FROM access_group_members WHERE group_id = $group_id AND wallet = $wallet LIMIT 1 {
        $is_member := true;
    }
    if !$is_member {
        ERROR('Wallet is not a member of group: ' || $group_id);
    }

    DELETE FROM access_group_members WHERE group_id = $group_id AND wallet = $wallet;
};

/**
 * delete_group: Deletes an access group. Only owner can delete.
 */
CREATE OR REPLACE ACTION delete_group(
    $group_id INT8
) PUBLIC {
    $owner := LOWER(@caller);
    $found := false;
    for $row in SELECT owned_by FROM access_groups WHERE group_id = $group_id LIMIT 1 {
        if $row.owned_by != $owner {
            ERROR('Only group owner can delete the group');
        }
        $found := true;
    }
    if !$found {
        ERROR('Group does not exist: ' || $group_id);
    }

    DELETE FROM access_group_members WHERE group_id = $group_id;
    DELETE FROM access_groups WHERE group_id = $group_id;
};

/**
 * is_member_of: Checks if a wallet is a member of a group.
 */
CREATE OR REPLACE ACTION is_member_of(
    $group_id INT8,
    $wallet TEXT
) PUBLIC view returns (is_member BOOL) {
    $wallet := LOWER($wallet);
    for $row in SELECT 1 FROM access_group_members WHERE group_id = $group_id AND wallet = $wallet LIMIT 1 {
        return true;
    }
    return false;
};

/**
 * list_members: Lists members of a group with pagination.
 */
CREATE OR REPLACE ACTION list_members(
    $group_id INT8,
    $offset INT,
    $limit INT
) PUBLIC view returns table(wallet TEXT) {
    if $limit IS NULL {
        $limit := 100;
    }
    if $offset IS NULL {
        $offset := 0;
    }
    RETURN SELECT wallet FROM access_group_members WHERE group_id = $group_id ORDER BY wallet ASC LIMIT $limit OFFSET $offset;
};