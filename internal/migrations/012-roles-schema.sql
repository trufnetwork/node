/*
    ROLES AND ROLE-BASED ACCESS CONTROL (RBAC) TABLES

    This migration introduces tables for a role-based access control system.
    This allows for gating actions and access to resources based on assigned roles.

    Tables:
    - roles: Defines available roles, which can be system-level or user-defined.
    - role_members: Maps wallets to roles, establishing membership.
*/

CREATE TABLE IF NOT EXISTS roles (
    owner TEXT NOT NULL, -- owner of the role
    role_name TEXT NOT NULL, -- unique by owner name of the role
    display_name TEXT NOT NULL, -- human-readable name for the role
    role_type TEXT NOT NULL, -- system or user
    created_at INT8 NOT NULL, -- height at which the role was created

    PRIMARY KEY (owner, role_name), -- owner and role_name must be unique

    CHECK (role_type IN ('system', 'user')),
    CHECK (owner = 'system' OR (owner LIKE '0x%' AND LENGTH(owner) = 42)),
    CHECK (LENGTH(role_name) > 0)
);

CREATE INDEX IF NOT EXISTS roles_type_idx ON roles (role_type);

CREATE TABLE IF NOT EXISTS role_members (
    owner TEXT NOT NULL, -- owner of the role
    role_name TEXT NOT NULL, -- name of the role
    wallet TEXT NOT NULL, -- wallet address of the member
    granted_at INT8 NOT NULL, -- height at which the member was granted the role
    granted_by TEXT NOT NULL, -- wallet address of the grantor

    PRIMARY KEY (owner, role_name, wallet),

    FOREIGN KEY (owner, role_name) REFERENCES roles(owner, role_name) ON DELETE CASCADE,

    CHECK (wallet LIKE '0x%' AND LENGTH(wallet) = 42),
    CHECK (granted_by = 'system' OR (granted_by LIKE '0x%' AND LENGTH(granted_by) = 42))
);

CREATE INDEX IF NOT EXISTS role_members_wallet_idx ON role_members (wallet, owner, role_name);

CREATE TABLE IF NOT EXISTS role_managers (
    owner TEXT NOT NULL, -- owner of the role
    role_name TEXT NOT NULL, -- name of the role
    manager_wallet TEXT NOT NULL, -- wallet address of the manager
    assigned_at INT8 NOT NULL, -- height at which the manager was assigned
    assigned_by TEXT NOT NULL, -- wallet address of the assigner (must be owner)

    PRIMARY KEY (owner, role_name, manager_wallet),

    FOREIGN KEY (owner, role_name) REFERENCES roles(owner, role_name) ON DELETE CASCADE,

    CHECK (manager_wallet LIKE '0x%' AND LENGTH(manager_wallet) = 42),
    CHECK (assigned_by = 'system' OR (assigned_by LIKE '0x%' AND LENGTH(assigned_by) = 42))
);

CREATE INDEX IF NOT EXISTS role_managers_wallet_idx ON role_managers (manager_wallet, owner, role_name); 
