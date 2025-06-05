/*
    ROLES AND ROLE-BASED ACCESS CONTROL (RBAC) TABLES

    This migration introduces tables for a role-based access control system.
    This allows for gating actions and access to resources based on assigned roles.

    Tables:
    - roles: Defines available roles, which can be system-level or user-defined.
    - role_members: Maps wallets to roles, establishing membership.
*/

CREATE TABLE IF NOT EXISTS roles (
    owner TEXT NOT NULL,
    role_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    role_type TEXT NOT NULL,
    created_at INT8 NOT NULL,

    PRIMARY KEY (owner, role_name),

    CHECK (role_type IN ('system', 'user')),
    CHECK (owner = 'system' OR (owner LIKE '0x%' AND LENGTH(owner) = 42)),
    CHECK (LENGTH(role_name) > 0)
);

CREATE INDEX IF NOT EXISTS roles_type_idx ON roles (role_type);

CREATE TABLE IF NOT EXISTS role_members (
    owner TEXT NOT NULL,
    role_name TEXT NOT NULL,
    wallet TEXT NOT NULL,
    granted_at INT8 NOT NULL,
    granted_by TEXT NOT NULL,

    PRIMARY KEY (owner, role_name, wallet),

    FOREIGN KEY (owner, role_name) REFERENCES roles(owner, role_name) ON DELETE CASCADE,

    CHECK (wallet LIKE '0x%' AND LENGTH(wallet) = 42),
    CHECK (granted_by = 'system' OR (granted_by LIKE '0x%' AND LENGTH(granted_by) = 42))
);

CREATE INDEX IF NOT EXISTS role_members_wallet_idx ON role_members (wallet, owner, role_name); 