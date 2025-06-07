/*
    SYSTEM ROLES BOOTSTRAP

    This migration creates the essential system roles required for normal operation.
    These roles gate critical actions like stream creation and should always exist.

    Roles Created:
    - system:network_writer: Required to create streams
    - system:network_writers_manager: Can manage the network_writer role
*/

-- Create the network_writer role (required for stream creation)
INSERT INTO roles (owner, role_name, display_name, role_type, created_at) VALUES
    ('system', 'network_writer', 'Network Writer', 'system', 0)
ON CONFLICT (owner, role_name) DO NOTHING;

-- Create the network_writers_manager role (can manage network_writer role)
INSERT INTO roles (owner, role_name, display_name, role_type, created_at) VALUES
    ('system', 'network_writers_manager', 'Network Writers Manager', 'system', 0)
ON CONFLICT (owner, role_name) DO NOTHING;

-- Set the network_writer role to be managed by the network_writers_manager role
UPDATE roles 
SET manager_owner = 'system', manager_role_name = 'network_writers_manager' 
WHERE owner = 'system' AND role_name = 'network_writer'; 