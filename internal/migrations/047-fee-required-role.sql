/*
    FEE-REQUIRED ROLE BOOTSTRAP

    Creates the `system:fee_required` role used to gate per-tx write-fee
    collection in create_streams / insert_records / insert_taxonomy.

    Phased rollout per truflation/website#3805: only wallets enrolled in
    this role are charged the flat 1 TRUF/tx. Empty role = no caller is
    charged. Wallets are enrolled one at a time as their TRUF funding
    lands. Once every active write wallet is enrolled, the gating
    `IF $fee_required` blocks in 001/003/004 can be deleted in a
    follow-up migration so universal charging resumes.

    Manager: network_writers_manager (same admin who already grants
    write capability — keeps fee-enrollment ops on a privilege the
    network already provisions).
*/

INSERT INTO roles (owner, role_name, display_name, role_type, created_at) VALUES
    ('system', 'fee_required', 'Fee Required', 'system', 0)
ON CONFLICT (owner, role_name) DO NOTHING;

UPDATE roles
SET manager_owner = 'system', manager_role_name = 'network_writers_manager'
WHERE owner = 'system' AND role_name = 'fee_required';
