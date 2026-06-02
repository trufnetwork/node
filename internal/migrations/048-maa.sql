/*
 * MIGRATION 048: MODULAR AGENT ADDRESSES (MAA)
 *
 * Rule store + append-only audit trail for fundable "agent wallets".
 * Node-side SQL only — same blast radius as 031-order-book-vault.sql. No consensus change.
 *
 * Addresses are stored as 20-byte BYTEA internally and exchanged as 0x-prefixed hex TEXT
 * at the API boundary. The MAA address and rules_hash are derived by the pure tn_utils
 * precompiles (derive_maa_address / compute_rules_hash); the exact byte layout is frozen in
 * 0GoalModularAgentAddresses/5RulesHash-Preimage-Spec.md and is shared with the SDKs.
 *
 * The rule (fee + allow-list) is set ONCE at maa_create and is IMMUTABLE thereafter, per the
 * spec ("the portion ... determined at the time of rule creation"; agents act on "preset rules").
 * rules_hash commits to exactly those terms and defines the permanent address. There are NO
 * setters — to change a rule, create a new MAA. The owner controls funds by withdrawing (a
 * later issue), not by editing the rule.
 *
 * Bridge-agnostic: NO mainnet .prod.sql twin — this file calls no bridge precompile.
 */

-- =============================================================================
-- maa_rules: one row per agent wallet (composite identity + fee config)
-- =============================================================================
CREATE TABLE IF NOT EXISTS maa_rules (
    maa_address       BYTEA PRIMARY KEY,        -- composite identity; funds credited here; route rewrites @caller to this
    rule_address      BYTEA NOT NULL,           -- spec "Rule Address" (== maa_address; kept for spec fidelity)
    restricted_addr   BYTEA NOT NULL,           -- agent: creates the rule, allow-list bound
    unrestricted_addr BYTEA NOT NULL,           -- owner/funder: full custody + withdraw
    rules_hash        BYTEA NOT NULL,           -- 32-byte creation-time commitment over fee + allow-list
    bridge            TEXT  NOT NULL,           -- 'eth_truf' | 'eth_usdc' (pins decimal scale)
    token             TEXT  NOT NULL,           -- 'TRUF' | 'USDC' (display; derived from bridge)
    fee_mode          TEXT  NOT NULL,           -- 'bps' | 'flat'
    fee_bps           INT   NOT NULL DEFAULT 0,            -- 0..10000 (0..100%); policy cap (if any) is enforced separately
    fee_flat          NUMERIC(78, 0) NOT NULL DEFAULT 0,  -- base units of `bridge`
    enabled           BOOLEAN NOT NULL DEFAULT true,       -- reserved; immutable in v1 (no revoke); always true
    created_at        INT8  NOT NULL,                       -- @height at creation

    CONSTRAINT chk_maa_rules_fee_bps  CHECK (fee_bps >= 0 AND fee_bps <= 10000),
    CONSTRAINT chk_maa_rules_fee_flat CHECK (fee_flat >= 0),
    CONSTRAINT chk_maa_rules_fee_mode CHECK (fee_mode = 'bps' OR fee_mode = 'flat')
);

CREATE INDEX IF NOT EXISTS idx_maa_rules_unrestricted ON maa_rules(unrestricted_addr);
CREATE INDEX IF NOT EXISTS idx_maa_rules_restricted   ON maa_rules(restricted_addr);

-- =============================================================================
-- maa_allowed_actions: action-name references, child of maa_rules
-- =============================================================================
CREATE TABLE IF NOT EXISTS maa_allowed_actions (
    maa_address BYTEA NOT NULL,
    namespace   TEXT  NOT NULL,                 -- e.g. 'main'
    action      TEXT  NOT NULL,                 -- allow-listed action name
    body_hash   BYTEA,                           -- optional action body-hash pin (MB8); NULL = unpinned

    PRIMARY KEY (maa_address, namespace, action),
    FOREIGN KEY (maa_address) REFERENCES maa_rules(maa_address) ON DELETE CASCADE
);

-- =============================================================================
-- maa_events: append-only audit log (permanent — NOT trimmed)
-- =============================================================================
CREATE TABLE IF NOT EXISTS maa_events (
    id              INT8 PRIMARY KEY,            -- MAX(id)+1; safe under Kwil sequential block exec
    maa_address     BYTEA NOT NULL,
    event_type      TEXT  NOT NULL,              -- 'CREATE' (FUND/EXEC/WITHDRAW added in later issues)
    actor_role      TEXT  NOT NULL,              -- 'restricted' | 'unrestricted'
    actor_addr      BYTEA NOT NULL,
    inner_namespace TEXT,                         -- nullable until exec events (later issue)
    inner_action    TEXT,
    amount          NUMERIC(78, 0),              -- nullable; populated on fee/withdraw events (later issue)
    tx_hash         BYTEA NOT NULL,
    block_height    INT8  NOT NULL,
    block_timestamp INT8  NOT NULL,

    -- RESTRICT (not CASCADE): the audit log is permanent — deleting a rule must never erase its history.
    FOREIGN KEY (maa_address) REFERENCES maa_rules(maa_address) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_maa_events_addr ON maa_events(maa_address);

-- =============================================================================
-- maa_record_event: append one audit row (private helper)
-- =============================================================================
CREATE OR REPLACE ACTION maa_record_event(
    $maa_address     BYTEA,
    $event_type      TEXT,
    $actor_role      TEXT,
    $actor_addr      BYTEA,
    $inner_namespace TEXT,
    $inner_action    TEXT,
    $amount          NUMERIC(78, 0)
) PRIVATE {
    -- MAX(id)+1 is safe in Kwil (sequential block execution) — see 044-order-book-events.sql.
    $next_id INT8;
    for $row in SELECT COALESCE(MAX(id), 0::INT8) + 1 AS val FROM maa_events {
        $next_id := $row.val;
    }

    INSERT INTO maa_events (
        id, maa_address, event_type, actor_role, actor_addr,
        inner_namespace, inner_action, amount, tx_hash, block_height, block_timestamp
    ) VALUES (
        $next_id, $maa_address, $event_type, $actor_role, $actor_addr,
        $inner_namespace, $inner_action, $amount, decode(@txid, 'hex'), @height, @block_timestamp
    );
};

-- =============================================================================
-- maa_create: the RESTRICTED key signs (lifecycle step 1). The rule is set ONCE here and is
-- IMMUTABLE thereafter (spec: fee "determined at the time of rule creation"; agents act on
-- "preset rules"). There are no setters — to change a rule, create a new MAA.
-- =============================================================================
CREATE OR REPLACE ACTION maa_create(
    $unrestricted_addr TEXT,       -- owner (0x-hex); the restricted signer is @caller
    $salt              BYTEA,      -- enables several MAAs per {restricted,unrestricted} pair (may be NULL)
    $bridge            TEXT,       -- 'eth_truf' | 'eth_usdc'; token is DERIVED from this (not a parameter)
    $fee_mode          TEXT,
    $fee_bps           INT,
    $fee_flat          NUMERIC(78, 0),
    $namespaces        TEXT[],     -- parallel arrays for the allow-list
    $actions           TEXT[],
    $body_hashes       BYTEA[]
) PUBLIC RETURNS (maa_address BYTEA) {
    -- Restricted signer = @caller (design §6 flow 1; "whoever signs becomes the restricted party").
    $restricted_bytes BYTEA := tn_utils.get_caller_bytes();

    -- Validate + decode the owner address (0x-hex -> 20-byte BYTEA).
    if $unrestricted_addr IS NULL OR length($unrestricted_addr) != 42
       OR substring(LOWER($unrestricted_addr), 1, 2) != '0x' {
        ERROR('unrestricted_addr must be a 0x-prefixed 40-hex address');
    }
    $unrestricted_bytes BYTEA := decode(substring(LOWER($unrestricted_addr), 3, 40), 'hex');

    if $restricted_bytes = $unrestricted_bytes {
        ERROR('restricted and unrestricted address must differ');
    }
    if $fee_mode != 'bps' AND $fee_mode != 'flat' {
        ERROR('fee_mode must be bps or flat');
    }
    if $fee_bps < 0 OR $fee_bps > 10000 {
        ERROR('fee_bps must be between 0 and 10000 (0..100%)');
    }

    -- token is DERIVED from bridge (spec 5RulesHash-Preimage-Spec.md §6.3: "bridge is committed;
    -- token is not — token is derivable from bridge"). Never trust a caller-supplied token, and
    -- reject any bridge we can't price (decimal scale + display token are bridge-specific).
    $token TEXT;
    if $bridge = 'eth_truf' {
        $token := 'TRUF';
    } elseif $bridge = 'eth_usdc' {
        $token := 'USDC';
    } else {
        ERROR('unsupported bridge (expected eth_truf or eth_usdc)');
    }

    -- Parallel allow-list arrays must be equal length (NULL/empty arrays are allowed and equal).
    -- Parens are required: IS DISTINCT FROM binds looser than OR (see 003-primitive-insertion.sql).
    $n INT := array_length($namespaces);
    if ($n IS DISTINCT FROM array_length($actions)) OR ($n IS DISTINCT FROM array_length($body_hashes)) {
        ERROR('namespaces, actions and body_hashes must be the same length');
    }

    -- Reject duplicate (namespace, action) pairs. compute_rules_hash canonicalizes duplicates
    -- (spec §1: dedup, last-write-wins on body_hash), but maa_allowed_actions has a
    -- (maa_address, namespace, action) PRIMARY KEY, so a raw duplicate would PK-violate the insert
    -- below. Fail closed here with a clear message and keep the stored allow-list 1:1 with the
    -- hashed rule set (no silently-dropped body_hash pin).
    $has_dup BOOL := false;
    for $d in
        SELECT 1 AS one
        FROM UNNEST($namespaces, $actions) AS u(ns, act)
        GROUP BY u.ns, u.act
        HAVING COUNT(*) > 1
        LIMIT 1
    {
        $has_dup := true;
    }
    if $has_dup {
        ERROR('duplicate (namespace, action) in allow-list; each pair may appear at most once');
    }

    -- Commitment computed ON-CHAIN from the rule terms (never trusted from a parameter).
    $rules_hash BYTEA := tn_utils.compute_rules_hash(
        $fee_mode, $fee_bps, $fee_flat::text, $bridge, $namespaces, $actions, $body_hashes
    );

    -- Deterministic address. rule_address == maa_address.
    $maa_address BYTEA := tn_utils.derive_maa_address(
        $restricted_bytes, $unrestricted_bytes, $rules_hash, $salt
    );

    -- Reject a duplicate identity.
    $exists BOOL := false;
    for $row in SELECT 1 AS one FROM maa_rules WHERE maa_address = $maa_address {
        $exists := true;
    }
    if $exists {
        ERROR('an MAA already exists for this {restricted, unrestricted, rules, salt}');
    }

    INSERT INTO maa_rules (
        maa_address, rule_address, restricted_addr, unrestricted_addr, rules_hash,
        bridge, token, fee_mode, fee_bps, fee_flat, enabled, created_at
    ) VALUES (
        $maa_address, $maa_address, $restricted_bytes, $unrestricted_bytes, $rules_hash,
        $bridge, $token, $fee_mode, $fee_bps, $fee_flat, true, @height
    );

    -- Allow-list: batch insert via parallel-array UNNEST (precedent: 033-order-book-settlement.sql:42).
    INSERT INTO maa_allowed_actions (maa_address, namespace, action, body_hash)
    SELECT $maa_address, t.ns, t.act, t.bh
    FROM UNNEST($namespaces, $actions, $body_hashes) AS t(ns, act, bh);

    maa_record_event($maa_address, 'CREATE', 'restricted', $restricted_bytes, NULL, NULL, NULL);

    RETURN $maa_address;
};

-- =============================================================================
-- Public getters (audit surface)
-- =============================================================================
CREATE OR REPLACE ACTION maa_get_rule($maa_address BYTEA)
PUBLIC VIEW RETURNS TABLE(
    maa_address TEXT,
    rule_address TEXT,
    restricted_addr TEXT,
    unrestricted_addr TEXT,
    rules_hash TEXT,
    bridge TEXT,
    token TEXT,
    fee_mode TEXT,
    fee_bps INT,
    fee_flat NUMERIC(78, 0),
    enabled BOOL,
    created_at INT8
) {
    for $r in
        SELECT
            '0x' || encode(maa_address, 'hex')       AS maa_a,
            '0x' || encode(rule_address, 'hex')      AS rule_a,
            '0x' || encode(restricted_addr, 'hex')   AS restr_a,
            '0x' || encode(unrestricted_addr, 'hex') AS unrestr_a,
            '0x' || encode(rules_hash, 'hex')        AS rh,
            bridge, token, fee_mode, fee_bps, fee_flat, enabled, created_at
        FROM maa_rules
        WHERE maa_address = $maa_address
    {
        RETURN NEXT $r.maa_a, $r.rule_a, $r.restr_a, $r.unrestr_a, $r.rh,
                    $r.bridge, $r.token, $r.fee_mode, $r.fee_bps, $r.fee_flat, $r.enabled, $r.created_at;
    }
};

CREATE OR REPLACE ACTION maa_get_allowed_actions($maa_address BYTEA)
PUBLIC VIEW RETURNS TABLE(
    namespace TEXT,
    action TEXT,
    body_hash TEXT
) {
    for $r in
        SELECT
            namespace,
            action,
            CASE WHEN body_hash IS NULL THEN NULL ELSE '0x' || encode(body_hash, 'hex') END AS bh
        FROM maa_allowed_actions
        WHERE maa_address = $maa_address
        ORDER BY namespace ASC, action ASC
    {
        RETURN NEXT $r.namespace, $r.action, $r.bh;
    }
};

CREATE OR REPLACE ACTION maa_list_by_unrestricted($owner TEXT, $limit INT, $offset INT)
PUBLIC VIEW RETURNS TABLE(
    maa_address TEXT,
    enabled BOOL,
    created_at INT8
) {
    if $limit IS NULL OR $limit <= 0 { $limit := 100; }
    if $offset IS NULL OR $offset < 0 { $offset := 0; }
    $owner_bytes BYTEA := decode(substring(LOWER($owner), 3, 40), 'hex');

    for $r in
        SELECT '0x' || encode(maa_address, 'hex') AS a, enabled, created_at
        FROM maa_rules
        WHERE unrestricted_addr = $owner_bytes
        ORDER BY created_at ASC, maa_address ASC
        LIMIT $limit OFFSET $offset
    {
        RETURN NEXT $r.a, $r.enabled, $r.created_at;
    }
};

CREATE OR REPLACE ACTION maa_list_by_restricted($agent TEXT, $limit INT, $offset INT)
PUBLIC VIEW RETURNS TABLE(
    maa_address TEXT,
    enabled BOOL,
    created_at INT8
) {
    if $limit IS NULL OR $limit <= 0 { $limit := 100; }
    if $offset IS NULL OR $offset < 0 { $offset := 0; }
    $agent_bytes BYTEA := decode(substring(LOWER($agent), 3, 40), 'hex');

    for $r in
        SELECT '0x' || encode(maa_address, 'hex') AS a, enabled, created_at
        FROM maa_rules
        WHERE restricted_addr = $agent_bytes
        ORDER BY created_at ASC, maa_address ASC
        LIMIT $limit OFFSET $offset
    {
        RETURN NEXT $r.a, $r.enabled, $r.created_at;
    }
};

CREATE OR REPLACE ACTION maa_get_events($maa_address BYTEA, $limit INT, $offset INT)
PUBLIC VIEW RETURNS TABLE(
    id INT8,
    event_type TEXT,
    actor_role TEXT,
    actor_addr TEXT,
    inner_namespace TEXT,
    inner_action TEXT,
    amount NUMERIC(78, 0),
    tx_hash TEXT,
    block_height INT8,
    block_timestamp INT8
) {
    if $limit IS NULL OR $limit <= 0 { $limit := 100; }
    if $offset IS NULL OR $offset < 0 { $offset := 0; }

    for $r in
        SELECT
            id, event_type, actor_role,
            '0x' || encode(actor_addr, 'hex') AS actor_a,
            inner_namespace, inner_action, amount,
            '0x' || encode(tx_hash, 'hex') AS txh,
            block_height, block_timestamp
        FROM maa_events
        WHERE maa_address = $maa_address
        ORDER BY id ASC
        LIMIT $limit OFFSET $offset
    {
        RETURN NEXT $r.id, $r.event_type, $r.actor_role, $r.actor_a,
                    $r.inner_namespace, $r.inner_action, $r.amount, $r.txh,
                    $r.block_height, $r.block_timestamp;
    }
};

CREATE OR REPLACE ACTION maa_is_known($maa_address BYTEA)
PUBLIC VIEW RETURNS (known BOOL) {
    for $r in SELECT 1 AS one FROM maa_rules WHERE maa_address = $maa_address {
        RETURN true;
    }
    RETURN false;
};
