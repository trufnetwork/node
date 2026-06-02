/*
 * MIGRATION 048: MODULAR AGENT ADDRESSES (MAA)
 *
 * Rule store + MAA-instance lookup + append-only audit for fundable "agent wallets".
 * Node-side SQL only — same blast radius as 031-order-book-vault.sql. No consensus change.
 *
 * TWO addresses (spec 0MainGoal.md): a RULE_ID identifies a rule set; a funder JOINS it to get a
 * distinct MAA. rule_id is a 32-byte CONTENT-HASH IDENTIFIER (not an ETH address — never funded
 * directly); maa_address is a real 20-byte ETH address that holds funds. Both are derived by the pure
 * tn_utils precompiles (compute_rules_hash / derive_rule_id / derive_maa_address); the exact byte layout
 * is frozen in 0GoalModularAgentAddresses/2MAA-Plan.md §5 and is shared with the SDKs.
 *
 * Token-agnostic (Vin §0.4): the rule pins NO bridge/token — the wallet accepts all bridged tokens, so
 * this file calls no bridge precompile (NO mainnet .prod.sql twin).
 *
 * The rule (fee + allow-list) is set ONCE at maa_create_rule and is IMMUTABLE thereafter (Vin §0.5).
 * rules_hash commits to exactly those terms and (via rule_id) defines the addresses. There are NO
 * setters — to change a rule, create a new one. The owner controls funds by withdrawing (a later issue),
 * not by editing the rule.
 *
 * Two write actions, BOTH fund-free:
 *   maa_create_rule  -- the RESTRICTED creator registers a rule, returns rule_id.
 *   maa_join         -- a funder (UNRESTRICTED) joins a rule_id, returns the derived MAA address.
 * Funding is a separate transfer to the MAA address (a later issue).
 */

-- =============================================================================
-- maa_rules: one row per RULE. Keyed by rule_id (a 32-byte identifier).
-- Reusable by unlimited funders -> many MAAs.
-- =============================================================================
CREATE TABLE IF NOT EXISTS maa_rules (
    rule_id         BYTEA PRIMARY KEY,        -- 32-byte id: keccak(version‖restricted‖rules_hash‖salt) (untruncated)
    restricted_addr BYTEA NOT NULL,           -- agent: creates the rule, allow-list bound
    rules_hash      BYTEA NOT NULL,           -- 32-byte creation-time commitment over fee + allow-list
    salt            BYTEA,                     -- rule nonce: lets one creator have several distinct rules (may be NULL)
    fee_mode        TEXT  NOT NULL,           -- 'bps' | 'flat'
    fee_bps         INT   NOT NULL DEFAULT 0,            -- 0..10000 (0..100%; 10000 = 100%). No policy cap (Vin §0.3).
    fee_flat        NUMERIC(78, 0) NOT NULL DEFAULT 0,  -- base units; denomination resolved at withdrawal
    created_at      INT8  NOT NULL,                       -- @height at creation

    CONSTRAINT chk_maa_rules_fee_bps  CHECK (fee_bps >= 0 AND fee_bps <= 10000),
    CONSTRAINT chk_maa_rules_fee_flat CHECK (fee_flat >= 0),
    CONSTRAINT chk_maa_rules_fee_mode CHECK (fee_mode = 'bps' OR fee_mode = 'flat')
);

CREATE INDEX IF NOT EXISTS idx_maa_rules_restricted ON maa_rules(restricted_addr);

-- =============================================================================
-- maa_allowed_actions: action-name references, child of maa_rules (by rule_id).
-- =============================================================================
CREATE TABLE IF NOT EXISTS maa_allowed_actions (
    rule_id   BYTEA NOT NULL,
    namespace TEXT  NOT NULL,                  -- e.g. 'main'
    action    TEXT  NOT NULL,                  -- allow-listed action name
    body_hash BYTEA,                            -- optional action body-hash pin (MB8); NULL = unpinned

    PRIMARY KEY (rule_id, namespace, action),
    FOREIGN KEY (rule_id) REFERENCES maa_rules(rule_id) ON DELETE CASCADE
);

-- =============================================================================
-- maa_instances: one row per joined MAA. Created by maa_join (fund-free).
-- THE lookup table: maa_address -> {rule_id, restricted (via rule), unrestricted}.
-- =============================================================================
CREATE TABLE IF NOT EXISTS maa_instances (
    maa_address       BYTEA PRIMARY KEY,       -- 20-byte ETH addr: keccak(version‖unrestricted‖restricted‖rule_id)[12:]
    rule_id           BYTEA NOT NULL,          -- FK to maa_rules
    unrestricted_addr BYTEA NOT NULL,          -- owner / funder (bound at maa_join)
    created_at        INT8  NOT NULL,

    FOREIGN KEY (rule_id) REFERENCES maa_rules(rule_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_maa_inst_unrestricted ON maa_instances(unrestricted_addr);
CREATE INDEX IF NOT EXISTS idx_maa_inst_rule         ON maa_instances(rule_id);

-- =============================================================================
-- maa_events: append-only audit log (permanent — NOT trimmed).
-- =============================================================================
CREATE TABLE IF NOT EXISTS maa_events (
    id              INT8 PRIMARY KEY,            -- MAX(id)+1; safe under Kwil sequential block exec
    rule_id         BYTEA NOT NULL,
    maa_address     BYTEA,                       -- nullable: rule-level events (CREATE_RULE) have no MAA
    event_type      TEXT  NOT NULL,              -- 'CREATE_RULE' | 'JOIN' (FUND/EXEC/WITHDRAW in later issues)
    actor_role      TEXT  NOT NULL,              -- 'restricted' | 'unrestricted'
    actor_addr      BYTEA NOT NULL,
    inner_namespace TEXT,                         -- nullable until exec events (later issue)
    inner_action    TEXT,
    amount          NUMERIC(78, 0),              -- nullable; populated on fee/withdraw events (later issue)
    tx_hash         BYTEA NOT NULL,
    block_height    INT8  NOT NULL,
    block_timestamp INT8  NOT NULL,

    -- RESTRICT (not CASCADE): the audit log is permanent — deleting a rule must never erase its history.
    FOREIGN KEY (rule_id) REFERENCES maa_rules(rule_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_maa_events_rule ON maa_events(rule_id);
CREATE INDEX IF NOT EXISTS idx_maa_events_maa  ON maa_events(maa_address);

-- =============================================================================
-- maa_record_event: append one audit row (private helper)
-- =============================================================================
CREATE OR REPLACE ACTION maa_record_event(
    $rule_id         BYTEA,
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
        id, rule_id, maa_address, event_type, actor_role, actor_addr,
        inner_namespace, inner_action, amount, tx_hash, block_height, block_timestamp
    ) VALUES (
        $next_id, $rule_id, $maa_address, $event_type, $actor_role, $actor_addr,
        $inner_namespace, $inner_action, $amount, decode(@txid, 'hex'), @height, @block_timestamp
    );
};

-- =============================================================================
-- maa_create_rule: the RESTRICTED key signs (lifecycle step 1). The rule is set ONCE here and is
-- IMMUTABLE thereafter (Vin §0.5). No setters. The unrestricted owner is NOT a parameter — it is bound
-- later, at maa_join. FUND-FREE.
-- =============================================================================
CREATE OR REPLACE ACTION maa_create_rule(
    $salt        BYTEA,      -- rule nonce (enables several distinct rules per creator; may be NULL)
    $fee_mode    TEXT,
    $fee_bps     INT,
    $fee_flat    NUMERIC(78, 0),
    $namespaces  TEXT[],     -- parallel arrays for the allow-list
    $actions     TEXT[],
    $body_hashes BYTEA[]
) PUBLIC RETURNS (rule_id BYTEA) {
    -- Creator = @caller = restricted (design flow 1; "whoever signs becomes the restricted party").
    $restricted_bytes BYTEA := tn_utils.get_caller_bytes();

    if $fee_mode != 'bps' AND $fee_mode != 'flat' {
        ERROR('fee_mode must be bps or flat');
    }
    if $fee_bps < 0 OR $fee_bps > 10000 {
        ERROR('fee_bps must be between 0 and 10000 (10000 = 100%)');
    }
    -- Friendly action-level guard mirroring chk_maa_rules_fee_flat (a raw CHECK violation is opaque).
    if $fee_flat < 0::NUMERIC(78, 0) {
        ERROR('fee_flat must be >= 0');
    }

    -- Parallel allow-list arrays must be equal length (NULL/empty arrays are allowed and equal).
    -- Parens are required: IS DISTINCT FROM binds looser than OR (see 003-primitive-insertion.sql).
    $n INT := array_length($namespaces);
    if ($n IS DISTINCT FROM array_length($actions)) OR ($n IS DISTINCT FROM array_length($body_hashes)) {
        ERROR('namespaces, actions and body_hashes must be the same length');
    }

    -- Reject duplicate (namespace, action) pairs. compute_rules_hash canonicalizes duplicates (plan §5.1:
    -- dedup, last-write-wins on body_hash), but maa_allowed_actions has a (rule_id, namespace, action)
    -- PRIMARY KEY, so a raw duplicate would PK-violate the insert below. Fail closed here with a clear
    -- message and keep the stored allow-list 1:1 with the hashed rule set.
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

    -- Commitment computed ON-CHAIN from the rule terms (never trusted from a parameter). NO bridge param.
    $rules_hash BYTEA := tn_utils.compute_rules_hash(
        $fee_mode, $fee_bps, $fee_flat::text, $namespaces, $actions, $body_hashes
    );

    -- rule_id is the 32-byte content-hash IDENTIFIER (untruncated).
    $rule_id BYTEA := tn_utils.derive_rule_id($restricted_bytes, $rules_hash, $salt);

    -- Reject a duplicate rule identity.
    $exists BOOL := false;
    for $row in SELECT 1 AS one FROM maa_rules WHERE rule_id = $rule_id {
        $exists := true;
    }
    if $exists {
        ERROR('a rule already exists for this {restricted, rules, salt}');
    }

    INSERT INTO maa_rules (
        rule_id, restricted_addr, rules_hash, salt, fee_mode, fee_bps, fee_flat, created_at
    ) VALUES (
        $rule_id, $restricted_bytes, $rules_hash, $salt, $fee_mode, $fee_bps, $fee_flat, @height
    );

    -- Allow-list: batch insert via parallel-array UNNEST (precedent: 033-order-book-settlement.sql:42).
    INSERT INTO maa_allowed_actions (rule_id, namespace, action, body_hash)
    SELECT $rule_id, t.ns, t.act, t.bh
    FROM UNNEST($namespaces, $actions, $body_hashes) AS t(ns, act, bh);

    maa_record_event($rule_id, NULL, 'CREATE_RULE', 'restricted', $restricted_bytes, NULL, NULL, NULL);

    RETURN $rule_id;
};

-- =============================================================================
-- maa_join: a funder (the UNRESTRICTED key) joins a rule_id (lifecycle step 2). Binds the caller as the
-- unrestricted owner, derives the deterministic MAA address, and records the instance. FUND-FREE — funding
-- is a separate transfer to the returned MAA address (a later issue). SDK name: joinAgentAddress(ruleId).
-- =============================================================================
CREATE OR REPLACE ACTION maa_join($rule_id BYTEA) PUBLIC RETURNS (maa_address BYTEA) {
    $unrestricted_bytes BYTEA := tn_utils.get_caller_bytes();

    -- Look up the rule's restricted creator (also asserts the rule_id exists).
    $restricted_bytes BYTEA;
    $found BOOL := false;
    for $r in SELECT restricted_addr FROM maa_rules WHERE rule_id = $rule_id {
        $restricted_bytes := $r.restricted_addr;
        $found := true;
    }
    if !$found {
        ERROR('unknown rule_id');
    }
    if $unrestricted_bytes = $restricted_bytes {
        ERROR('unrestricted must differ from the rule creator');
    }

    -- Derive the real 20-byte MAA address from the composite (unrestricted, restricted, rule_id).
    $maa_address BYTEA := tn_utils.derive_maa_address($unrestricted_bytes, $restricted_bytes, $rule_id);

    -- Reject a duplicate join (this funder already has an MAA for this rule).
    $exists BOOL := false;
    for $row in SELECT 1 AS one FROM maa_instances WHERE maa_address = $maa_address {
        $exists := true;
    }
    if $exists {
        ERROR('this funder has already joined this rule (MAA exists)');
    }

    INSERT INTO maa_instances (maa_address, rule_id, unrestricted_addr, created_at)
    VALUES ($maa_address, $rule_id, $unrestricted_bytes, @height);

    maa_record_event($rule_id, $maa_address, 'JOIN', 'unrestricted', $unrestricted_bytes, NULL, NULL, NULL);

    RETURN $maa_address;
};

-- =============================================================================
-- Public getters (audit / transparency surface)
-- =============================================================================
CREATE OR REPLACE ACTION maa_get_rule($rule_id BYTEA)
PUBLIC VIEW RETURNS TABLE(
    rule_id TEXT,
    restricted_addr TEXT,
    rules_hash TEXT,
    fee_mode TEXT,
    fee_bps INT,
    fee_flat NUMERIC(78, 0),
    created_at INT8
) {
    for $r in
        SELECT
            '0x' || encode(rule_id, 'hex')         AS rid,
            '0x' || encode(restricted_addr, 'hex') AS restr_a,
            '0x' || encode(rules_hash, 'hex')      AS rh,
            fee_mode, fee_bps, fee_flat, created_at
        FROM maa_rules
        WHERE rule_id = $rule_id
    {
        RETURN NEXT $r.rid, $r.restr_a, $r.rh, $r.fee_mode, $r.fee_bps, $r.fee_flat, $r.created_at;
    }
};

CREATE OR REPLACE ACTION maa_get_allowed_actions($rule_id BYTEA)
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
        WHERE rule_id = $rule_id
        ORDER BY namespace ASC, action ASC
    {
        RETURN NEXT $r.namespace, $r.action, $r.bh;
    }
};

-- maa_get_instance: the primary explorer/wallet lookup — MAA address -> both keys + rule set (via rule_id).
CREATE OR REPLACE ACTION maa_get_instance($maa_address BYTEA)
PUBLIC VIEW RETURNS TABLE(
    maa_address TEXT,
    rule_id TEXT,
    restricted_addr TEXT,
    unrestricted_addr TEXT,
    created_at INT8
) {
    for $r in
        SELECT
            '0x' || encode(i.maa_address, 'hex')       AS maa_a,
            '0x' || encode(i.rule_id, 'hex')           AS rid,
            '0x' || encode(r.restricted_addr, 'hex')   AS restr_a,
            '0x' || encode(i.unrestricted_addr, 'hex') AS unrestr_a,
            i.created_at AS ca
        FROM maa_instances i
        JOIN maa_rules r ON r.rule_id = i.rule_id
        WHERE i.maa_address = $maa_address
    {
        RETURN NEXT $r.maa_a, $r.rid, $r.restr_a, $r.unrestr_a, $r.ca;
    }
};

CREATE OR REPLACE ACTION maa_list_by_restricted($agent TEXT, $limit INT, $offset INT)
PUBLIC VIEW RETURNS TABLE(
    rule_id TEXT,
    created_at INT8
) {
    if $limit IS NULL OR $limit <= 0 { $limit := 100; }
    if $offset IS NULL OR $offset < 0 { $offset := 0; }
    -- Normalize $agent: accept with/without 0x prefix; require 40 hex chars (decode rejects non-hex digits).
    $agent_hex TEXT := LOWER($agent);
    if substring($agent_hex, 1, 2) = '0x' { $agent_hex := substring($agent_hex, 3, length($agent_hex)); }
    if length($agent_hex) != 40 { ERROR('agent must be a 20-byte hex address (40 hex chars, optional 0x prefix)'); }
    $agent_bytes BYTEA := decode($agent_hex, 'hex');

    for $r in
        SELECT '0x' || encode(rule_id, 'hex') AS rid, created_at
        FROM maa_rules
        WHERE restricted_addr = $agent_bytes
        ORDER BY created_at ASC, rule_id ASC
        LIMIT $limit OFFSET $offset
    {
        RETURN NEXT $r.rid, $r.created_at;
    }
};

CREATE OR REPLACE ACTION maa_list_by_unrestricted($owner TEXT, $limit INT, $offset INT)
PUBLIC VIEW RETURNS TABLE(
    maa_address TEXT,
    rule_id TEXT,
    created_at INT8
) {
    if $limit IS NULL OR $limit <= 0 { $limit := 100; }
    if $offset IS NULL OR $offset < 0 { $offset := 0; }
    -- Normalize $owner: accept with/without 0x prefix; require 40 hex chars (decode rejects non-hex digits).
    $owner_hex TEXT := LOWER($owner);
    if substring($owner_hex, 1, 2) = '0x' { $owner_hex := substring($owner_hex, 3, length($owner_hex)); }
    if length($owner_hex) != 40 { ERROR('owner must be a 20-byte hex address (40 hex chars, optional 0x prefix)'); }
    $owner_bytes BYTEA := decode($owner_hex, 'hex');

    for $r in
        SELECT
            '0x' || encode(maa_address, 'hex') AS maa_a,
            '0x' || encode(rule_id, 'hex')     AS rid,
            created_at
        FROM maa_instances
        WHERE unrestricted_addr = $owner_bytes
        ORDER BY created_at ASC, maa_address ASC
        LIMIT $limit OFFSET $offset
    {
        RETURN NEXT $r.maa_a, $r.rid, $r.created_at;
    }
};

CREATE OR REPLACE ACTION maa_list_instances_by_rule($rule_id BYTEA, $limit INT, $offset INT)
PUBLIC VIEW RETURNS TABLE(
    maa_address TEXT,
    unrestricted_addr TEXT,
    created_at INT8
) {
    if $limit IS NULL OR $limit <= 0 { $limit := 100; }
    if $offset IS NULL OR $offset < 0 { $offset := 0; }

    for $r in
        SELECT
            '0x' || encode(maa_address, 'hex')       AS maa_a,
            '0x' || encode(unrestricted_addr, 'hex') AS unrestr_a,
            created_at
        FROM maa_instances
        WHERE rule_id = $rule_id
        ORDER BY created_at ASC, maa_address ASC
        LIMIT $limit OFFSET $offset
    {
        RETURN NEXT $r.maa_a, $r.unrestr_a, $r.created_at;
    }
};

CREATE OR REPLACE ACTION maa_get_events($rule_id BYTEA, $limit INT, $offset INT)
PUBLIC VIEW RETURNS TABLE(
    id INT8,
    maa_address TEXT,
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
            id,
            CASE WHEN maa_address IS NULL THEN NULL ELSE '0x' || encode(maa_address, 'hex') END AS maa_a,
            event_type, actor_role,
            '0x' || encode(actor_addr, 'hex') AS actor_a,
            inner_namespace, inner_action, amount,
            '0x' || encode(tx_hash, 'hex') AS txh,
            block_height, block_timestamp
        FROM maa_events
        WHERE rule_id = $rule_id
        ORDER BY id ASC
        LIMIT $limit OFFSET $offset
    {
        RETURN NEXT $r.id, $r.maa_a, $r.event_type, $r.actor_role, $r.actor_a,
                    $r.inner_namespace, $r.inner_action, $r.amount, $r.txh,
                    $r.block_height, $r.block_timestamp;
    }
};

-- maa_is_known: true iff $maa_address is a joined MAA (used later by the exec route to detect MAAs).
CREATE OR REPLACE ACTION maa_is_known($maa_address BYTEA)
PUBLIC VIEW RETURNS (known BOOL) {
    for $r in SELECT 1 AS one FROM maa_instances WHERE maa_address = $maa_address {
        RETURN true;
    }
    RETURN false;
};
