/*
 * MIGRATION 049: MODULAR AGENT ADDRESSES (MAA) — WITHDRAWAL WITH COMMISSION
 *
 * Builds on 048-maa.sql (the rule/instance/audit store). This file owns the MONEY
 * MOVEMENT: the owner withdraws the agent wallet's funds, paying the agent its agreed
 * commission on the way out (the "withdrawal rule").
 *
 * FUNDING is intentionally NOT a new action here: funding an agent wallet is a normal
 * bridged-token transfer to the (now-known) MAA address — the existing per-bridge transfer
 * actions and L1 deposits already credit kwil_erc20_meta.balances keyed by that address.
 *
 * WITHDRAWAL runs AS the agent wallet: the maa_exec route rewrites @caller to the MAA
 * address before calling maa_withdraw / maa_bridge_out, so the bridge transfer/bridge
 * primitives below debit the MAA's OWN balance. A normal signer who calls these directly
 * has @caller = themselves, which is not a known MAA, so the lookup fails closed and moves
 * nothing. The recipients are always resolved from the store (restricted = commission,
 * unrestricted = payout) — never from a caller argument — so the destinations cannot be
 * steered. Each withdrawal is two legs (commission -> restricted, payout -> owner/L1) in a
 * single action, and every transaction's route body runs inside its own nested DB
 * transaction that is rolled back when the route errors — so a failure on either leg
 * leaves nothing applied (the engine itself does not undo an aborted action's earlier
 * writes; the per-transaction boundary does).
 *
 * Fee math: 'bps' = gross * fee_bps / 10000, rounded HALF-UP to a whole base unit (the
 * engine's NUMERIC rounding mode); 'flat' = fee_flat in the withdrawn token's own base
 * units. The commission is deducted FROM the gross (the owner receives the remainder), so
 * the two legs always sum to the gross exactly regardless of rounding; it is clamped to
 * never exceed the gross. There is no protocol fee cap.
 *
 * Token-agnostic: an agent wallet can hold several bridged tokens, so withdrawal targets one
 * bridge per call via the $bridge argument. The bridge namespace is the per-token instance:
 * dev = hoodi_tt (TRUF) / hoodi_tt2 (USDC); the generated mainnet .prod.sql twin targets
 * eth_truf / eth_usdc.
 */

-- =============================================================================
-- Bridge dispatch helpers — the only per-environment (dev vs mainnet) surface.
-- The mainnet .prod.sql twin substitutes the bridge namespaces; everything below
-- these helpers is environment-independent.
-- =============================================================================

-- maa_dispatch_balance: the agent wallet's spendable ledger balance for one token.
-- The ERC20 ledger only ever holds free funds — order-book collateral is moved OUT of the
-- ledger into the network vault by lock() — so this is already net of locked collateral.
CREATE OR REPLACE ACTION maa_dispatch_balance($bridge TEXT, $addr TEXT)
PRIVATE VIEW RETURNS (balance NUMERIC(78, 0)) {
    if $bridge = 'hoodi_tt2' {
        RETURN hoodi_tt2.balance($addr);
    } else if $bridge = 'hoodi_tt' {
        RETURN hoodi_tt.balance($addr);
    } else {
        ERROR('Invalid bridge. Supported: hoodi_tt, hoodi_tt2');
    }
};

-- maa_dispatch_transfer: internal ledger transfer; debits @caller, credits $to.
CREATE OR REPLACE ACTION maa_dispatch_transfer($bridge TEXT, $to TEXT, $amount NUMERIC(78, 0))
PRIVATE {
    if $bridge = 'hoodi_tt2' {
        hoodi_tt2.transfer($to, $amount);
    } else if $bridge = 'hoodi_tt' {
        hoodi_tt.transfer($to, $amount);
    } else {
        ERROR('Invalid bridge. Supported: hoodi_tt, hoodi_tt2');
    }
};

-- maa_dispatch_offramp: L1 off-ramp (bridge out); debits @caller, sends to $recipient on L1.
CREATE OR REPLACE ACTION maa_dispatch_offramp($bridge TEXT, $recipient TEXT, $amount NUMERIC(78, 0))
PRIVATE {
    if $bridge = 'hoodi_tt2' {
        hoodi_tt2.bridge($recipient, $amount);
    } else if $bridge = 'hoodi_tt' {
        hoodi_tt.bridge($recipient, $amount);
    } else {
        ERROR('Invalid bridge. Supported: hoodi_tt, hoodi_tt2');
    }
};

-- =============================================================================
-- maa_commission: the commission charged on a gross withdrawal, per the rule's fee terms.
-- 'flat' returns fee_flat verbatim (the withdrawn token's base units); 'bps' computes
-- gross * fee_bps / 10000, rounded half-up to a whole base unit.
-- =============================================================================
CREATE OR REPLACE ACTION maa_commission(
    $fee_mode TEXT,
    $fee_bps  INT,
    $fee_flat NUMERIC(78, 0),
    $gross    NUMERIC(78, 0)
) PRIVATE RETURNS (commission NUMERIC(78, 0)) {
    if $fee_mode = 'flat' {
        RETURN $fee_flat;
    }
    -- The division is brought to scale 0 with the engine's NUMERIC rounding mode, which is
    -- ROUND HALF-UP (e.g. 19999 * 250 / 10000 = 499.975 -> 500; .5 rounds away from zero).
    -- The dust direction never creates or destroys funds: the caller pays out
    -- gross - commission, so the two legs always sum to the gross exactly.
    $c NUMERIC(78, 0) := ($gross * $fee_bps::NUMERIC(78, 0)) / 10000::NUMERIC(78, 0);
    RETURN $c;
};

-- =============================================================================
-- maa_do_withdraw: the shared two-leg withdrawal body. Runs AS the MAA (@caller = MAA).
-- $offramp = false -> payout is an internal transfer to the owner; true -> payout is an L1
-- bridge-off to $recipient (defaulting to the owner).
-- =============================================================================
CREATE OR REPLACE ACTION maa_do_withdraw(
    $bridge    TEXT,
    $amount    NUMERIC(78, 0),
    $offramp   BOOL,
    $recipient TEXT
) PRIVATE {
    -- @caller is the MAA (rewritten by the maa_exec route). A direct caller is not a known
    -- MAA, so the lookup below fails closed and nothing moves.
    $maa_bytes BYTEA := tn_utils.get_caller_bytes();

    if $amount IS NULL OR $amount <= 0::NUMERIC(78, 0) {
        ERROR('withdraw amount must be positive');
    }

    -- Resolve the instance: this caller MUST be a known agent wallet.
    $rule_id BYTEA;
    $unrestricted_bytes BYTEA;
    $found BOOL := false;
    for $i in SELECT rule_id, unrestricted_addr FROM maa_instances WHERE maa_address = $maa_bytes {
        $rule_id := $i.rule_id;
        $unrestricted_bytes := $i.unrestricted_addr;
        $found := true;
    }
    if !$found {
        ERROR('caller is not a known agent wallet (withdrawal must run as the MAA)');
    }

    -- Resolve the rule's restricted creator (the commission payee) and the fee terms.
    $restricted_bytes BYTEA;
    $fee_mode TEXT;
    $fee_bps INT;
    $fee_flat NUMERIC(78, 0);
    $rule_found BOOL := false;
    for $r in SELECT restricted_addr, fee_mode, fee_bps, fee_flat FROM maa_rules WHERE rule_id = $rule_id {
        $restricted_bytes := $r.restricted_addr;
        $fee_mode := $r.fee_mode;
        $fee_bps := $r.fee_bps;
        $fee_flat := $r.fee_flat;
        $rule_found := true;
    }
    -- The maa_instances FK guarantees the rule exists with NOT NULL / CHECK-valid fee + payee
    -- columns, but guard explicitly so a withdrawal can never proceed (or record an event) with a
    -- NULL payee or fee term if that invariant is ever weakened.
    if !$rule_found {
        ERROR('agent wallet references a missing rule');
    }

    $maa_hex TEXT := '0x' || encode($maa_bytes, 'hex');

    -- Withdraw free balance only. The ledger balance is already the free balance (locked
    -- order-book collateral lives in the network vault, not the ledger).
    $free NUMERIC(78, 0) := COALESCE(maa_dispatch_balance($bridge, $maa_hex), 0::NUMERIC(78, 0));
    if $free < $amount {
        ERROR('insufficient free balance for withdrawal');
    }

    -- Commission deducted FROM the gross; clamped so it never exceeds the gross.
    $commission NUMERIC(78, 0) := maa_commission($fee_mode, $fee_bps, $fee_flat, $amount);
    if $commission > $amount {
        $commission := $amount;
    }
    $payout NUMERIC(78, 0) := $amount - $commission;

    $restricted_hex TEXT := '0x' || encode($restricted_bytes, 'hex');
    $unrestricted_hex TEXT := '0x' || encode($unrestricted_bytes, 'hex');

    -- Commission leg first (fee-leg-first keeps the move atomic). Internal transfer
    -- MAA -> restricted: the agent's earned commission lands in its own plain wallet,
    -- spendable with a normal signature.
    if $commission > 0::NUMERIC(78, 0) {
        maa_dispatch_transfer($bridge, $restricted_hex, $commission);
    }

    -- Payout leg: to the owner internally, or an L1 bridge-off to a chosen recipient
    -- (defaulting to the owner). Both debit @caller = MAA.
    if $payout > 0::NUMERIC(78, 0) {
        if $offramp {
            $dest TEXT := LOWER(COALESCE($recipient, $unrestricted_hex));
            maa_dispatch_offramp($bridge, $dest, $payout);
        } else {
            maa_dispatch_transfer($bridge, $unrestricted_hex, $payout);
        }
    }

    maa_record_event($rule_id, $maa_bytes, 'WITHDRAW', 'unrestricted', $unrestricted_bytes, NULL, NULL, $amount);
};

-- =============================================================================
-- Public withdrawal entry points (called by the owner through the maa_exec route).
-- =============================================================================

-- maa_withdraw: move $amount of $bridge out of the agent wallet to the owner, internally
-- (commission to the agent first). SDK name: withdraw.
CREATE OR REPLACE ACTION maa_withdraw($bridge TEXT, $amount NUMERIC(78, 0)) PUBLIC {
    maa_do_withdraw($bridge, $amount, false, NULL);
};

-- maa_bridge_out: same as maa_withdraw but the payout is bridged off to L1 ($recipient
-- defaults to the owner). SDK name: bridgeOut.
CREATE OR REPLACE ACTION maa_bridge_out($bridge TEXT, $amount NUMERIC(78, 0), $recipient TEXT DEFAULT NULL) PUBLIC {
    maa_do_withdraw($bridge, $amount, true, $recipient);
};

-- =============================================================================
-- maa_get_balance: the agent wallet's free balance for one token (explorer/SDK surface).
-- =============================================================================
CREATE OR REPLACE ACTION maa_get_balance($maa_address BYTEA, $bridge TEXT)
PUBLIC VIEW RETURNS (balance NUMERIC(78, 0)) {
    $addr TEXT := '0x' || encode($maa_address, 'hex');
    RETURN COALESCE(maa_dispatch_balance($bridge, $addr), 0::NUMERIC(78, 0));
};
