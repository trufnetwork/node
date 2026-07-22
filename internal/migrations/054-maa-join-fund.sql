/*
 * MIGRATION 054: MODULAR AGENT ADDRESSES (MAA) — ATOMIC JOIN + FUND
 *
 * One-transaction MAA activation (truf-network#1399): a funder joins a rule AND funds the
 * derived agent wallet in a single action, so activation either completes whole or leaves
 * nothing behind. Closes the two-transaction window in the 048 join -> ordinary-transfer
 * funding flow (049 header), where a failure between the two transactions left a
 * joined-but-unfunded wallet.
 *
 * A kwil transaction executes exactly one action, so the SDKs cannot compose maa_join plus
 * a bridge transfer atomically client-side; a composite action is the only place the two
 * legs can share a transaction. The whole body runs inside the route's nested DB
 * transaction (the same guarantee 049's two-leg withdrawal relies on): a failure on either
 * leg — unknown rule, duplicate join, invalid bridge, insufficient balance — rolls back BOTH.
 *
 * Token-agnostic like the rest of the MAA surface: the per-environment bridge dispatch is
 * delegated to 049's PRIVATE maa_dispatch_transfer, whose mainnet .prod.sql twin already
 * remaps the bridge branches. This file names no bridge namespace, so it needs NO mainnet
 * .prod.sql twin — the file you read runs on both networks. Keep it that way: never name a
 * bridge namespace here (the prod-twin generator selects override candidates by textual
 * match on those names).
 *
 * FUND-FREE at the protocol level like the other MAA setup actions (048): no wrapper fee
 * and no transaction event. Top-ups after activation keep using the public per-bridge
 * transfer actions (and keep paying their flat fee). Spec + decisions:
 * 0GoalModularAgentAddresses/9MAA-Atomic-JoinFund-Spec.md.
 *
 * NEVER allow-list this action in a rule. Unlike the fund-free maa_join, it MOVES FUNDS:
 * run through the maa_exec route it would debit the acting MAA to fund a nested wallet the
 * MAA can never withdraw from (owner-exit is reserved for the unrestricted key, and that key
 * would be the outer MAA, which cannot sign) — i.e. a self-strand. A restricted agent is
 * blocked (the erc20 transfer guard fires on the restricted role), so no untrusted party can
 * strand or steal; only the owner could self-inflict it via a mis-authored rule. Value-moving
 * primitives are not meant to be allow-listed (see maa_exec route notes); this is a normal
 * wallet-signed activation call, not an agent action.
 */

-- =============================================================================
-- maa_join_and_fund: join a rule and fund the derived agent wallet atomically.
-- Leg 1 delegates to maa_join (048) — all of its guards apply: unknown rule_id,
-- creator-join rejection, duplicate-join rejection (an already-joined funder
-- tops up with a plain transfer instead). Leg 2 debits @caller (the funder) and
-- credits the fresh MAA on the chosen bridge's ledger.
-- SDK name: joinAndFundAgentAddress.
-- =============================================================================
CREATE OR REPLACE ACTION maa_join_and_fund($rule_id BYTEA, $bridge TEXT, $amount NUMERIC(78, 0))
PUBLIC RETURNS (maa_address BYTEA) {
    -- An atomic "join and fund" with an empty fund leg is just maa_join — reject it.
    -- Two separate ifs on purpose: the engine's OR does not follow SQL three-valued logic
    -- (TRUE OR NULL yields NULL, not TRUE), so a combined `$amount IS NULL OR $amount <= 0`
    -- collapses to NULL on a NULL amount and the `if` is skipped — letting the NULL fall
    -- through to the transfer. Checking IS NULL in its own statement fires reliably.
    if $amount IS NULL {
        ERROR('Funding amount must be positive');
    }
    if $amount <= 0::NUMERIC(78, 0) {
        ERROR('Funding amount must be positive');
    }

    -- Leg 1 — join: binds @caller as the unrestricted owner, derives and registers the MAA.
    $maa_address BYTEA := maa_join($rule_id);

    -- Leg 2 — fund: internal ledger transfer, funder -> agent wallet.
    $maa_hex TEXT := '0x' || encode($maa_address, 'hex');
    maa_dispatch_transfer($bridge, $maa_hex, $amount);

    RETURN $maa_address;
};
