# Modular Agent Addresses — the worked examples

A **Modular Agent Address (MAA)** is a non-custodial agent wallet: a token holder (the
*owner*) delegates a limited set of actions to an *agent* key, which can operate the wallet —
but provably **cannot move funds out**. The owner keeps full control and withdraws at any time,
paying the agent an agreed commission. This is TRUF.NETWORK's analogue of a Safe Module.

This document walks the two end-to-end examples:

1. a **Liquidity Vault Operator** — an LP funds an agent that runs a trading bot on the order
   books;
2. a **Data Provision Agent** — an AI agent creates indexes and provides regular data to them,
   paying the network's write fees out of the wallet's escrow.

> Roles, in MAA terms:
> - **Restricted address (agent):** creates the rule; limited to the rule's allow-list; can
>   never move funds out.
> - **Unrestricted address (owner / LP):** funds the agent wallet; can run any allowed action
>   *and* withdraw funds at any time.
>
> The two component keys sign their own transactions; the **MAA** is the wallet they operate.
> See the rule store (`048-maa.sql`) and the withdrawal/commission actions (`049-maa-funding.sql`).

Activating the `maa_exec` route on a running network is a coordinated, height-gated rollout —
see the [MAA activation rollout runbook](./maa-rollout.md).

## Example 1 — Liquidity Vault Operator

### The canonical LP allow-list

A liquidity-vault agent is allow-listed for exactly the four order-book trading actions, all in
the default (`main`) namespace:

| Namespace | Action                    | Purpose                                    |
|-----------|---------------------------|--------------------------------------------|
| `main`    | `place_buy_order`         | Open a buy order (locks collateral)        |
| `main`    | `place_sell_order`        | Open a sell order from existing holdings   |
| `main`    | `place_split_limit_order` | Mint a pair and list one side              |
| `main`    | `cancel_order`            | Cancel an open order (unlocks collateral)  |

These are the only powers the bot needs. Deliberately **excluded**:

- **`create_market`** — market provisioning is not a liquidity-maintenance task; the market is
  created by an ordinary account. (Its leader-paid creation fee would pass the token boundary's
  write-fee carve-out, but the allow-list is the owner's choice of *job*, not of what's possible.)
- **`settle_market`** — settlement is permissionless and left to the network.
- **Any withdrawal / bridge primitive** — exits are never allow-listed; the owner withdraws
  through the route-privileged `maa_withdraw` / `maa_bridge_out`, which the agent cannot reach.

Body-hash pinning (`body_hash`) is supported by the rule store but left unpinned in this
example.

### Why the agent can lock collateral but can't steal

Every order-book trade only ever moves the agent wallet's tokens through the erc20 **`lock`**
(escrow into the network vault) and **`unlock`** (refunds, fills, settlement) primitives. The
MAA token boundary blocks `transfer` / `bridge` / `issue` / `lock_admin` for a restricted agent
at any call depth, but leaves `lock` / `unlock` open — so allow-listed trading survives while
every path that would move funds *out of the network* for the agent is closed. Placing an order
escrows collateral; it never sends tokens to an address the agent chooses.

There is exactly **one carve-out** in the boundary: a `transfer` whose recipient is the **block
leader's sender address** — the network's write-fee sink (`@leader_sender` in the fee-charging
actions). The recipient is consensus-determined, never a call parameter, so the agent still
cannot steer a single token to an address of its choosing; the worst a malicious agent can do
is spend the wallet's escrow on protocol fees, which is precisely the spending the owner funded
it for. The carve-out is what makes the Data Provision Agent below possible.

### Lifecycle

1. **Create the rule** — the agent signs `maa_create_rule(salt, 'bps', fee_bps, fee_flat,
   namespaces, actions, body_hashes)` with the allow-list above, and gets a `rule_id`.
2. **Join** — the LP signs `maa_join(rule_id)`, which derives and registers the agent wallet
   (the MAA address) and binds the LP as the owner.
3. **Fund** — the LP sends bridged tokens (e.g. USDC) to the MAA address. No special action;
   it is a normal bridged-token transfer/deposit naming the MAA.
4. **Trade** — the agent runs the bot, submitting `place_*` / `cancel_order` *as the MAA*
   through the agent route. Collateral locks against the MAA's own balance.
5. **Monitor** — the LP reads the bot's activity with
   `get_order_events_by_wallet($maa_address, $query_id, $limit, $offset)` (placements, fills,
   cancels), and the rule/audit getters (`maa_get_instance`, `maa_get_rule`,
   `maa_get_allowed_actions`, `maa_get_events`).
6. **Withdraw any time** — the LP signs `maa_withdraw($bridge, $amount)` (internal payout) or
   `maa_bridge_out($bridge, $amount, $recipient)` (off-ramp). The agent's commission is paid out
   of the gross on the way out; the owner receives the remainder. Withdrawals operate on the
   **free** balance, so collateral in open orders must be unwound (`cancel_order`) first — after
   which the LP can recover everything.

### Monitoring getter

`get_order_events_by_wallet` (migration `050-order-book-event-queries.sql`) exposes the order
book's per-event history for any wallet address — the surface an owner uses to watch an agent
wallet they do not sign for. (The existing portfolio getters are caller-scoped and only return
the signer's own rows.)

```
get_order_events_by_wallet(
  $wallet_address TEXT,        -- the MAA address (with or without 0x)
  $query_id INT DEFAULT NULL,  -- NULL = all markets
  $limit INT DEFAULT 100,
  $offset INT DEFAULT 0
) -> (id, query_id, event_type, outcome, price, amount,
      wallet_address, counterparty_address, block_height, block_timestamp)
```

`event_type` is one of: `buy_placed`, `sell_placed`, `split_placed`, `bid_changed`,
`ask_changed`, `cancelled`, `direct_buy_fill`, `direct_sell_fill`, `mint_fill`, `burn_fill`,
`settled`.

### End-to-end test

`tests/streams/maa/lp_vault_test.go` drives the whole lifecycle against the real order book:

- **`testLPVaultLifecycle`** — the bot places an order as the MAA (collateral locks); the owner
  monitors it, withdraws the free portion (commission to the agent), cancels the order to free
  the rest, and withdraws everything — proving "withdraw at any time" returns *all* the LP's
  funds, net of commission.
- **`testLPVaultBotFillIsAudited`** — the bot's order fills against a counterparty (a mint
  match) and the fill is visible to the owner via `get_order_events_by_wallet`.
- **`testLPVaultAgentCannotDrain`** — the agent can place allow-listed orders, but every attempt
  to withdraw or bridge funds out is blocked at the token boundary; nothing moves and no
  commission is skimmed.

## Example 2 — Data Provision Agent

An AI agent creates new indexes (streams) and provides regular data to them, running entirely
as its agent wallet. The owner fills the wallet with **TRUF** — the agent's working budget —
secure that the agent can spend it only on the network's write fees, never take it.

### The data-provision allow-list

| Namespace | Action           | Purpose                                          |
|-----------|------------------|---------------------------------------------------|
| `main`    | `create_streams` | Create new indexes (owned by the agent wallet)    |
| `main`    | `insert_records` | Provide data to those indexes                     |

Both actions charge **caller-keyed** bridge fees — `create_streams` 100 TRUF per stream,
`insert_records` a flat 1 TRUF per transaction — so with `@caller` rewritten to the MAA they
debit the **agent wallet's own escrow**, with zero fee code specific to MAA. The fee
`transfer` targets `@leader_sender`, which is exactly the token boundary's write-fee
carve-out; any other `transfer` recipient remains blocked for the restricted agent.

The streams the agent creates belong to the **MAA**, not to the agent's own key: the agent
wallet is the data provider of record, so replacing the agent (a new rule + wallet) never
strands the data identity with a key the owner doesn't control.

### Lifecycle

1. **Create the rule** — the agent signs `maa_create_rule` with the two-action allow-list.
2. **Join** — the owner signs `maa_join(rule_id)` and gets the wallet address.
3. **Fill up** — the owner sends TRUF to the MAA address (a normal bridged-token transfer).
4. **Provide** — the agent submits `create_streams` / `insert_records` *as the MAA*; every fee
   comes out of the wallet's TRUF escrow and lands with the block leader. An underfunded wallet
   fails closed: the action reverts before any state is written.
5. **Monitor** — anyone can read the provided data back (`get_record`); the rule/audit getters
   (`maa_get_instance`, `maa_get_events`, …) cover the wallet itself.
6. **Withdraw any time** — the owner signs `maa_withdraw($bridge, $amount)` against the TRUF
   bridge and recovers the un-spent escrow, net of the agent's commission.

### End-to-end test

`tests/streams/maa/data_agent_test.go`:

- **`testDataAgentLifecycle`** — the agent creates an index and provides data as the MAA, the
  100-TRUF and 1-TRUF fees coming out of the wallet's escrow and landing with the block leader;
  the owner reads the data back and withdraws the rest, paying the commission.
- **`testDataAgentInsufficientEscrow`** — with less than the per-stream fee on the wallet, the
  agent's `create_streams` reverts; no stream, no partial fee.
- **`testDataAgentCannotExfiltrate`** — the agent can do its fee-paying job, but withdrawing,
  bridging out, and raw transfers to any non-leader recipient are all blocked at the token
  boundary; nothing moves and no commission is skimmed.
