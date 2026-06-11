# MAA Activation Rollout (the `maa_exec` hardfork)

This runbook activates Modular Agent Addresses (MAA) on a running network.
It covers the only consensus-coupled piece of the MAA stack that is not live
yet: the **`maa_exec` transaction route**, which lets a rule's restricted or
unrestricted key execute allow-listed actions *as* an agent wallet (see
[modular-agent-addresses.md](./modular-agent-addresses.md) for what MAA is
and the worked examples).

The rollout is deliberately **two-phase**, and the order is load-bearing.

## How activation works

`maa_exec` is gated by a consensus network parameter, **`maa_activation_height`**:

- **`0` (the default) means not activated.** Transactions are rejected at
  mempool admission and again at block execution, with the same "unknown
  payload type" answer a pre-MAA binary gives.
- **A non-zero height `H` activates the route from block `H` on.**

The parameter is engineered to be **invisible while zero**: it is omitted
from the stored network-params bytes and skipped in the params hash that
every block header carries (`NetworkParamsHash`). A binary that has the
parameter is therefore **byte-identical in consensus, block for block, to a
binary that predates it** — until the network explicitly sets the parameter.
That is what makes Phase 1 a plain, no-coordination binary upgrade.

Two consequences worth internalizing:

- The **only consensus-visible event** in this rollout is the on-chain
  resolution that sets the parameter (Phase 2). Everything before it is
  reversible and unobservable on-chain.
- Once the parameter is set, any node still running an old binary **halts
  cleanly** at the next block (`network params hash mismatch` in its logs)
  instead of forking silently. A straggler is a stuck node, never a chain
  split.

## Phase 0 — Preconditions

- A node release whose embedded kwil-db includes the MAA stack **and the
  activation gate** (kwil-db `main` at `1e94bbd6`, PR #1710, or later —
  i.e. a node `go.mod` pinning kwil-db `v0.10.3-0.20260611144500-9fab49f49c4a`
  or newer). The release notes will name the minimum node version.
- Admin RPC access on at least one validator (the default admin socket,
  e.g. `docker exec tn-node kwild consensus params`).

## Phase 1 — Upgrade the fleet (hash-neutral)

Upgrade **every node — all validators and all sentries — before Phase 2.**

Follow the standard [node upgrade guide](./node-upgrade-guide.md) per node
(backup → stop → install → start → verify). Stagger sentries; a mixed-version
fleet is safe for as long as you need in this phase, because the new binary
is consensus-identical while the parameter is unset.

Per-node verification (unchanged from any upgrade):

```bash
# height advances (run twice, ~10s apart)
curl -s http://127.0.0.1:8484/api/v1/health | jq '.services.user | {block_height, healthy, syncing}'
```

> **DANGER — do not start Phase 2 with old binaries still running.** A
> pre-MAA binary cannot decode the `maa_activation_height` parameter name;
> the scheduling resolution itself would diverge on it. Phase 2 begins only
> when the whole fleet reports the new version.

## Phase 2 — Schedule activation on-chain

### Pick the activation height

Measure the real block rate (do not assume it), then convert your desired
lead time:

```bash
H1=$(curl -s http://127.0.0.1:8484/api/v1/health | jq '.services.user.block_height'); sleep 60
H2=$(curl -s http://127.0.0.1:8484/api/v1/health | jq '.services.user.block_height')
echo "blocks/min: $((H2 - H1))"
```

```
H  =  current_height  +  lead_time_in_minutes × blocks_per_minute
```

Choose a lead time that comfortably covers operator sign-off and a re-check
of Phase 1 completeness (for a fleet with external operators, days; for a
fully internally-operated fleet, hours is defensible). At TN mainnet's
typical ~1 block/s, 24 hours ≈ 86,400 blocks.

### Propose (and, if needed, approve) the parameter update

On a validator, via the admin RPC:

```bash
kwild consensus propose \
  --updates '{"maa_activation_height": <H>}' \
  --description "activate maa_exec at height <H>"

# inspect / get the proposal id
kwild consensus list-proposals
kwild consensus update-status <proposal_id>
```

Approval threshold is >50% of validators; the proposer's own approval is
implicit. **On a single-validator network (TN mainnet today) the proposal
resolves by itself in the next block — treat `propose` as the activation
trigger.** On a multi-validator network, the remaining validators run:

```bash
kwild consensus approve <proposal_id>
```

### Verify the schedule on every node

```bash
kwild consensus params   # → maa_activation_height: <H>
```

Every node must report the same `H`. From the block where the resolution
passed, the params hash in block headers includes the parameter — this is
the activation fork, and the whole (upgraded) fleet crosses it together.

### Re-scheduling

`maa_activation_height` is an ordinary network parameter: a follow-up
resolution can move `H` later (or set `0` to cancel) **as long as it passes
before the earlier `H` is reached**.

## Phase 3 — Activation and verification

At height `H` the gate opens. Verify:

1. **Chain crosses `H` healthy** — height advances past `H` on every node;
   `healthy: true`, `syncing: false`.
2. **Pre-`H` rejection / post-`H` acceptance** — a broadcast `maa_exec`
   transaction is rejected with `maa_exec activates at height <H>` before,
   and executes after. (Until the MAAExec SDK encoders ship, the reference
   for driving a raw `maa_exec` transaction is kwil-db's
   `test/integration/maa_activation_test.go`, which rehearses this entire
   runbook — genesis-without-activation, schedule-by-resolution, pre-height
   rejection, post-height execution as the MAA, and cross-node AppHash
   equality at every height.)
3. **No stragglers** — a node that halts right after the schedule resolution
   with `network params hash mismatch` in its logs is running the old
   binary: upgrade it and restart; if it already diverged, recover via state
   sync (see the operations guide's AppHash-mismatch recovery).

## Failure modes and rollback

| Situation | What happens | What to do |
|---|---|---|
| Old binary in fleet during Phase 1 | Nothing — consensus-identical while the parameter is unset | Finish upgrading before Phase 2 |
| Old binary still running when Phase 2 resolution passes | That node halts at the next block (params hash mismatch) | Upgrade the node; state-sync if it diverged |
| Need to delay after scheduling | — | Propose a new resolution with a later `H` (or `0`) before `H` |
| Want MAA on a fresh dev/test network | — | `kwild setup genesis --maa-activation-height 1 …` activates from genesis |

There is no binary rollback after `H` with `maa_exec` traffic on-chain:
blocks past `H` may contain `maa_exec` transactions an old binary cannot
execute. Rolling back the binary after activation means state-syncing from
peers, not replaying.
