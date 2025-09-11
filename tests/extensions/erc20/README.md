Notice: to run tests in this directory, you need to build the node with the `kwiltest` tag.
To make it easier on your IDE, you might want to include in the configurations, for example, for VSCode, you can add the following to your `settings.json`:

```json
{
    "go.testTags": "kwiltest"
}
```

### Objective

This directory demonstrates a minimal, production-faithful structure for wiring and invoking the ERC‑20 bridge extension in tests. It:

- Shows how to bootstrap an instance, call actions, and cleanly tear down using the `ForTesting*` helpers.
- Demonstrates how tests should evoke the extension (e.g., alias usage, numeric arguments, engine context, authz boundaries) in isolation.
- Serves as a reference pattern to later integrate these flows into our standard streams tests appropriately.

### Quick start

- Enable the `kwiltest` tag (above) so the test helpers are available.
- Seed and activate an instance for a test:

```go
err := erc20shim.ForTestingSeedAndActivateInstance(ctx, platform, chain, escrow, erc20, 18, 60, alias)
```

- Invoke actions using the alias (e.g., `balance`, `bridge`, `lock_admin`).
- For epoch flows, use `ForTestingFinalizeAndConfirmCurrentEpoch(...)` to deterministically advance.
- Cleanup explicitly with `ForTestingDisableInstance(...)` or rely on the test wrapper’s registered cleanup.

### Why helpers over plain SQL/`USE ...` for ERC-20 tests

- **The ERC-20 bridge extension has runtime wiring beyond SQL.** In production it creates and manages background components and in‑memory state: ordered‑sync topics/listeners for `Transfer` events, a state poller, a singleton cache rehydrated on start, epoch lifecycle rows, and synced ERC‑20 metadata. A bare `USE erc20 { ... } AS alias` only creates the alias; it does not guarantee these side‑effects are present or initialized in the current process.
- **Plain seeds miss critical initialization.** Relying only on SQL seeds/`USE` won’t ensure the `kwil_erc20_meta` schema exists, the `reward_instances` row and first epoch are created, the instance is marked synced with the ERC‑20 address/decimals, the ordered‑sync topic is registered, or that the extension’s singleton has been rehydrated. That leads to flaky tests and "already active"/missing‑wiring failures.

### What `ForTestingSeedAndActivateInstance` does

`ForTestingSeedAndActivateInstance(ctx, platform, chain, escrow, erc20, decimals, periodSeconds, alias)` performs, idempotently:

- Creates the extension alias via `USE erc20 { chain: '<chain>', escrow: '<escrow>' } AS <alias>`.
- Ensures the `kwil_erc20_meta` namespace/schema exist and computes the deterministic instance ID.
- Registers the ordered‑sync transfer topic/state‑poller names for the instance.
- Creates the `reward_instances` row if missing and guarantees a first pending epoch.
- Marks the instance as synced with the provided ERC‑20 address and `decimals`.
- Sets the distribution period to `periodSeconds` for predictable epochs.
- Re‑initializes the extension (OnStart‑like) so the in‑memory singleton reflects DB state.

This yields a production‑like, deterministic setup inside a test transaction, without depending on external chains.

### Cleanup: disabling and teardown

`ForTestingDisableInstance(ctx, platform, chain, escrow, alias)` performs deterministic teardown:

- Executes `UNUSE <alias>` to drop the alias in the test DB.
- Calls `kwil_erc20_meta.disable(id)` to deactivate the instance in storage.
- Unregisters the state poller and transfer listener for the instance.
- Resets and re‑initializes the singleton to a clean state.

Additionally, our test wrapper registers cleanup that resets the singleton and clears runtimes to prevent cross‑test contamination.

### TL;DR

Use the provided `ForTesting*` helpers instead of plain SQL/`USE` to mirror production wiring that SQL alone cannot trigger, ensuring deterministic, isolated ERC‑20 extension tests.