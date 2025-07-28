# Node Upgrade Guide

Keeping your node up-to-date ensures you benefit from the latest performance improvements, bug-fixes, and security patches.  
This page describes the **most common upgrade path – an in-place binary upgrade that does *not* change consensus rules** (i.e. no schema or protocol breaking changes).

---

## 1. (Optional but Recommended) Backup

While an in-place binary upgrade is safe, taking a quick backup guarantees you can roll back if something goes wrong.

* **Node root directory** – copy the folder that contains `config.toml`, `genesis.json`, `nodekey.json`, and the `store/` & `db/` sub-folders.  
  Example:
  ```bash
  cp -r ~/.kwild ~/.kwild.bak.$(date +%Y%m%d_%H%M%S)
  ```
* **PostgreSQL database** – make a snapshot or export of your `kwild` database using whatever backup tooling you normally rely on (e.g. cloud volume snapshot, `pg_dump`, `pgBackRest`, etc.).

Upgrades normally leave data intact; the backup is an insurance policy.

---

## 2. Stop Services

```bash
# systemd example
sudo systemctl stop kwild
# If you run Postgres in a container, it can keep running
```

If you manage `kwild` with another init system or inside Docker Compose, use the matching stop command (`docker compose stop`, `task single:stop`, etc.).

---

## 3. Install the Latest `kwild` Binary

1. **Download** – grab the newest release from GitHub:
   ```bash
   curl -L "https://github.com/trufnetwork/node/releases/latest/download/tn_<VERSION>_linux_amd64.tar.gz" \
       -o kwild.tgz
   tar -xzf kwild.tgz kwild
   ```
   Note: This extracts the `kwild` binary to your current directory.

   Or, **build from source** in your clone:
   ```bash
   git pull && task build
   ```
   Note: This builds the binary to `.build/kwild` in your repository directory.

2. **Replace** the old binary (adjust paths based on where you downloaded/built and where kwild is installed):
   
   If you built from source:
   ```bash
   sudo mv .build/kwild /usr/local/bin/kwild
   sudo chmod +x /usr/local/bin/kwild
   ```
   
   If you downloaded the binary:
   ```bash
   sudo mv kwild /usr/local/bin/kwild
   sudo chmod +x /usr/local/bin/kwild
   ```
   
   Note: Adjust the source path if you downloaded/built in a different location, and the destination path if your kwild binary is installed elsewhere (e.g., `/usr/bin/kwild`, `~/bin/kwild`, etc.).
3. Check the version:
   ```bash
   kwild version
   ```

---

## 4. Start Services

```bash
sudo systemctl start kwild
```

Validate that the node is running and catching up:
```bash
kwild admin status
```
`syncing: false` indicates you are fully synced.

---

## 5. Post-Upgrade Verification

1. Tail the logs for a minute to ensure there are no showstopper errors:
   ```bash
   sudo journalctl -u kwild -f | cat
   ```
2. Confirm your best block height advances and peers connect.
3. Use the built-in health endpoint to confirm the node is **healthy** and **not syncing**:
   ```bash
   # Overall health should be true
   curl -s http://127.0.0.1:8484/api/v1/health | jq '.healthy'

   # Node-level sync status should be false once caught up
   curl -s http://127.0.0.1:8484/api/v1/health | jq '.services.user.syncing'
   ```

   A value of `true` for `.healthy` **and** `false` for `.services.user.syncing` means the upgrade was successful. If syncing remains `true` for an extended period, inspect the logs and peer connectivity.

---

## What About PostgreSQL & Other Components?

*Minor* Kwil releases do **not** require a database upgrade.  
If the release notes specify a new official Postgres image (e.g. `kwildb/postgres:x.y-z`) you can recreate the container at your convenience – data volumes are preserved.

---

## Breaking Changes & Migrations

When a new version introduces consensus-breaking changes, a simple binary replacement is **not sufficient**.  
These scenarios require a **network migration** (offline or zero-downtime) and coordination with the core team.

### Creating your own fork

If you want to create your own fork (as opposed to recovering from another node's network fork), you can follow the [Offline migrations guide](https://docs.kwil.com/docs/node/migrations/offline-migrations) to create a new genesis file and a snapshot of the database.

Then you'll be able to follow the [Resetting Your Node Instance](node-operator-guide.md#resetting-your-node-instance) procedure to reset your node and sync with the new genesis file and database snapshot. It's important to also update the `config.toml` file to point to the new snapshot by updating the `genesis_state` parameter.

### Network Fork Recovery

When the network requires a fork due to consensus changes or critical issues, you'll need to reset your node with a new genesis file.

**Prerequisites:**
- Check official announcements for specific migration instructions
- Ensure you have the latest `truf-node-operator` repository (`git pull`)

**Recovery Steps:**

Follow the **[Resetting Your Node Instance](node-operator-guide.md#resetting-your-node-instance)** procedure in the Node Operator Guide. The process preserves your node identity and configuration while syncing with the new network state.

**What's Preserved During Network Forks:**
- Node identity (nodekey) and validator status
- All custom configuration settings
- Chain ID continuity
- Historical data up to the fork point

### Other Migration Types

See:
* [Migrations overview](https://docs.kwil.com/docs/node/migrations)
* [Zero-downtime migrations guide](https://docs.kwil.com/docs/node/migrations/zero-downtime-migrations)

If your target version mentions a *migration required* flag in the release notes, **contact the TRUF.NETWORK team before proceeding**.

---

### Still Stuck?

Reach out on Discord or open a GitHub issue – we are happy to help.
