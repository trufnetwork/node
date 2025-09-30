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

## 6. (New Feature) Enable Peer Blacklisting

Starting with recent versions, kwil-db includes peer blacklisting functionality to improve network stability and give operators better control over problematic peers.

### What Blacklisting Provides

- **Manual peer control** - Block specific peers via CLI commands
- **Persistent storage** - Blacklist survives node restarts
- **Automatic protection** - Auto-blacklist peers that exhaust connection retries (when enabled)
- **Better network health** - Prevent resource waste from problematic peers

### Enable Blacklist (Recommended)

**Option 1: Automated (append to config)**

Use this bash command to automatically append the blacklist configuration to the end of your `config.toml` file if the blacklist section doesn't already exist:

```bash
# Stop your node first
sudo systemctl stop kwild

# Add blacklist configuration to end of config.toml
# Note: Adjust the path if you used a different directory during setup
cat >> ~/truf-node-operator/my-node-config/config.toml << 'EOF'

# peer blacklisting configuration
[p2p.blacklist]
# enable peer blacklisting functionality
enable = true
# automatically blacklist peers that exhaust connection retries
auto_blacklist_on_max_retries = true
# duration to blacklist peers that exhaust connection retries (0 = permanent)
auto_blacklist_duration = "1h"
EOF

# Restart node
sudo systemctl start kwild
```

This bash heredoc command (`cat >> file << 'EOF'`) safely appends the configuration block to the bottom of your config file without modifying existing sections.

**Note**: The path above assumes you followed the standard [Node Operator Guide](https://github.com/trufnetwork/node/blob/main/docs/node-operator-guide.md) which uses `~/truf-node-operator/my-node-config/`. If you created your configuration in a different location, adjust the path accordingly.

**Option 2: Manual Configuration**

Edit your `~/truf-node-operator/my-node-config/config.toml` and add this section if it doesn't exist (adjust path if you used a different directory):

```toml
# peer blacklisting configuration
[p2p.blacklist]
# enable peer blacklisting functionality
enable = true
# automatically blacklist peers that exhaust connection retries
auto_blacklist_on_max_retries = true
# duration to blacklist peers that exhaust connection retries (0 = permanent)
auto_blacklist_duration = "1h"
```

**Supported duration formats:**
- `"30s"` - 30 seconds
- `"5m"` - 5 minutes
- `"1h"` - 1 hour (default)
- `"2h30m"` - 2 hours and 30 minutes
- `"1h30m45s"` - 1 hour, 30 minutes, and 45 seconds
- `"0s"` or `"0"` - Permanent blacklist

### Using Blacklist Commands

Once enabled, you can manage peer blacklists:

```bash
# Block a problematic peer permanently
kwild blacklist add 12D3KooWExample... --reason "connection_issues"

# Block a peer temporarily (1 hour)
kwild blacklist add 12D3KooWExample... --reason "temporary_ban" --duration "1h"

# List blacklisted peers
kwild blacklist list

# Remove a peer from blacklist
kwild blacklist remove 12D3KooWExample...
```

### Verification

Check that blacklist is working:
```bash
# Should show blacklist section in config
kwild print-config | grep -A 5 "blacklist"

# Test CLI access
kwild blacklist list
```
---

## What About PostgreSQL & Other Components?

*Minor* Kwil releases do **not** require a database upgrade.  
If the release notes specify a new official Postgres image (e.g. `ghcr.io/trufnetwork/kwil-postgres:x.y-z`) you can recreate the container at your convenience – data volumes are preserved.

---

## Breaking Changes & Migrations

When a new version introduces consensus-breaking changes, a simple binary replacement is **not sufficient**.  
These scenarios require a **network migration** (offline or zero-downtime) and coordination with the core team.

### Creating your own fork

If you want to create your own fork (as opposed to recovering from another node's network fork), you can follow the [Offline migrations guide](https://docs.kwil.com/docs/node/migrations/offline-migrations) until **step 2** to create a new genesis file and a snapshot of the database. 

After you create the new genesis file, beware that the content still points to the old network configuration, please open the generated genesis file and confirm about:
1. `db_owner`: admin address

- Testnet example: `0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf`  
- Corresponding test key: `0000…001`

2. `validators`: every entry’s `pubkey` and `type` must match the validator nodes you will run, either the validator will tell you the details or for a single-validator test network you can reuse your own key.

3. `leader`: choose which validator starts as leader. In a single-validator setup this is the same key as in step 2.

Then you'll be able to follow the [Resetting Your Node Instance](node-operator-guide.md#resetting-your-node-instance) procedure to reset your node and sync with the new genesis file and database snapshot. It's important to also update the `config.toml` file to point to the new snapshot by updating the `genesis_state` parameter.

note: 
- You can use this command to see your node address and keys, `kwild key info -o ./new-node-config/nodekey.json`
- Please be careful about the directory you use for new network / fork, don't use the old directory / content or you will generate the wrong data / encounter errors.

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
