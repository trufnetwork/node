# Node Upgrade Guide

Keeping your node up-to-date ensures you benefit from the latest performance improvements, bug-fixes, and security patches.  
This page describes the **most common upgrade path – an in-place upgrade**: you replace the binary or the image and keep your data. A release that changes how blocks run from some block on is required by that block; see [Required In-Place Upgrade](#required-in-place-upgrade). A network migration to a new genesis is covered under [Breaking Changes & Migrations](#breaking-changes--migrations).

---

## Is the Upgrade Required?

Each release page opens with one line that says whether your node must install the release:

```text
Upgrade: optional.
Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).
Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), optional on testnet.
Upgrade: optional on mainnet, required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).
```

The blocks and times above are examples. `optional` means a node on the release before keeps following the chain. `required ... from block N` means your node must run this release, or a later one, before that network reaches block `N`. The time is an estimate from the block rate; the block is what counts.

A release that is required on mainnet is published at least 7 days before its mainnet block. Releases up to v2.6.8 were published before this line, so their pages do not carry it.

Release notices go out on the [release page](https://github.com/trufnetwork/node/releases). To get them, choose **Watch → Custom → Releases** on the [repository](https://github.com/trufnetwork/node), or follow the feed at <https://github.com/trufnetwork/node/releases.atom>.

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

1. **Download** – grab the release you are installing from GitHub. `<VERSION>` is its version without the `v`, such as `2.6.9`:
   ```bash
   curl -L "https://github.com/trufnetwork/node/releases/download/v<VERSION>/tn_<VERSION>_linux_amd64.tar.gz" \
       -o kwild.tgz
   tar -xzf kwild.tgz kwild
   ```
   Note: This extracts the `kwild` binary to your current directory.

   Or, **build from source** in your clone, at the release tag:
   ```bash
   git fetch --tags && git checkout v<VERSION> && task build
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
   `Version` is the TN release the binary was built from, and `Git commit` is the first 9 characters of its commit. A `v2.6.9` release binary prints, for example:
   ```text
    Version:	2.6.9
    Git commit:	<first 9 characters of the release commit>
    Built:		<commit time> (commit time)
    Go version:	go1.25.3
    OS/Arch:	linux/amd64
   ```
   `kwild --version` prints the same release (`kwild version 2.6.9`), and `kwild version --output json | jq -r .result.version` prints only `2.6.9`. A binary built with `task build` prints the `git describe` version of your clone instead, such as `2.6.8-4-g9e008683` for a clone 4 commits past `v2.6.8`.

   A binary that prints `0.10.1` (kwil-db's version) was built before TN releases carried their own version, as the v2.6.8 release was. Read its release from the release page or tarball you downloaded it from.

   If your node runs in Docker, read the release from the image label of the running container:
   ```bash
   docker inspect --format '{{ index .Config.Labels "org.opencontainers.image.version" }}' <container>
   ```
   For an image that carries the TN version, `docker exec <container> /app/kwild version` prints the same release; an older image prints `0.10.1` there.

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

## Required In-Place Upgrade

A required release changes how blocks run from the block its release page names. It runs every block before that one the same way as the release before it, so you can install it at any time before the block. A node that still runs an older release when the network reaches the block stops there; see [If Your Node Stopped at a Block](#if-your-node-stopped-at-a-block).

1. Read the first line of the release page, for example:
   ```text
   Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), optional on testnet.
   ```
   A mainnet node must run this release, or a later one, before mainnet reaches block 3300000. On testnet the release is optional.
2. Check how far your node is from that block:
   ```bash
   curl -s http://127.0.0.1:8484/api/v1/health | jq '.services.user.block_height'
   ```
3. Install the release before the block:
   * **Binary** (for example with systemd): follow steps 1 to 5 above with the release's version.
   * **Docker Compose**, as in the [Container Image Guide](container-image-guide.md): set `TN_NODE_TAG` in `.env` to the release tag, such as `v2.6.9`, then run:
     ```bash
     docker compose pull tn-node postgres-roles
     docker compose up -d
     ```
   * **AMI**: run `sudo tn-node-update v2.6.9` with the release tag. See [Updates](ami-deployment-guide.md#updates) in the AMI guide. A node built from an older AMI, one with no `/opt/tn/ami-node-tag` file, has a `tn-node-update` that ignores the tag; follow [Nodes From an Older AMI](ami-deployment-guide.md#nodes-from-an-older-ami) instead.
4. Check that your node reports the release, as in [Check the version](#3-install-the-latest-kwild-binary), and that its height keeps advancing.

### If Your Node Stopped at a Block

A node that runs an older release stops at the first block it computes differently from the network. `kwild` logs:

```text
processAndCommit: AppHash mismatch, halting the node. expected: <app hash your node computed>, received: <app hash the network committed>
```

When the node halts while following the chain, this text is the `reason` of the log line `Received halt signal, stopping the consensus engine`. When it halts while catching up, for example after a restart, the text is part of the error `kwild` stops with, such as `failed to apply block at height: <height>: error: processAndCommit: AppHash mismatch, halting the node. ...`. A validator that ran the block as a proposal logs `Incorrect AppHash, halting the node. received: <app hash the network committed>, computed: <app hash your node computed>` instead.

The node does not commit that block. Its writes stay in Postgres transactions that `kwild` rolls back when it stops or starts, the block is not stored, and the node stays at the block before it. Started again on the same release, it applies the block again and stops at the same place, so `Restart=always` or `restart: unless-stopped` only repeats the stop.

To recover:

1. On the [release page](https://github.com/trufnetwork/node/releases), find the release that is required from that block on.
2. Install it as in step 3 of [Required In-Place Upgrade](#required-in-place-upgrade).
3. Start the node, if the install step did not. It applies the block again with the new release and continues from there. You do not need to reset the node or restore a snapshot.
4. Check that the height passes the block:
   ```bash
   curl -s http://127.0.0.1:8484/api/v1/health | jq '.services.user.block_height'
   ```

If the node stops at the same block while `kwild version` prints the required release, reach out on Discord or open a GitHub issue with the log lines.

---

## What About PostgreSQL & Other Components?

*Minor* Kwil releases do **not** require a database upgrade.  
If the release notes specify a new official Postgres image (e.g. `ghcr.io/trufnetwork/kwil-postgres:x.y-z`) you can recreate the container at your convenience – data volumes are preserved.

`kwild` connects to Postgres as a role without superuser rights, set up by `deployments/postgres-roles.sql`. The Compose files run that script before the node starts. If you run `kwild` as a service, stop it, run the script once when you upgrade from a release that connected as a superuser, and start it again. See [Moving an Existing Node Off the Superuser Role](./node-operator-guide.md#moving-an-existing-node-off-the-superuser-role). Run the script again after an upgrade that changes it, such as the one that adds the event trigger for `pg_repack`'s copies. That change needs no restart of `kwild`, but run it between `tn_vacuum` runs, and read [Replica Identity Errors](../extensions/tn_vacuum/README.md#replica-identity-errors) first: once the trigger is in place, a run copies every table.

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

Then you'll be able to follow the [Resetting Your Node Instance](node-operator-guide.md#resetting-your-node-instance) procedure to reset your node and sync with the new genesis file and database snapshot. It's important to also update the `config.toml` file to point to the new snapshot by updating the `genesis_state` parameter. The procedure recreates the Postgres container on an empty volume, where the image's init script makes `kwild` a superuser again, so do not skip its step that runs `postgres-roles.sql` again before `kwild` starts.

note: 
- You can use this command to see your node address and keys, `kwild key info -o ./new-node-config/nodekey.json`
- Please be careful about the directory you use for new network / fork, don't use the old directory / content or you will generate the wrong data / encounter errors.

### Network Fork Recovery

When the network requires a fork due to consensus changes or critical issues, you'll need to reset your node with a new genesis file.

**Prerequisites:**
- Read the [release page](https://github.com/trufnetwork/node/releases) of the release that moves the network: it gives the migration instructions
- Ensure you have the latest `truf-node-operator` repository (`git pull`)

**Recovery Steps:**

Follow the **[Resetting Your Node Instance](node-operator-guide.md#resetting-your-node-instance)** procedure in the Node Operator Guide. The process preserves your node identity and configuration while syncing with the new network state. It recreates the Postgres container on an empty volume, where the image's init script makes `kwild` a superuser again, so do not skip its step that runs `postgres-roles.sql` again before `kwild` starts.

**What's Preserved During Network Forks:**
- Node identity (nodekey) and validator status
- All custom configuration settings
- Chain ID continuity
- Historical data up to the fork point

### Other Migration Types

See:
* [Migrations overview](https://docs.kwil.com/docs/node/migrations)
* [Zero-downtime migrations guide](https://docs.kwil.com/docs/node/migrations/zero-downtime-migrations)

If the release page of your target version says it needs a network migration, **contact the TRUF.NETWORK team before proceeding**.

---

### Still Stuck?

Reach out on Discord or open a GitHub issue – we are happy to help.
