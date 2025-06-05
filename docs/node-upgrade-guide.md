# Node Upgrade

Keeping your node up-to-date is crucial for maintaining network security and performance. Follow this guide to upgrade your node in the TRUF.NETWORK.

## 1. Backup Existing Data

- **Node Data Directory**: Backup the directory containing `config.toml` and `private_key`.
- **PostgreSQL Database**: Backup the `kwild` database to prevent data loss during the upgrade process.

## 2. Stop Running Services

- **Node Service**: Stop the `tsn` or `kwild` service.
- **PostgreSQL Container**: Stop the PostgreSQL container to prepare for the upgrade.

## 3. Update PostgreSQL Container

- **New Container**: Create a new PostgreSQL container using the latest Docker image from Kwil: `kwildb/postgres:16.5-1`.
- **Shared Memory Size**: Set the shared memory size to `2g` using the `--shm-size` parameter. Adjust this value if necessary.

_Note: An existing container cannot be updated; you must stop the existing container and create a new one._

## 4. Remove Specific Directories

From the `tsn` data directory, remove the following folders:

- `abci`
- `signing`
- `rcvdSnaps` (if existing)
- `snapshots` (if existing)

## 5. Configure State Sync

In the `config.toml` file located in the `tsn` directory:

- **Enable State Sync**: Set `enable = true` in the `[chain.statesync]` section.
- **Snapshot Directory**: Set `snapshot_dir` to the appropriate path, e.g., `/data/tsn-root/rcvdSnaps`.
- **RPC Servers**: Set `rpc_servers` to the following:

```
http://staging.node-1.tsn.truflation.com:26657,http://3.92.83.167:26657
```

## 6. Start Services

- **Node Service**: Start the `tsn` or `kwild` service.
- **PostgreSQL Container**: Start the new PostgreSQL container.

## 7. Verify State Sync

Monitor the logs to ensure state sync is functioning correctly. Look for messages indicating:

- RPC provider used for snapshot header verification.
- State sync enabled.
- Discovery and acceptance of a new snapshot.
- Retrieval and restoration of snapshot data.
- Completion of the database restore process.

## 8. Confirm Synchronization

After synchronization, confirm that the node is caught up with the network without errors. Ignore messages about received transactions with invalid nonces, as these may originate from out-of-sync nodes or clients.

## 9. Verify Application Hash

Use the following command to verify the application hash:

```bash
curl -s http://127.0.0.1:26657/abci_info
```

Ensure the `last_block_app_hash` matches the expected value.

For detailed information and advanced configurations, refer to the [TRUF.NETWORK Whitepaper](https://whitepaper.truflation.com/).
