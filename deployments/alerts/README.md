# Node alerts

Two small alerts send Telegram messages from each node. Both read the same settings file and run every 5 minutes from a systemd timer. `tn-node-alert` can also watch the whole network, from a node or from a host that runs no node.

## tn-pg-alert: Postgres runs a program

`tn-pg-alert` sends a Telegram message when a node's Postgres container logs a statement that runs a server program, such as `COPY ... TO PROGRAM`. kwild's database role may not run programs (see `deployments/postgres-roles.sql`), so Postgres refuses the statement and logs it. Any such line means something tried, and it is worth a look.

Each run reads the container's log lines written since the last run that sent its alert. If a send fails, the next run reads the same lines again, so the alert is retried.

## tn-node-alert: the node stops or crashes

`tn-node-alert` sends a message when:

- the node's block height has not changed for 10 minutes (`STALL_MINUTES`), including when its RPC stops answering. The message repeats every hour (`REPEAT_MINUTES`) while the stall lasts, and one more message says when blocks move again.
- Docker restarted the kwild container on its own, which means it crashed. A manual restart or a recreate does not count.
- the kwild container is not running.

These messages go to the Telegram chat and the system journal. With `NODE_ALERTS=journal` they go to the journal only (`journalctl -t tn-node-alert`), and the chat hears nothing about this node. With `NODE_CONTAINER=none` the script skips these checks, for a host that only watches the network.

## tn-node-alert: the network is down

Set `GATEWAY_RPC` to the JSON-RPC address of the network's gateway, and each run also checks the network. It reads the block height from the gateway and from each node in `NODE_RPCS`, and keeps the time each one last showed a new block. A target that does not answer keeps its last height, so its clock keeps running. The network is down when:

- the gateway has shown no new block for `STALL_MINUTES`, or
- every node in `NODE_RPCS` has shown no new block for `STALL_MINUTES`, and the gateway has no block newer than theirs.

The gateway decides: while it serves blocks newer than the listed nodes have, the network counts as up, even if none of those nodes answers.

While the network is down, a message goes out and repeats every `REPEAT_MINUTES`. One more message says when blocks move again and how long they stopped. For example:

```text
TN DOWN: no new block through the gateway for 37 min, last height 3229140 seen at 09:18 UTC. The gateway is not answering. node-1: 3229140 (not answering), node-2: 3229140.
TN DOWN: no node has a new block for 36 min (node-1: 3229140 at 09:18 UTC, node-2: 3229139 at 09:18 UTC). The gateway still answers at height 3229140.
TN OK: blocks are moving again through the gateway, height 3229190, after 41 min.
```

These messages always go to the chat and the journal, whatever `NODE_ALERTS` says. A message that fails to send goes out on the next run. If the chain starts again from a lower height, delete `/var/lib/tn-alert/network-state`, or the network looks down until it passes the old height.

## A chat shared with other teams

When other teams read the chat too, let it hear only about network outages:

1. On one host outside the nodes, install the alerts (see below) and add to its `/etc/tn-alert/telegram.env`:

   ```bash
   GATEWAY_RPC=https://gateway.example.com/rpc/v1
   NODE_RPCS='node-1=http://10.0.0.1:8484/rpc/v1 node-2=http://10.0.0.2:8484/rpc/v1'
   NODE_CONTAINER=none
   STALL_MINUTES=35
   ```

   The host must reach every address in `NODE_RPCS`. It runs no Postgres, so turn its `tn-pg-alert` off: `sudo systemctl disable --now tn-pg-alert.timer`
2. On each node, add `NODE_ALERTS=journal`. `tn-pg-alert` on the node still sends to the chat.

## Install on a node

From a checkout of this repository on the node, or on the host that watches the network:

```bash
cd deployments/alerts
sudo ./install.sh
```

The script installs `/usr/local/bin/tn-pg-alert` and `/usr/local/bin/tn-node-alert`, their services and timers, and an empty settings file `/etc/tn-alert/telegram.env` (mode 600). Until the token is set, alerts go to the system journal only (`journalctl -t tn-pg-alert -t tn-node-alert`).

## Set the Telegram token

1. Create a bot with [@BotFather](https://t.me/BotFather) and copy its token.
2. Add the bot to the chat that should receive alerts and find the chat id, for example from `https://api.telegram.org/bot<token>/getUpdates` after posting a message in the chat.
3. On each node, put both in `/etc/tn-alert/telegram.env`:

   ```bash
   TELEGRAM_BOT_TOKEN=123456:ABC...
   TELEGRAM_CHAT_ID=-1001234567890
   # only for a chat with topics:
   TELEGRAM_TOPIC_ID=42
   ```

4. Check that messages arrive: `sudo tn-pg-alert --test` and `sudo tn-node-alert --test`. The test message goes to the chat even with `NODE_ALERTS=journal`.

No restart is needed. The next timer run reads the file.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | empty | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | empty | Chat that receives the alerts |
| `TELEGRAM_TOPIC_ID` | empty | Topic in a chat with topics, where the alerts go |
| `PG_CONTAINER` | `tn-postgres` | Name of the node's Postgres container |
| `NODE_CONTAINER` | `tn-node` | Name of the node's kwild container, or `none` to skip the checks of this node |
| `NODE_RPC` | `http://127.0.0.1:8484/rpc/v1` | The node's JSON-RPC address |
| `NODE_ALERTS` | `chat` | Where this node's messages go: `chat` (the chat and the journal) or `journal` (the journal only) |
| `NODE_LABEL` | the hostname | Node name shown in the message |
| `GATEWAY_RPC` | empty | The gateway's JSON-RPC address. Set it to watch the network |
| `NODE_RPCS` | empty | Nodes to watch with the gateway, as `label=url` pairs separated by spaces |
| `NETWORK_LABEL` | `TN` | Network name shown in the network messages |
| `STALL_MINUTES` | `10` | Minutes without a new block before the first alert, for the node and for the network |
| `REPEAT_MINUTES` | `60` | Minutes between repeats while a stall or an outage lasts |

## Test

`./node-health-alert.test.sh` runs the script against a local fake RPC server and a fake Telegram. It needs bash, python3 and curl.
