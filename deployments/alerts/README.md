# Node alerts

Two small alerts send Telegram messages from each node. Both read the same settings file and run every 5 minutes from a systemd timer.

## tn-pg-alert: Postgres runs a program

`tn-pg-alert` sends a Telegram message when a node's Postgres container logs a statement that runs a server program, such as `COPY ... TO PROGRAM`. kwild's database role may not run programs (see `deployments/postgres-roles.sql`), so Postgres refuses the statement and logs it. Any such line means something tried, and it is worth a look.

Each run reads the container's log lines written since the previous run, so nothing is counted twice.

## tn-node-alert: the node stops or crashes

`tn-node-alert` sends a message when:

- the node's block height has not changed for 10 minutes (`STALL_MINUTES`), including when its RPC stops answering. The message repeats every hour (`REPEAT_MINUTES`) while the stall lasts, and one more message says when blocks move again.
- Docker restarted the kwild container on its own, which means it crashed. A manual restart or a recreate does not count.
- the kwild container is not running.

## Install on a node

From a checkout of this repository on the node:

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

4. Check that messages arrive: `sudo tn-pg-alert --test` and `sudo tn-node-alert --test`

No restart is needed. The next timer run reads the file.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | empty | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | empty | Chat that receives the alerts |
| `TELEGRAM_TOPIC_ID` | empty | Topic in a chat with topics, where the alerts go |
| `PG_CONTAINER` | `tn-postgres` | Name of the node's Postgres container |
| `NODE_CONTAINER` | `tn-node` | Name of the node's kwild container |
| `NODE_RPC` | `http://127.0.0.1:8484/rpc/v1` | The node's JSON-RPC address |
| `STALL_MINUTES` | `10` | Minutes without a new block before the first alert |
| `REPEAT_MINUTES` | `60` | Minutes between repeats while a stall lasts |
| `NODE_LABEL` | the hostname | Node name shown in the message |
