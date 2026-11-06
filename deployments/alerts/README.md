# Postgres program alert

`tn-pg-alert` sends a Telegram message when a node's Postgres container logs a statement that runs a server program, such as `COPY ... TO PROGRAM`. kwild's database role may not run programs (see `deployments/postgres-roles.sql`), so Postgres refuses the statement and logs it. Any such line means something tried, and it is worth a look.

A systemd timer runs the check every 5 minutes. Each run reads the container's log lines written since the previous run, so nothing is counted twice.

## Install on a node

From a checkout of this repository on the node:

```bash
cd deployments/alerts
sudo ./install.sh
```

The script installs `/usr/local/bin/tn-pg-alert`, the `tn-pg-alert` service and timer, and an empty settings file `/etc/tn-alert/telegram.env` (mode 600). Until the token is set, alerts go to the system journal only (`journalctl -t tn-pg-alert`).

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

4. Check that a message arrives: `sudo tn-pg-alert --test`

No restart is needed. The next timer run reads the file.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | empty | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | empty | Chat that receives the alerts |
| `TELEGRAM_TOPIC_ID` | empty | Topic in a chat with topics, where the alerts go |
| `PG_CONTAINER` | `tn-postgres` | Name of the node's Postgres container |
| `NODE_LABEL` | the hostname | Node name shown in the message |
