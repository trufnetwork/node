#!/usr/bin/env bash
# Installs tn-pg-alert on a node. Run it as root from this directory:
#   sudo ./install.sh
# It is safe to run again and keeps an existing /etc/tn-alert/telegram.env.
set -euo pipefail
cd "$(dirname "$0")"

install -m 755 pg-program-alert.sh /usr/local/bin/tn-pg-alert
install -m 644 tn-pg-alert.service tn-pg-alert.timer /etc/systemd/system/
install -d -m 700 /etc/tn-alert
if [ ! -e /etc/tn-alert/telegram.env ]; then
	install -m 600 /dev/null /etc/tn-alert/telegram.env
	printf '%s\n' 'TELEGRAM_BOT_TOKEN=' 'TELEGRAM_CHAT_ID=' '# PG_CONTAINER=tn-postgres' '# NODE_LABEL=' >/etc/tn-alert/telegram.env
fi
systemctl daemon-reload
systemctl enable --now tn-pg-alert.timer
echo "tn-pg-alert installed. Put the bot token and chat id in /etc/tn-alert/telegram.env, then run: sudo tn-pg-alert --test"
