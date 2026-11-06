#!/usr/bin/env bash
# Sends a Telegram message when the node's Postgres container logs a statement
# that runs a server program (COPY ... TO PROGRAM or FROM PROGRAM). kwild's
# database role may not run programs, so Postgres refuses such a statement and
# logs it. Any such line means something tried.
#
# Settings come from /etc/tn-alert/telegram.env (see README.md):
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  where the message goes
#   TELEGRAM_TOPIC_ID                     topic (thread) in a forum chat, optional
#   PG_CONTAINER                          Postgres container, default tn-postgres
#   NODE_LABEL                            node name in the message, default the hostname
# Without a token and a chat id, the alert goes to the system journal only.
#
# Usage: tn-pg-alert          check the logs written since the last run
#        tn-pg-alert --test   send a test message
set -euo pipefail

ENV_FILE=${TN_ALERT_ENV:-/etc/tn-alert/telegram.env}
STATE_DIR=${TN_ALERT_STATE:-/var/lib/tn-alert}
if [ -r "$ENV_FILE" ]; then
	# shellcheck disable=SC1090
	. "$ENV_FILE"
fi
PG_CONTAINER=${PG_CONTAINER:-tn-postgres}
NODE_LABEL=${NODE_LABEL:-$(hostname)}

send() {
	local text=$1 code topic=()
	logger -t tn-pg-alert -- "$text" || true
	if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
		echo "tn-pg-alert: no Telegram token or chat id set, wrote the alert to the journal only" >&2
		return 0
	fi
	if [ -n "${TELEGRAM_TOPIC_ID:-}" ]; then
		topic=(--data-urlencode "message_thread_id=${TELEGRAM_TOPIC_ID}")
	fi
	# The URL holds the token, so it goes through stdin, not the command line.
	code=$(printf 'url = "https://api.telegram.org/bot%s/sendMessage"\n' "$TELEGRAM_BOT_TOKEN" |
		curl -sS -m 20 -o /dev/null -w '%{http_code}' -K - \
			--data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" ${topic[@]+"${topic[@]}"} \
			--data-urlencode "text=${text}") || code=000
	if [ "$code" != 200 ]; then
		echo "tn-pg-alert: Telegram answered HTTP $code" >&2
		return 1
	fi
}

if [ "${1:-}" = "--test" ]; then
	send "${NODE_LABEL}: test message from tn-pg-alert"
	exit
fi

mkdir -p "$STATE_DIR"
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
since=$(cat "$STATE_DIR/last-run" 2>/dev/null || date -u -d '-10 min' +%Y-%m-%dT%H:%M:%SZ)

if ! logs=$(docker logs --since "$since" --until "$now" "$PG_CONTAINER" 2>&1); then
	send "${NODE_LABEL}: tn-pg-alert cannot read the logs of ${PG_CONTAINER}"
	exit 1
fi

count=$(printf '%s\n' "$logs" | grep -c -i -E '(to|from)[[:space:]]+program' || true)
if [ "$count" -gt 0 ]; then
	# The last run time only moves once the message is out, so a failed send
	# is retried on the next run.
	send "${NODE_LABEL}: Postgres logged ${count} line(s) with TO PROGRAM between ${since} and ${now}. Read them with: docker logs --since ${since} --until ${now} ${PG_CONTAINER} 2>&1 | grep -i program"
fi
echo "$now" >"$STATE_DIR/last-run"
