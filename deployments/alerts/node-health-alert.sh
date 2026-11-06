#!/usr/bin/env bash
# Sends a Telegram message when this node stops moving: its block height has
# not changed for STALL_MINUTES, or Docker restarted its kwild container on its
# own (a crash), or the container is not running. A stall alert repeats every
# REPEAT_MINUTES while it lasts, and one message says when blocks move again.
#
# Settings come from /etc/tn-alert/telegram.env, shared with tn-pg-alert:
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  where the message goes
#   TELEGRAM_TOPIC_ID                     topic (thread) in a forum chat, optional
#   NODE_CONTAINER                        kwild container, default tn-node
#   NODE_RPC                              JSON-RPC URL, default http://127.0.0.1:8484/rpc/v1
#   STALL_MINUTES                         default 10
#   REPEAT_MINUTES                        default 60
#   NODE_LABEL                            node name in the message, default the hostname
# Without a token and a chat id, the alert goes to the system journal only.
#
# Usage: tn-node-alert          check the node
#        tn-node-alert --test   send a test message
set -euo pipefail

ENV_FILE=${TN_ALERT_ENV:-/etc/tn-alert/telegram.env}
STATE_DIR=${TN_ALERT_STATE:-/var/lib/tn-alert}
if [ -r "$ENV_FILE" ]; then
	# shellcheck disable=SC1090
	. "$ENV_FILE"
fi
NODE_CONTAINER=${NODE_CONTAINER:-tn-node}
NODE_RPC=${NODE_RPC:-http://127.0.0.1:8484/rpc/v1}
STALL_MINUTES=${STALL_MINUTES:-10}
REPEAT_MINUTES=${REPEAT_MINUTES:-60}
NODE_LABEL=${NODE_LABEL:-$(hostname)}

send() {
	local text=$1 code topic=() logged=0
	logger -t tn-node-alert -- "$text" || logged=1
	if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
		echo "tn-node-alert: no Telegram token or chat id set, wrote the alert to the journal only" >&2
		return "$logged"
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
		echo "tn-node-alert: Telegram answered HTTP $code" >&2
		return 1
	fi
}

if [ "${1:-}" = "--test" ]; then
	send "${NODE_LABEL}: test message from tn-node-alert"
	exit
fi

mkdir -p "$STATE_DIR"
state="$STATE_DIR/node-state"
now=$(date -u +%s)

# Saved state: height, when it last changed, restart count, when the last stall
# alert went out (0 when none is open), and the minutes of a stall whose
# recovery message has not gone out yet (0 when none).
last_height=0 changed_at=$now last_restarts=-1 alerted_at=0 recovery=0
if [ -r "$state" ]; then
	read -r last_height changed_at last_restarts alerted_at recovery <"$state" || true
fi
for v in last_height changed_at last_restarts alerted_at recovery; do
	[[ ${!v} =~ ^-?[0-9]+$ ]] || printf -v "$v" '%s' 0
done
[ "$changed_at" -gt 0 ] || changed_at=$now

failed=0

# Crashes: Docker counts only the restarts its restart policy made, so a
# manual restart or a recreate does not alert. A recreate resets the count.
if info=$(docker inspect -f '{{.RestartCount}} {{.State.Status}} {{.State.ExitCode}}' "$NODE_CONTAINER" 2>/dev/null); then
	read -r restarts status exit_code <<<"$info"
	if [ "$status" != running ]; then
		send "${NODE_LABEL}: ${NODE_CONTAINER} is not running (status ${status}, last exit code ${exit_code})." || failed=1
	elif [ "$last_restarts" -ge 0 ] && [ "$restarts" -gt "$last_restarts" ]; then
		send "${NODE_LABEL}: ${NODE_CONTAINER} restarted by itself $((restarts - last_restarts)) time(s) since the last check (${restarts} in total). Check: docker logs --tail 50 ${NODE_CONTAINER}" || failed=1
	fi
else
	restarts=$last_restarts
	send "${NODE_LABEL}: cannot inspect the container ${NODE_CONTAINER}." || failed=1
fi

# Height: a node that does not answer keeps its last height, so it counts as
# stalled too.
height=$(curl -s -m 10 -X POST "$NODE_RPC" -H 'content-type: application/json' \
	-d '{"jsonrpc":"2.0","id":1,"method":"user.health","params":{}}' 2>/dev/null |
	grep -o '"block_height"[[:space:]]*:[[:space:]]*[0-9]*' | head -1 | grep -o '[0-9]*$') || true
answering=yes
if [ -z "$height" ]; then
	answering=no
	height=$last_height
fi

if [ "$height" -gt "$last_height" ]; then
	if [ "$alerted_at" -gt 0 ]; then
		recovery=$(((now - changed_at) / 60))
		[ "$recovery" -gt 0 ] || recovery=1
	fi
	changed_at=$now
	alerted_at=0
else
	stalled=$(((now - changed_at) / 60))
	if [ "$stalled" -ge "$STALL_MINUTES" ] &&
		{ [ "$alerted_at" -eq 0 ] || [ $(((now - alerted_at) / 60)) -ge "$REPEAT_MINUTES" ]; }; then
		why=""
		[ "$answering" = no ] && why=" The node RPC is not answering."
		if send "${NODE_LABEL}: no new block for ${stalled} min, still at height ${height}.${why}"; then
			alerted_at=$now
		else
			failed=1
		fi
	fi
fi

# A recovery message that fails is kept and sent again on the next run.
if [ "$recovery" -gt 0 ]; then
	if send "${NODE_LABEL}: blocks are moving again, height ${height}, after ${recovery} min without a new block."; then
		recovery=0
	else
		failed=1
	fi
fi

printf '%s %s %s %s %s\n' "$height" "$changed_at" "${restarts:--1}" "$alerted_at" "$recovery" >"$state.tmp"
sync "$state.tmp" 2>/dev/null || true
mv -f "$state.tmp" "$state"
exit "$failed"
