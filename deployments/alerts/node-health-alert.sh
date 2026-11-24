#!/usr/bin/env bash
# Watches this node and, when GATEWAY_RPC is set, the whole network.
#
# This node: a message when its block height has not changed for STALL_MINUTES,
# when Docker restarted its kwild container on its own (a crash), or when the
# container is not running. A stall message repeats every REPEAT_MINUTES while
# it lasts, and one message says when blocks move again. These messages go to
# Telegram, or to the system journal only with NODE_ALERTS=journal.
#
# The network: a message when it is down, meaning the gateway has shown no new
# block for STALL_MINUTES, or every node in NODE_RPCS has shown none for
# STALL_MINUTES and the gateway has no block newer than theirs. It repeats every
# REPEAT_MINUTES while it lasts, and one message says when blocks move again.
# These messages always go to Telegram.
#
# Settings come from /etc/tn-alert/telegram.env, shared with tn-pg-alert:
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  where the message goes
#   TELEGRAM_TOPIC_ID                     topic (thread) in a forum chat, optional
#   NODE_CONTAINER                        kwild container, default tn-node; none skips this node
#   NODE_RPC                              JSON-RPC URL, default http://127.0.0.1:8484/rpc/v1
#   NODE_ALERTS                           where this node's messages go: chat (default) or journal
#   NODE_LABEL                            node name in the message, default the hostname
#   GATEWAY_RPC                           gateway JSON-RPC URL; set it to watch the network
#   NODE_RPCS                             nodes to watch with it, label=url pairs split by spaces
#   NETWORK_LABEL                         network name in the message, default TN
#   STALL_MINUTES                         default 10
#   REPEAT_MINUTES                        default 60
# Without a token and a chat id, every message goes to the system journal only.
#
# Usage: tn-node-alert          check the node and the network
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
NODE_ALERTS=${NODE_ALERTS:-chat}
STALL_MINUTES=${STALL_MINUTES:-10}
REPEAT_MINUTES=${REPEAT_MINUTES:-60}
NODE_LABEL=${NODE_LABEL:-$(hostname)}
GATEWAY_RPC=${GATEWAY_RPC:-}
NODE_RPCS=${NODE_RPCS:-}
NETWORK_LABEL=${NETWORK_LABEL:-TN}
# For tests: another Telegram server, and a fixed time (TN_ALERT_NOW, Unix seconds).
TELEGRAM_API=${TELEGRAM_API:-https://api.telegram.org}
if [ "$NODE_ALERTS" != chat ] && [ "$NODE_ALERTS" != journal ]; then
	echo "tn-node-alert: NODE_ALERTS must be chat or journal, not ${NODE_ALERTS}" >&2
	exit 2
fi

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
	code=$(printf 'url = "%s/bot%s/sendMessage"\n' "$TELEGRAM_API" "$TELEGRAM_BOT_TOKEN" |
		curl -sS -m 20 -o /dev/null -w '%{http_code}' -K - \
			--data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" ${topic[@]+"${topic[@]}"} \
			--data-urlencode "text=${text}") || code=000
	if [ "$code" != 200 ]; then
		echo "tn-node-alert: Telegram answered HTTP $code" >&2
		return 1
	fi
}

# Messages about this node alone: to the chat, or with NODE_ALERTS=journal to
# the journal only.
node_send() {
	if [ "$NODE_ALERTS" = journal ]; then
		logger -t tn-node-alert -- "$1"
	else
		send "$1"
	fi
}

# Prints the block height that user.health reports at the JSON-RPC URL $1,
# or nothing when it does not answer.
rpc_height() {
	curl -s -m 10 -X POST "$1" -H 'content-type: application/json' \
		-d '{"jsonrpc":"2.0","id":1,"method":"user.health","params":{}}' 2>/dev/null |
		grep -o '"block_height"[[:space:]]*:[[:space:]]*[0-9]*' | head -1 | grep -o '[0-9]*$' || true
}

hhmm() { date -u -d "@$1" +%H:%M; }

if [ "${1:-}" = "--test" ]; then
	send "${NODE_LABEL}: test message from tn-node-alert"
	exit
fi

mkdir -p "$STATE_DIR"
now=${TN_ALERT_NOW:-$(date -u +%s)}
failed=0

# probe URL: reads a network target and sets h (the highest height it has
# shown), at (when that height first showed) and up (no when it did not
# answer). A target that stops answering keeps its clock running.
probe() {
	local got
	got=$(rpc_height "$1")
	h=${seen_h[$1]:-0} at=${seen_at[$1]:-$now} up=yes
	if [ -z "$got" ]; then
		up=no
	elif [ "$got" -gt "$h" ]; then
		h=$got at=$now
	fi
	seen_h[$1]=$h seen_at[$1]=$at
}

# Saved state: when the last network alert went out (0 when none is open), when
# the last new block came before that alert, the minutes of an outage whose
# recovery message has not gone out yet (0 when none), then one line per
# target: its URL, highest height and when that height first showed.
check_network() {
	local file=$STATE_DIR/network-state alerted_at=0 last_block=0 recovery=0
	local url h at up v label spec stalled since down="" msg why best=0
	local gw_h gw_at gw_up node_h=0 node_at=0 count=0 failing=0 gw_list="" node_list=""
	local -a nodes=() urls=()
	local -A seen_h=() seen_at=()
	if [ -r "$file" ]; then
		{
			read -r alerted_at last_block recovery || true
			while read -r url h at; do
				if [[ $h =~ ^[0-9]+$ && $at =~ ^[0-9]+$ ]]; then
					seen_h[$url]=$h seen_at[$url]=$at
				fi
			done
		} <"$file"
	fi
	for v in alerted_at last_block recovery; do
		[[ ${!v} =~ ^[0-9]+$ ]] || printf -v "$v" '%s' 0
	done
	# The highest block any target showed before this run. An open outage
	# closes only on a block above it, so a new or replaced target that shows
	# an old height does not count as the network moving.
	for h in ${seen_h[@]+"${seen_h[@]}"}; do
		[ "$h" -le "$best" ] || best=$h
	done

	probe "$GATEWAY_RPC"
	gw_h=$h gw_at=$at gw_up=$up
	urls=("$GATEWAY_RPC")
	read -r -d '' -a nodes <<<"$NODE_RPCS" || true
	for spec in ${nodes[@]+"${nodes[@]}"}; do
		label=${spec%%=*} url=${spec#*=}
		probe "$url"
		urls+=("$url")
		count=$((count + 1))
		[ $(((now - at) / 60)) -lt "$STALL_MINUTES" ] || failing=$((failing + 1))
		[ "$h" -le "$node_h" ] || node_h=$h
		[ "$at" -le "$node_at" ] || node_at=$at
		why=""
		[ "$up" = yes ] || why=" (not answering)"
		if [ "$h" -gt 0 ]; then
			gw_list+="${gw_list:+, }${label}: ${h}${why}"
			node_list+="${node_list:+, }${label}: ${h} at $(hhmm "$at") UTC${why}"
		else
			gw_list+="${gw_list:+, }${label}: no answer yet"
			node_list+="${node_list:+, }${label}: no answer yet"
		fi
	done

	# The gateway decides: a block newer than every listed node's means the
	# network moves, even when none of those nodes answers.
	if [ $(((now - gw_at) / 60)) -ge "$STALL_MINUTES" ]; then
		down=gateway since=$gw_at
		msg="${NETWORK_LABEL} DOWN: no new block through the gateway for $(((now - since) / 60)) min, "
		if [ "$gw_h" -gt 0 ]; then
			msg+="last height ${gw_h} seen at $(hhmm "$gw_at") UTC."
		else
			msg+="no height seen yet."
		fi
		[ "$gw_up" = yes ] || msg+=" The gateway is not answering."
		[ -z "$gw_list" ] || msg+=" ${gw_list}."
	elif [ "$count" -gt 0 ] && [ "$failing" -eq "$count" ] && [ "$gw_h" -le "$node_h" ]; then
		down=nodes since=$node_at
		msg="${NETWORK_LABEL} DOWN: no node has a new block for $(((now - since) / 60)) min (${node_list})."
		if [ "$gw_up" = yes ]; then
			msg+=" The gateway still answers at height ${gw_h}."
		else
			msg+=" The gateway is not answering."
		fi
	fi

	if [ -n "$down" ]; then
		if [ "$alerted_at" -eq 0 ] || [ $(((now - alerted_at) / 60)) -ge "$REPEAT_MINUTES" ]; then
			if send "$msg"; then
				[ "$alerted_at" -gt 0 ] || last_block=$since recovery=0
				alerted_at=$now
			else
				failed=1
			fi
		fi
	else
		# Ages alone can look fresh without a new block (a replaced gateway
		# address, a clock that stepped back), so an open outage closes only
		# when some target shows a block above the best one seen before.
		if [ "$alerted_at" -gt 0 ] && { [ "$gw_h" -gt "$best" ] || [ "$node_h" -gt "$best" ]; }; then
			stalled=$(((now - last_block) / 60))
			recovery=$((stalled > 0 ? stalled : 1))
			alerted_at=0
		fi
		# A recovery message that fails is kept and sent again on the next run.
		if [ "$recovery" -gt 0 ]; then
			if [ "$gw_up" = yes ]; then
				msg="${NETWORK_LABEL} OK: blocks are moving again through the gateway, height ${gw_h}, after ${recovery} min."
			else
				msg="${NETWORK_LABEL} OK: blocks are moving again, height $((gw_h > node_h ? gw_h : node_h)), after ${recovery} min. The gateway is not answering."
			fi
			if send "$msg"; then
				recovery=0
			else
				failed=1
			fi
		fi
	fi

	{
		printf '%s %s %s\n' "$alerted_at" "$last_block" "$recovery"
		for url in "${urls[@]}"; do
			printf '%s %s %s\n' "$url" "${seen_h[$url]}" "${seen_at[$url]}"
		done
	} >"$file.tmp"
	sync "$file.tmp" 2>/dev/null || true
	mv -f "$file.tmp" "$file"
}

if [ -n "$GATEWAY_RPC" ]; then
	check_network
fi
if [ "$NODE_CONTAINER" = none ]; then
	exit "$failed"
fi

state="$STATE_DIR/node-state"

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

# Crashes: Docker counts only the restarts its restart policy made, so a
# manual restart or a recreate does not alert. A recreate resets the count.
if info=$(docker inspect -f '{{.RestartCount}} {{.State.Status}} {{.State.ExitCode}}' "$NODE_CONTAINER" 2>/dev/null); then
	read -r restarts status exit_code <<<"$info"
	if [ "$status" != running ]; then
		node_send "${NODE_LABEL}: ${NODE_CONTAINER} is not running (status ${status}, last exit code ${exit_code})." || failed=1
	elif [ "$last_restarts" -ge 0 ] && [ "$restarts" -gt "$last_restarts" ]; then
		node_send "${NODE_LABEL}: ${NODE_CONTAINER} restarted by itself $((restarts - last_restarts)) time(s) since the last check (${restarts} in total). Check: docker logs --tail 50 ${NODE_CONTAINER}" || failed=1
	fi
else
	restarts=$last_restarts
	node_send "${NODE_LABEL}: cannot inspect the container ${NODE_CONTAINER}." || failed=1
fi

# Height: a node that does not answer keeps its last height, so it counts as
# stalled too.
height=$(rpc_height "$NODE_RPC")
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
		if node_send "${NODE_LABEL}: no new block for ${stalled} min, still at height ${height}.${why}"; then
			alerted_at=$now
		else
			failed=1
		fi
	fi
fi

# A recovery message that fails is kept and sent again on the next run.
if [ "$recovery" -gt 0 ]; then
	if node_send "${NODE_LABEL}: blocks are moving again, height ${height}, after ${recovery} min without a new block."; then
		recovery=0
	else
		failed=1
	fi
fi

printf '%s %s %s %s %s\n' "$height" "$changed_at" "${restarts:--1}" "$alerted_at" "$recovery" >"$state.tmp"
sync "$state.tmp" 2>/dev/null || true
mv -f "$state.tmp" "$state"
exit "$failed"
