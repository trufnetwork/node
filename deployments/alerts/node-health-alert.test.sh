#!/usr/bin/env bash
# Tests node-health-alert.sh against a local fake: one HTTP server answers
# user.health at /<target> with the height in $tmp/height/<target> (no file: no
# answer) and records each Telegram sendMessage text. docker and logger are
# fakes too. Needs bash, python3 and curl.
#
# Usage: ./node-health-alert.test.sh
set -euo pipefail

script=$(cd "$(dirname "$0")" && pwd)/node-health-alert.sh
tmp=$(mktemp -d)
trap 'kill "$server" 2>/dev/null || true; rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin" "$tmp/height"

exec 3< <(exec python3 -I - "$tmp" <<'EOF'
import http.server, os, sys, urllib.parse
d = sys.argv[1]
class Fake(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def reply(self, code, body):
        self.send_response(code)
        self.send_header('content-length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get('content-length', 0))).decode()
        if self.path.endswith('/sendMessage'):
            if os.path.exists(os.path.join(d, 'telegram-down')):
                return self.reply(502, b'')
            with open(os.path.join(d, 'sent'), 'a') as f:
                f.write(urllib.parse.parse_qs(body)['text'][0] + '\n')
            return self.reply(200, b'{"ok":true}')
        try:
            with open(os.path.join(d, 'height', self.path.strip('/'))) as f:
                height = int(f.read())
        except (OSError, ValueError):
            return self.reply(503, b'')
        self.reply(200, b'{"jsonrpc":"2.0","id":1,"result":{"chain_id":"tn","block_height":%d,"healthy":true}}' % height)
server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Fake)
print(server.server_address[1], flush=True)
server.serve_forever()
EOF
)
server=$!
read -r port <&3
rpc=http://127.0.0.1:$port

cat >"$tmp/bin/docker" <<'EOF'
#!/bin/sh
# docker inspect prints the line in $FAKE_DIR/docker, and fails without it.
cat "$FAKE_DIR/docker"
EOF
cat >"$tmp/bin/logger" <<'EOF'
#!/bin/sh
# logger appends the message, its last argument, to $FAKE_DIR/journal.
for a; do :; done
printf '%s\n' "$a" >>"$FAKE_DIR/journal"
EOF
chmod +x "$tmp/bin/docker" "$tmp/bin/logger"

t0=$(date -u -d '2026-11-24 09:00' +%s)
fails=0 status=0 case_dir="" run_args=()

# new_case SETTING...: a fresh state directory and settings file, all targets silent.
new_case() {
	case_dir=$(mktemp -d "$tmp/case.XXXXXX")
	printf '%s\n' TELEGRAM_BOT_TOKEN=123:abc TELEGRAM_CHAT_ID=-100 NODE_LABEL=node-a "$@" >"$case_dir/env"
	rm -f "$tmp/height/"* "$tmp/telegram-down"
	echo "0 running 0" >"$tmp/docker"
}

# at MINUTE [TARGET=HEIGHT|TARGET=-]...: sets what each target answers (- for
# nothing), then runs the script at that minute after 09:00 UTC.
at() {
	local minute=$1 kv
	shift
	for kv; do
		if [ "${kv#*=}" = - ]; then
			rm -f "$tmp/height/${kv%%=*}"
		else
			echo "${kv#*=}" >"$tmp/height/${kv%%=*}"
		fi
	done
	: >"$tmp/sent"
	: >"$tmp/journal"
	status=0
	PATH="$tmp/bin:$PATH" FAKE_DIR=$tmp TN_ALERT_ENV="$case_dir/env" TN_ALERT_STATE="$case_dir/state" \
		TN_ALERT_NOW=$((t0 + minute * 60)) TELEGRAM_API=$rpc \
		bash "$script" ${run_args[@]+"${run_args[@]}"} >/dev/null 2>&1 || status=$?
}

check() { # check NAME FILE [LINE...]: FILE holds exactly these lines
	local name=$1 file=$2 want=""
	shift 2
	[ $# -eq 0 ] || want=$(printf '%s\n' "$@")
	if [ "$(cat "$file")" = "$want" ]; then
		echo "ok   $name"
	else
		fails=$((fails + 1))
		printf 'FAIL %s\n  want: %s\n  got:  %s\n' "$name" "$want" "$(cat "$file")"
	fi
}
sent() { check "$1" "$tmp/sent" "${@:2}"; }
journal() { check "$1" "$tmp/journal" "${@:2}"; }
exited() { # exited NAME STATUS: the run exited with this status
	if [ "$status" = "$2" ]; then
		echo "ok   $1"
	else
		fails=$((fails + 1))
		echo "FAIL $1: exit status $status, want $2"
	fi
}

echo "# default settings: this node's messages go to the chat, as before"
new_case "NODE_RPC=$rpc/local"
at 0 local=100; sent "first run"; journal "first run, journal"; exited "first run exits 0" 0
at 9; sent "9 min"
at 10; sent "stall at 10 min" "node-a: no new block for 10 min, still at height 100."
journal "stall goes to the journal too" "node-a: no new block for 10 min, still at height 100."
at 15 local=-; sent "no repeat before 60 min"
at 70; sent "repeat" "node-a: no new block for 70 min, still at height 100. The node RPC is not answering."
touch "$tmp/telegram-down"
at 75 local=105; sent "recovery send fails"; exited "failed send exits 1" 1
rm "$tmp/telegram-down"
at 80 local=110; sent "recovery retried" "node-a: blocks are moving again, height 110, after 75 min without a new block."
echo "1 running 0" >"$tmp/docker"
at 85 local=115; sent "crash" "node-a: tn-node restarted by itself 1 time(s) since the last check (1 in total). Check: docker logs --tail 50 tn-node"
echo "1 exited 137" >"$tmp/docker"
at 90 local=120; sent "not running" "node-a: tn-node is not running (status exited, last exit code 137)."
rm "$tmp/docker"
at 95 local=125; sent "cannot inspect" "node-a: cannot inspect the container tn-node."
run_args=(--test)
at 96; sent "--test" "node-a: test message from tn-node-alert"
run_args=()

echo "# NODE_ALERTS=journal: this node's messages go to the journal only"
new_case "NODE_RPC=$rpc/local" NODE_ALERTS=journal
at 0 local=100
at 10; sent "stall: nothing to the chat"; journal "stall in the journal" "node-a: no new block for 10 min, still at height 100."
echo "1 exited 1" >"$tmp/docker"
at 15 local=101; sent "crash and recovery: nothing to the chat"; exited "exits 0" 0
journal "crash and recovery in the journal" "node-a: tn-node is not running (status exited, last exit code 1)." \
	"node-a: blocks are moving again, height 101, after 15 min without a new block."
new_case "NODE_RPC=$rpc/local" NODE_ALERTS=jornal
at 0 local=100; exited "unknown NODE_ALERTS exits 2" 2

net=("GATEWAY_RPC=$rpc/gw" "NODE_RPCS='node-1=$rpc/n1 node-2=$rpc/n2'" NODE_CONTAINER=none STALL_MINUTES=35)

echo "# network: one node stalled 40 min while the gateway moves"
new_case "${net[@]}"
for m in 0 10 20 30 35 40; do
	at "$m" gw=$((1000 + m)) n1=990 n2=$((1000 + m))
	sent "$m min: nothing to the chat"; journal "$m min: nothing in the journal"; exited "$m min: exits 0" 0
done

echo "# network: no node answers this host for 40 min, the gateway moves"
new_case "${net[@]}"
for m in 0 10 20 30 35 40; do
	at "$m" gw=$((1000 + m)) n1=- n2=-; sent "$m min: nothing to the chat"
done

echo "# network: every node stalled 36 min, the gateway moves ahead of them"
new_case "${net[@]}"
at 0 gw=1000 n1=1000 n2=1000
for m in 10 20 30 36 40; do
	at "$m" gw=$((1000 + m)); sent "$m min: nothing to the chat"
done

echo "# network: the gateway stops answering"
new_case "${net[@]}"
at 0 gw=3229140 n1=3229140 n2=3229140
at 5 gw=- n1=-
at 30; sent "30 min: nothing"
at 34; sent "34 min: nothing"
at 35; sent "35 min: DOWN" "TN DOWN: no new block through the gateway for 35 min, last height 3229140 seen at 09:00 UTC. The gateway is not answering. node-1: 3229140 (not answering), node-2: 3229140."
at 40; sent "next run: nothing"
at 94; sent "94 min: nothing"
at 95; sent "repeat after 60 min" "TN DOWN: no new block through the gateway for 95 min, last height 3229140 seen at 09:00 UTC. The gateway is not answering. node-1: 3229140 (not answering), node-2: 3229140."
at 100 gw=3229190; sent "OK" "TN OK: blocks are moving again through the gateway, height 3229190, after 100 min."
at 105 gw=3229195; sent "OK only once"

echo "# network: every node stalled 36 min, the gateway answers at their height"
new_case "${net[@]}"
at 0 gw=3229139 n1=3229140 n2=3229139
at 10 gw=3229140
at 34; sent "34 min: nothing"
at 36; sent "36 min: DOWN" "TN DOWN: no node has a new block for 36 min (node-1: 3229140 at 09:00 UTC, node-2: 3229139 at 09:00 UTC). The gateway still answers at height 3229140."
at 41 gw=3229141; sent "the gateway gets ahead: OK" "TN OK: blocks are moving again through the gateway, height 3229141, after 41 min."

echo "# network: failed sends are retried"
new_case "${net[@]}"
at 0 gw=500 n1=500 n2=500
touch "$tmp/telegram-down"
at 35; sent "DOWN send fails"; exited "exits 1" 1
rm "$tmp/telegram-down"
at 40; sent "DOWN retried" "TN DOWN: no new block through the gateway for 40 min, last height 500 seen at 09:00 UTC. node-1: 500, node-2: 500."
touch "$tmp/telegram-down"
at 45 gw=501; sent "OK send fails"; exited "exits 1" 1
rm "$tmp/telegram-down"
at 50 gw=502; sent "OK retried" "TN OK: blocks are moving again through the gateway, height 502, after 45 min."
at 55 gw=503; sent "then nothing"

echo "# network: the clock steps back during an outage"
new_case "${net[@]}"
at 0 gw=700 n1=700 n2=700
at 1 gw=-
at 35; sent "DOWN" "TN DOWN: no new block through the gateway for 35 min, last height 700 seen at 09:00 UTC. The gateway is not answering. node-1: 700, node-2: 700."
at 10; sent "clock 25 min back: no OK"
at 36; sent "clock back on time: nothing"
at 50 gw=701; sent "real recovery: OK" "TN OK: blocks are moving again through the gateway, height 701, after 50 min."

echo "# network watch with a node check on the same host"
new_case "${net[@]}" NODE_CONTAINER=tn-node "NODE_RPC=$rpc/local" NODE_ALERTS=journal
at 0 gw=10 n1=10 n2=10 local=10
at 35; sent "network DOWN goes to the chat" "TN DOWN: no new block through the gateway for 35 min, last height 10 seen at 09:00 UTC. node-1: 10, node-2: 10."
journal "both in the journal" "TN DOWN: no new block through the gateway for 35 min, last height 10 seen at 09:00 UTC. node-1: 10, node-2: 10." \
	"node-a: no new block for 35 min, still at height 10."

[ "$fails" -eq 0 ] && echo "all passed" || echo "$fails failed"
[ "$fails" -eq 0 ]
