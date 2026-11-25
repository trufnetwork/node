#!/usr/bin/env bash
# Tests scripts/release/announce-telegram.sh: the message for each form of the
# upgrade line, block separators, HTML escaping, refused bodies and arguments,
# a missing token or chat id, and sends to a local fake Telegram that records
# each request. A curl wrapper records curl's command line, which must never
# hold the token. Needs bash, python3 and curl.
#
# Usage: scripts/release/test-announce-telegram.sh
set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
announce=$dir/announce-telegram.sh
guide=$dir/../../docs/node-upgrade-guide.md
tmp=$(mktemp -d)
server=""
trap '[ -z "$server" ] || kill "$server" 2>/dev/null || true; rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin" "$tmp/req"

# Every run below goes to the fake server, never to Telegram.
unset TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID TELEGRAM_TOPIC_ID TELEGRAM_API GITHUB_ACTIONS

# The fake Telegram: each POST becomes $tmp/req/<n>/, with the request path in
# "path", the sorted field names in "fields" and each field's value in a file of
# its name. With $tmp/telegram-fail present it refuses the message as Telegram
# does.
exec 3< <(exec python3 -I - "$tmp" <<'EOF'
import http.server, os, sys, urllib.parse
d = sys.argv[1]
class Fake(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def reply(self, code, body):
        self.send_response(code)
        self.send_header('content-type', 'application/json')
        self.send_header('content-length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get('content-length', 0))).decode()
        fields = urllib.parse.parse_qs(body, keep_blank_values=True)
        req = os.path.join(d, 'req', str(len(os.listdir(os.path.join(d, 'req'))) + 1))
        os.mkdir(req)
        with open(os.path.join(req, 'path'), 'w') as f:
            f.write(self.path)
        with open(os.path.join(req, 'fields'), 'w') as f:
            f.write(' '.join(sorted(fields)))
        for name, values in fields.items():
            if name.isidentifier():
                with open(os.path.join(req, name), 'wb') as f:
                    f.write(values[0].encode())
        if os.path.exists(os.path.join(d, 'telegram-fail')):
            return self.reply(400, b'{"ok":false,"error_code":400,"description":"Bad Request: message thread not found"}')
        self.reply(200, b'{"ok":true,"result":{"message_id":7}}')
server = http.server.HTTPServer(('127.0.0.1', 0), Fake)
print(server.server_address[1], flush=True)
server.serve_forever()
EOF
)
server=$!
read -r port <&3
api=http://127.0.0.1:$port

real_curl=$(command -v curl)
cat >"$tmp/bin/curl" <<EOF
#!/bin/sh
# Records curl's arguments, one per line, then runs the real curl.
printf '%s\n' "\$@" >>"$tmp/argv"
exec "$real_curl" "\$@"
EOF
chmod +x "$tmp/bin/curl"

token=123456:fake-token-for-tests
chat=-1001234
url=https://github.com/trufnetwork/node/releases/tag/v2.6.9
notes=$'\n## What\'s Changed\n* chore: an example change by @someone in https://github.com/trufnetwork/node/pull/1\n'
both='Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).'
mainnet='Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), optional on testnet.'
testnet='Upgrade: optional on mainnet, required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).'

want_both=$(cat <<'EOF'
REQUIRED: TN node v2.6.9

Install it before:
- mainnet block 3,300,000 (about 2026-12-10 12:00 UTC)
- testnet block 16,300,000 (about 2026-12-03 12:00 UTC)

A node still on an older release stops at that block. After upgrading, <code>kwild version</code> prints 2.6.9.

Release: https://github.com/trufnetwork/node/releases/tag/v2.6.9
How to upgrade: https://github.com/trufnetwork/node/blob/main/docs/node-upgrade-guide.md#required-in-place-upgrade
EOF
)
want_mainnet=$(cat <<'EOF'
REQUIRED on mainnet: TN node v2.6.9

Install it before mainnet block 3,300,000 (about 2026-12-10 12:00 UTC). On testnet it is optional.

A node still on an older release stops at that block. After upgrading, <code>kwild version</code> prints 2.6.9.

Release: https://github.com/trufnetwork/node/releases/tag/v2.6.9
How to upgrade: https://github.com/trufnetwork/node/blob/main/docs/node-upgrade-guide.md#required-in-place-upgrade
EOF
)
want_testnet=$(cat <<'EOF'
REQUIRED on testnet: TN node v2.6.9

Install it before testnet block 16,300,000 (about 2026-12-03 12:00 UTC). On mainnet it is optional.

A node still on an older release stops at that block. After upgrading, <code>kwild version</code> prints 2.6.9.

Release: https://github.com/trufnetwork/node/releases/tag/v2.6.9
How to upgrade: https://github.com/trufnetwork/node/blob/main/docs/node-upgrade-guide.md#required-in-place-upgrade
EOF
)
want_optional=$(cat <<'EOF'
TN node v2.6.9 is out. Optional: nodes on older releases keep following the chain, so upgrade when it suits you.

Release: https://github.com/trufnetwork/node/releases/tag/v2.6.9
EOF
)

fails=0
settings=()
pass() { echo "ok   $1"; }
fail() {
	echo "FAIL $1"
	fails=$((fails + 1))
}

# is NAME GOT WANT
is() {
	if [ "$2" = "$3" ]; then
		pass "$1"
	else
		fail "$1"
		diff <(printf '%s\n' "$3") <(printf '%s\n' "$2") | sed 's/^/     /' || true
	fi
}

# has NAME TEXT PART
has() {
	if [[ $2 == *"$3"* ]]; then
		pass "$1"
	else
		fail "$1: no '$3' in: $2"
	fi
}

# run BODY ARG...: runs the script with BODY on stdin, the settings in
# $settings and the fake Telegram; sets status, out and err. Clears the requests
# of the run before.
run() {
	rm -rf "${tmp:?}/req/"*
	printf '%s' "$1" >"$tmp/body"
	shift
	status=0
	env TELEGRAM_API="$api" PATH="$tmp/bin:$PATH" ${settings[@]+"${settings[@]}"} \
		"$announce" "$@" <"$tmp/body" >"$tmp/out" 2>"$tmp/err" || status=$?
	out=$(<"$tmp/out")
	err=$(<"$tmp/err")
	cat "$tmp/out" "$tmp/err" >>"$tmp/all-output"
}

sent() { ls "$tmp/req"; }

# message NAME WANT BODY: the dry run prints WANT and sends nothing.
message() {
	settings=(TELEGRAM_BOT_TOKEN="$token" TELEGRAM_CHAT_ID="$chat")
	run "$3" --dry-run v2.6.9 "$url"
	is "$1: exit" "$status" 0
	is "$1: message" "$out" "$2"
	is "$1: nothing sent" "$(sent)" ""
}

message 'required on both networks' "$want_both" "$both$notes"
message 'required on mainnet only' "$want_mainnet" "$mainnet$notes"
message 'required on testnet only' "$want_testnet" "$testnet$notes"
message 'optional' "$want_optional" "Upgrade: optional.$notes"
message 'optional, line alone' "$want_optional" 'Upgrade: optional.'
message 'required on both, CRLF line ends' "$want_both" "$both"$'\r\n## What\'s Changed\r\n'

# Thousands separators, past what bash arithmetic holds.
settings=()
for pair in 1=1 999=999 1000=1,000 12345=12,345 999999=999,999 1000000=1,000,000 \
	3300000=3,300,000 16300000=16,300,000 12345678901234567890=12,345,678,901,234,567,890; do
	run "Upgrade: required on mainnet from block ${pair%%=*} (about 2026-12-10 12:00 UTC), optional on testnet." --dry-run v2.6.9 "$url"
	has "block ${pair%%=*} prints as ${pair#*=}" "$out" "Install it before mainnet block ${pair#*=} (about 2026-12-10 12:00 UTC)."
done
run "Upgrade: optional on mainnet, required on testnet from block 1000 (about 2026-12-03 12:00 UTC)." --dry-run v2.6.9 "$url"
has 'testnet block 1000 prints as 1,000' "$out" "Install it before testnet block 1,000 (about 2026-12-03 12:00 UTC)."

# The tag and the release URL are escaped for Telegram's HTML parse mode.
run "$mainnet" --dry-run 'v2.6.9<b>&x' 'https://example.test/r?a=1&b=<2>'
has 'tag escaped in the title' "$out" 'REQUIRED on mainnet: TN node v2.6.9&lt;b&gt;&amp;x'
has 'version escaped' "$out" '<code>kwild version</code> prints 2.6.9&lt;b&gt;&amp;x.'
has 'release URL escaped' "$out" 'Release: https://example.test/r?a=1&amp;b=&lt;2&gt;'
run 'Upgrade: optional.' --dry-run 'v1&2' "$url"
has 'tag escaped in the optional form' "$out" 'TN node v1&amp;2 is out.'
run 'Upgrade: optional.' --dry-run 2.6.9 "$url"
has 'a tag without v' "$out" 'TN node 2.6.9 is out.'

# The upgrade link points at a heading that exists in the guide, as GitHub
# names its anchors.
anchor=${want_both##*#}
anchors=$(sed -n 's/^#\{1,6\} //p' "$guide" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9 _-]//g; s/ /-/g')
if grep -qxF -- "$anchor" <<<"$anchors"; then
	pass "the guide has the heading #$anchor"
else
	fail "no heading in docs/node-upgrade-guide.md has the anchor #$anchor"
fi

# A body that check-upgrade-line.sh refuses is refused too, and nothing is sent.
settings=(TELEGRAM_BOT_TOKEN="$token" TELEGRAM_CHAT_ID="$chat")
refused() {
	run "$2" v2.6.9 "$url"
	is "refused, $1: exit" "$status" 1
	has "refused, $1: says why" "$err" 'must open with the upgrade line'
	is "refused, $1: nothing sent" "$(sent)" ""
}
refused 'empty body' ''
refused 'generated notes only' "${notes#$'\n'}"
refused 'line in the second line' $'## Notes\nUpgrade: optional.'
refused 'required without blocks' 'Upgrade: required.'
refused 'block with separators' 'Upgrade: required on mainnet from block 3,300,000 (about 2026-12-10 12:00 UTC), optional on testnet.'
refused 'bold' '**Upgrade: optional.**'
run '' --dry-run v2.6.9 "$url"
is 'refused in a dry run too' "$status" 1
is 'refused in a dry run: no message' "$out" ""

# Wrong arguments.
for args in '' 'v2.6.9' '--dry-run v2.6.9' 'v2.6.9 url extra'; do
	read -r -a argv <<<"$args"
	run 'Upgrade: optional.' ${argv[@]+"${argv[@]}"}
	is "arguments '$args': exit" "$status" 2
	has "arguments '$args': usage" "$err" 'usage:'
done
run 'Upgrade: optional.' '' "$url"
is 'empty tag: exit' "$status" 2

# Without a token or a chat id it skips with a notice, and the release goes on.
for missing in TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID; do
	settings=(TELEGRAM_BOT_TOKEN="$token" TELEGRAM_CHAT_ID="$chat" TELEGRAM_TOPIC_ID=42 "$missing=")
	run "$both" v2.6.9 "$url"
	is "no $missing: exit" "$status" 0
	has "no $missing: notice" "$err" 'notice: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set, so v2.6.9 is not announced in Telegram.'
	is "no $missing: nothing sent" "$(sent)" ""
done
settings=(GITHUB_ACTIONS=true)
run "$both" v2.6.9 "$url"
is 'no settings in Actions: exit' "$status" 0
has 'no settings in Actions: notice annotation' "$err" '::notice::TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set'
is 'no settings in Actions: nothing sent' "$(sent)" ""

# A real send to the fake Telegram.
# sends NAME BODY WANT FIELDS [TOPIC]
sends() {
	local req=$tmp/req/1
	run "$2" v2.6.9 "$url"
	is "$1: exit" "$status" 0
	is "$1: output" "$out" 'Announced v2.6.9 in Telegram.'
	is "$1: one request" "$(sent)" 1
	is "$1: path" "$(cat "$req/path")" "/bot$token/sendMessage"
	is "$1: fields" "$(cat "$req/fields")" "$4"
	is "$1: chat_id" "$(cat "$req/chat_id")" "$chat"
	is "$1: parse_mode" "$(cat "$req/parse_mode")" HTML
	is "$1: disable_web_page_preview" "$(cat "$req/disable_web_page_preview")" true
	is "$1: text" "$(cat "$req/text"; echo .)" "$3."
	if [ -n "${5:-}" ]; then
		is "$1: message_thread_id" "$(cat "$req/message_thread_id")" "$5"
	fi
}
settings=(TELEGRAM_BOT_TOKEN="$token" TELEGRAM_CHAT_ID="$chat" TELEGRAM_TOPIC_ID=42)
sends 'send to a topic' "$both$notes" "$want_both" 'chat_id disable_web_page_preview message_thread_id parse_mode text' 42
sends 'send, optional' "Upgrade: optional.$notes" "$want_optional" 'chat_id disable_web_page_preview message_thread_id parse_mode text' 42
settings=(TELEGRAM_BOT_TOKEN="$token" TELEGRAM_CHAT_ID="$chat")
sends 'send without a topic' "$mainnet$notes" "$want_mainnet" 'chat_id disable_web_page_preview parse_mode text'
settings=(TELEGRAM_BOT_TOKEN="$token" TELEGRAM_CHAT_ID="$chat" TELEGRAM_TOPIC_ID=)
sends 'send with an empty topic' "$testnet$notes" "$want_testnet" 'chat_id disable_web_page_preview parse_mode text'

# Telegram refuses the message, or does not answer.
settings=(TELEGRAM_BOT_TOKEN="$token" TELEGRAM_CHAT_ID="$chat" TELEGRAM_TOPIC_ID=999)
touch "$tmp/telegram-fail"
run "$both" v2.6.9 "$url"
rm -f "$tmp/telegram-fail"
is 'Telegram refuses: exit' "$status" 1
has 'Telegram refuses: says why' "$err" 'Telegram answered HTTP 400: Bad Request: message thread not found'
settings=(TELEGRAM_BOT_TOKEN="$token" TELEGRAM_CHAT_ID="$chat" TELEGRAM_API=http://127.0.0.1:1)
run "$both" v2.6.9 "$url"
is 'Telegram unreachable: exit' "$status" 1
has 'Telegram unreachable: says so' "$err" 'Telegram answered HTTP 000'

# The token went to curl through stdin only, and no output printed it.
if [ -s "$tmp/argv" ] && grep -q -- '--data-urlencode' "$tmp/argv"; then
	pass 'curl ran through the wrapper'
else
	fail 'curl did not run through the wrapper'
fi
if grep -qF -- "$token" "$tmp/argv"; then
	fail "the token is on curl's command line"
else
	pass "the token is not on curl's command line"
fi
if grep -qF -- "$token" "$tmp/all-output"; then
	fail 'the token is in the output'
else
	pass 'the token is not in the output'
fi

if [ "$fails" -ne 0 ]; then
	echo "$fails check(s) failed"
	exit 1
fi
echo "all checks passed"
