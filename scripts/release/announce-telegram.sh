#!/usr/bin/env bash
# Posts one message about a release to Telegram, to the chat and topic that the
# node alerts use. The message says whether operators must install the release,
# read from the upgrade line that opens the release body (docs/release-process.md).
# release.yaml runs it once per release, after it uploads the binaries.
#
# Usage: scripts/release/announce-telegram.sh [--dry-run] TAG RELEASE_URL < release-notes.md
#
#   --dry-run  print the message and send nothing
#
# Settings, from the environment:
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  where the message goes; without both,
#                                         it prints a notice and sends nothing
#   TELEGRAM_TOPIC_ID                     topic (thread) in a forum chat, optional
#   TELEGRAM_API                          for tests: another Telegram server
#
# Exits 1 when the body does not open with the upgrade line or Telegram does not
# take the message, and 2 on wrong arguments.
set -euo pipefail

guide_url=https://github.com/trufnetwork/node/blob/main/docs/node-upgrade-guide.md#required-in-place-upgrade

usage() {
	echo "usage: $0 [--dry-run] TAG RELEASE_URL < release-notes.md" >&2
	exit 2
}

dry_run=no
if [ "${1:-}" = --dry-run ]; then
	dry_run=yes
	shift
fi
[ $# -eq 2 ] && [ -n "$1" ] && [ -n "$2" ] || usage
tag=$1 release_url=$2

notice() {
	if [ "${GITHUB_ACTIONS:-}" = true ]; then
		echo "::notice::$1" >&2
	else
		echo "notice: $1" >&2
	fi
}

# Escapes the characters that Telegram's HTML parse mode reads as markup.
# The replacements are quoted so that bash 5.2 does not read & in them as the
# matched text.
html() {
	local s=$1
	s=${s//'&'/'&amp;'}
	s=${s//'<'/'&lt;'}
	s=${s//'>'/'&gt;'}
	printf '%s' "$s"
}

# Writes a block number with thousands separators: 3300000 -> 3,300,000.
commas() {
	local n=$1 out=''
	while [ ${#n} -gt 3 ]; do
		out=,${n: -3}$out
		n=${n:0:${#n}-3}
	done
	printf '%s' "$n$out"
}

line=''
IFS= read -r line || true
line=${line%$'\r'}
# The same check that release.yaml runs before it builds; it reads only the
# first line. Its "Upgrade line:" output goes to stderr so that --dry-run
# prints the message alone.
printf '%s\n' "$line" | "$(dirname "$0")/check-upgrade-line.sh" >&2

declare -A block=() when=()
for net in mainnet testnet; do
	re="required on $net from block ([0-9]+) \\(about ([^)]*)\\)"
	if [[ $line =~ $re ]]; then
		block[$net]=$(commas "${BASH_REMATCH[1]}")
		when[$net]=$(html "${BASH_REMATCH[2]}")
	fi
done

name=$(html "$tag")
version=$(html "${tag#v}")
links="Release: $(html "$release_url")"
stops="A node still on an older release stops at that block. After upgrading, <code>kwild version</code> prints ${version}."

if [ -n "${block[mainnet]:-}" ] && [ -n "${block[testnet]:-}" ]; then
	text="REQUIRED: TN node ${name}

Install it before:
- mainnet block ${block[mainnet]} (about ${when[mainnet]})
- testnet block ${block[testnet]} (about ${when[testnet]})

${stops}

${links}
How to upgrade: ${guide_url}"
elif [ -n "${block[mainnet]:-}" ] || [ -n "${block[testnet]:-}" ]; then
	if [ -n "${block[mainnet]:-}" ]; then
		net=mainnet other=testnet
	else
		net=testnet other=mainnet
	fi
	text="REQUIRED on ${net}: TN node ${name}

Install it before ${net} block ${block[$net]} (about ${when[$net]}). On ${other} it is optional.

${stops}

${links}
How to upgrade: ${guide_url}"
else
	text="TN node ${name} is out. Optional: nodes on older releases keep following the chain, so upgrade when it suits you.

${links}"
fi

if [ "$dry_run" = yes ]; then
	printf '%s\n' "$text"
	exit 0
fi

if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
	notice "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set, so ${tag} is not announced in Telegram."
	exit 0
fi

topic=()
if [ -n "${TELEGRAM_TOPIC_ID:-}" ]; then
	topic=(--data-urlencode "message_thread_id=${TELEGRAM_TOPIC_ID}")
fi
answer=$(mktemp)
trap 'rm -f "$answer"' EXIT
# The URL holds the token, so it goes to curl through stdin, not the command line.
code=$(printf 'url = "%s/bot%s/sendMessage"\n' "${TELEGRAM_API:-https://api.telegram.org}" "$TELEGRAM_BOT_TOKEN" |
	curl -sS -m 20 -o "$answer" -w '%{http_code}' -K - \
		--data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" ${topic[@]+"${topic[@]}"} \
		--data-urlencode parse_mode=HTML \
		--data-urlencode disable_web_page_preview=true \
		--data-urlencode "text=${text}") || code=000
if [ "$code" != 200 ]; then
	# Telegram explains a refusal in "description", such as a wrong topic id.
	why=$(sed -n 's/.*"description":"\([^"]*\)".*/\1/p' "$answer" | head -c 300)
	echo "announce-telegram: Telegram answered HTTP ${code}${why:+: $why}" >&2
	exit 1
fi
echo "Announced ${tag} in Telegram."
