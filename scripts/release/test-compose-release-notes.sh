#!/usr/bin/env bash
# Tests scripts/release/compose-release-notes.sh, the step of the Draft release
# form that checks the answers and writes the upgrade line: every combination of
# optional and required networks, with and without a summary, every refusal,
# the short-notice and past-time warnings, the calendar, and the height check
# against a fake curl. Every line it prints must pass check-upgrade-line.sh.
#
# Usage: scripts/release/test-compose-release-notes.sh
set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)
compose=$dir/compose-release-notes.sh
check=$dir/check-upgrade-line.sh
announce=$dir/announce-telegram.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"

unset GITHUB_ACTIONS FAKE_MAINNET FAKE_TESTNET

# The fake curl answers a height read from FAKE_MAINNET or FAKE_TESTNET,
# chosen by the URL, and fails as an unreachable host when that is empty. It
# records its arguments, one per line.
cat >"$tmp/bin/curl" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$@" >>"$tmp/curl-argv"
case "\${*: -1}" in
*mainnet*) reply=\${FAKE_MAINNET:-} ;;
*testnet*) reply=\${FAKE_TESTNET:-} ;;
*) reply= ;;
esac
[ -n "\$reply" ] || exit 7
printf '%s' "\$reply"
EOF
chmod +x "$tmp/bin/curl"

now='2026-11-25 12:00'
mainnet=(--mainnet required --mainnet-block 3300000 --mainnet-time '2026-12-10 12:00')
testnet=(--testnet required --testnet-block 16300000 --testnet-time '2026-12-03 12:00')
line_optional='Upgrade: optional.'
line_both='Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).'
line_mainnet='Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), optional on testnet.'
line_testnet='Upgrade: optional on mainnet, required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).'

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

# lacks NAME TEXT PART
lacks() {
	if [[ $2 == *"$3"* ]]; then
		fail "$1: '$3' in: $2"
	else
		pass "$1"
	fi
}

# run ARG...: runs the script at $now with the settings in $settings and the
# fake curl; sets status, out (exactly as printed) and err.
run() {
	rm -f "$tmp/curl-argv"
	status=0
	env PATH="$tmp/bin:$PATH" ${settings[@]+"${settings[@]}"} \
		"$compose" --now "$now" "$@" >"$tmp/out" 2>"$tmp/err" || status=$?
	out=$(
		cat "$tmp/out"
		echo .
	)
	out=${out%.}
	err=$(<"$tmp/err")
}

# passes_check NAME: the printed text passes check-upgrade-line.sh.
passes_check() {
	if "$check" <"$tmp/out" >/dev/null 2>&1; then
		pass "$1: check-upgrade-line.sh accepts it"
	else
		fail "$1: check-upgrade-line.sh refuses: $out"
	fi
}

# accepts NAME WANT ARG...: exit 0, prints WANT exactly, which passes the check,
# and says nothing but the upgrade line on stderr.
accepts() {
	local name=$1 want=$2
	shift 2
	run "$@"
	is "$name: exit" "$status" 0
	is "$name: notes" "$out" "$want"
	passes_check "$name"
	is "$name: stderr" "$err" "Upgrade line: ${want%%$'\n'*}"
}

# refuses NAME MESSAGE ARG...: exit 1, prints nothing, and says MESSAGE.
refuses() {
	local name=$1 message=$2
	shift 2
	run "$@"
	is "refused, $name: exit" "$status" 1
	is "refused, $name: prints nothing" "$out" ""
	has "refused, $name: says why" "$err" "error: $message"
	has "refused, $name: counts the problems" "$err" 'The form has 1 problem(s). Fix them and run it again.'
}

# warns NAME KIND MESSAGE ARG...: exit 0, says KIND: MESSAGE, and prints a
# line that passes the check.
warns() {
	local name=$1 kind=$2 message=$3
	shift 3
	run "$@"
	is "$name: exit" "$status" 0
	has "$name: $kind" "$err" "$kind: $message"
	passes_check "$name"
}

# Every combination, with and without a summary. The Telegram message that the
# release will post, from announce-telegram.sh --dry-run, names the right
# networks.
for summary in '' 'Adds the new market actions.'; do
	with=${summary:+, with a summary}
	more=${summary:+$'\n\n'$summary}
	for combo in optional mainnet testnet both; do
		case $combo in
		optional) args=() line=$line_optional title='TN node v2.7.0 is out. Optional:' ;;
		mainnet) args=("${mainnet[@]}") line=$line_mainnet title='REQUIRED on mainnet: TN node v2.7.0' ;;
		testnet) args=("${testnet[@]}") line=$line_testnet title='REQUIRED on testnet: TN node v2.7.0' ;;
		both) args=("${mainnet[@]}" "${testnet[@]}") line=$line_both title='REQUIRED: TN node v2.7.0' ;;
		esac
		accepts "$combo$with" "$line$more"$'\n' --tag v2.7.0 ${args[@]+"${args[@]}"} --summary "$summary"
		message=$("$announce" --dry-run v2.7.0 https://github.com/trufnetwork/node/releases/tag/v2.7.0 <"$tmp/out" 2>/dev/null) || true
		has "$combo$with: the Telegram message" "$message" "$title"
	done
done

# The defaults, and values with spaces around them. A time may end in UTC.
accepts 'no options but the tag' "$line_optional"$'\n' --tag v2.7.0
accepts 'explicit optional and target' "$line_optional"$'\n' --tag v2.7.0 --target main --mainnet optional --testnet optional --short-notice false
accepts 'a commit as the target' "$line_optional"$'\n' --tag v2.7.0 --target 8e70cf06d80f9975c31930ed710ef88a2d2e89a8
accepts 'a branch with a slash as the target' "$line_optional"$'\n' --tag v2.7.0 --target release/v2.7
accepts 'spaces around the values' "$line_mainnet"$'\n\nA summary.\n' --tag v2.7.0 --mainnet required \
	--mainnet-block ' 3300000 ' --mainnet-time ' 2026-12-10 12:00 UTC ' --summary '  A summary.  '
accepts 'a summary of spaces is no summary' "$line_optional"$'\n' --tag v2.7.0 --summary '   '
accepts 'empty blocks and times on optional networks' "$line_optional"$'\n' --tag v2.7.0 \
	--mainnet-block '' --mainnet-time '' --testnet-block ' ' --testnet-time ''
accepts 'markup in the summary is kept' "$line_optional"$'\n\n**Note:** see [the guide](https://example.test).\n' \
	--tag v2.7.0 --summary '**Note:** see [the guide](https://example.test).'

# The tag.
refuses 'no tag' 'The tag is missing.'
refuses 'empty tag' 'The tag is missing.' --tag ''
for bad in 2.7.0 v2.7 v2.7.0.1 v2.7.0-rc.1 v02.7.0 v2.07.0 v2.7.01 V2.7.0 'v2.7.0 ' ' v2.7.0' 'v2.7.0/x'; do
	refuses "tag '$bad'" "The tag '$bad' is not of the form vX.Y.Z" --tag "$bad"
done
refuses 'tag with a new line' "The tag 'v2.7.0?x' is not of the form vX.Y.Z" --tag $'v2.7.0\nx'

# The target.
refuses 'empty target' 'The target is empty.' --tag v2.7.0 --target ''
for bad in ../x main..x -x /x x/ a//b 'a b' 'main?x' 'main#x' 'main~1' 'main^'; do
	refuses "target '$bad'" "The target '$bad' is not a branch name or a commit." --tag v2.7.0 --target "$bad"
done

# The choices.
for net in mainnet testnet; do
	for bad in Required yes ''; do
		refuses "$net choice '$bad'" "The $net choice is '$bad'; it is optional or required." --tag v2.7.0 --$net "$bad"
	done
done
refuses "short notice 'yes'" "Short notice is 'yes'; it is true or false." --tag v2.7.0 --short-notice yes

# A required network needs its block and its time.
refuses 'mainnet required, no block' 'Mainnet is required, so give the mainnet block' \
	--tag v2.7.0 --mainnet required --mainnet-time '2026-12-10 12:00'
refuses 'mainnet required, no time' 'Mainnet is required, so give the time of the mainnet block' \
	--tag v2.7.0 --mainnet required --mainnet-block 3300000
refuses 'testnet required, no block' 'Testnet is required, so give the testnet block' \
	--tag v2.7.0 --testnet required --testnet-time '2026-12-03 12:00'
refuses 'testnet required, no time' 'Testnet is required, so give the time of the testnet block' \
	--tag v2.7.0 --testnet required --testnet-block 16300000
refuses 'testnet required, block of spaces' 'Testnet is required, so give the testnet block' \
	--tag v2.7.0 --testnet required --testnet-block '  ' --testnet-time '2026-12-03 12:00'

# An optional network takes no block and no time, so nothing typed is dropped.
refuses 'stray mainnet block' "The mainnet block is '3300000', but mainnet is optional." --tag v2.7.0 --mainnet-block 3300000
refuses 'stray mainnet time' "The mainnet time is '2026-12-10 12:00', but mainnet is optional." --tag v2.7.0 --mainnet-time '2026-12-10 12:00'
refuses 'stray testnet block' "The testnet block is '16300000', but testnet is optional." --tag v2.7.0 --testnet-block 16300000
refuses 'stray testnet time' "The testnet time is '2026-12-03 12:00', but testnet is optional." --tag v2.7.0 --testnet-time '2026-12-03 12:00'
refuses 'stray testnet block, mainnet required' "The testnet block is '16300000', but testnet is optional." \
	--tag v2.7.0 "${mainnet[@]}" --testnet-block 16300000

# Blocks.
for bad in 0 03300000 3,300,000 3.3e6 3_300_000 '3300000 blocks' abc -5; do
	refuses "block '$bad'" "The mainnet block '$bad' is not a block height." \
		--tag v2.7.0 --mainnet required --mainnet-block "$bad" --mainnet-time '2026-12-10 12:00'
done

# Times.
for bad in '2026-12-10T12:00Z' '2026-12-10 12:00:00' '2026-12-10 9:00' '2026-12-10 12' '10-12-2026 12:00' \
	'2026/12/10 12:00' '2026-12-10 12:00 utc' '2026-13-10 12:00' '2026-00-10 12:00' '2026-12-32 12:00' \
	'2026-12-00 12:00' '2026-11-31 12:00' '2026-12-10 24:00' '2026-12-10 12:60' '2027-02-29 12:00' \
	'2100-02-29 12:00' 'next week'; do
	refuses "time '$bad'" "The testnet time '$bad' is not a UTC time of the form YYYY-MM-DD HH:MM" \
		--tag v2.7.0 --testnet required --testnet-block 16300000 --testnet-time "$bad"
done
accepts 'leap day 2000-02-29' 'Upgrade: optional on mainnet, required on testnet from block 16300000 (about 2000-02-29 12:00 UTC).'$'\n' \
	--now '2000-01-01 00:00' --tag v2.7.0 --testnet required --testnet-block 16300000 --testnet-time '2000-02-29 12:00'
accepts 'leap day 2028-02-29' 'Upgrade: optional on mainnet, required on testnet from block 16300000 (about 2028-02-29 12:00 UTC).'$'\n' \
	--tag v2.7.0 --testnet required --testnet-block 16300000 --testnet-time '2028-02-29 12:00'

# At least 7 days of notice on mainnet, counted from now.
refuses 'mainnet time 6 days away' \
	'The mainnet time 2026-12-01 12:00 UTC is less than 7 days away. A required release goes out at least 7 days before its mainnet block, so pick a block at 2026-12-02 12:00 UTC or later, or tick short notice for an urgent release.' \
	--tag v2.7.0 --mainnet required --mainnet-block 3300000 --mainnet-time '2026-12-01 12:00'
refuses 'mainnet time in the past' 'The mainnet time 2026-11-20 12:00 UTC is less than 7 days away.' \
	--tag v2.7.0 --mainnet required --mainnet-block 3300000 --mainnet-time '2026-11-20 12:00'
# The boundary, across a month, a year end, and February in a leap and a common year.
for pair in '2026-11-25 12:00=2026-12-02' '2026-12-28 12:00=2027-01-04' '2028-02-25 12:00=2028-03-03' '2027-02-25 12:00=2027-03-04'; do
	start=${pair%=*} day=${pair#*=}
	accepts "exactly 7 days after $start" "Upgrade: required on mainnet from block 3300000 (about $day 12:00 UTC), optional on testnet."$'\n' \
		--now "$start" --tag v2.7.0 --mainnet required --mainnet-block 3300000 --mainnet-time "$day 12:00"
	refuses "a minute short of 7 days after $start" "The mainnet time $day 11:59 UTC is less than 7 days away." \
		--now "$start" --tag v2.7.0 --mainnet required --mainnet-block 3300000 --mainnet-time "$day 11:59"
done
# Testnet has no notice rule.
accepts 'testnet time 1 day away' 'Upgrade: optional on mainnet, required on testnet from block 16300000 (about 2026-11-26 12:00 UTC).'$'\n' \
	--tag v2.7.0 --testnet required --testnet-block 16300000 --testnet-time '2026-11-26 12:00'

# Short notice lets a nearer mainnet time through, with a warning.
warns 'short notice' warning 'The mainnet time 2026-11-28 12:00 UTC is less than 7 days away. Short notice is ticked, so the draft goes ahead' \
	--tag v2.7.0 --mainnet required --mainnet-block 3300000 --mainnet-time '2026-11-28 12:00' --short-notice true
is 'short notice: notes' "$out" $'Upgrade: required on mainnet from block 3300000 (about 2026-11-28 12:00 UTC), optional on testnet.\n'
warns 'short notice, time passed' warning 'The mainnet time 2026-11-24 12:00 UTC has passed. Short notice is ticked, so the draft goes ahead' \
	--tag v2.7.0 --mainnet required --mainnet-block 3300000 --mainnet-time '2026-11-24 12:00' --short-notice true
warns 'short notice not needed' notice 'Short notice is ticked, but the mainnet time is 7 days away or more, so it changes nothing.' \
	--tag v2.7.0 "${mainnet[@]}" --short-notice true
warns 'short notice, mainnet optional' notice 'Short notice is ticked, but mainnet is not required, so it changes nothing.' \
	--tag v2.7.0 "${testnet[@]}" --short-notice true
warns 'testnet time passed' warning 'The testnet time 2026-11-24 12:00 UTC has passed.' \
	--tag v2.7.0 --testnet required --testnet-block 16300000 --testnet-time '2026-11-24 12:00'
refuses 'short notice does not let a bad time through' "The mainnet time '2026-11-28' is not a UTC time" \
	--tag v2.7.0 --mainnet required --mainnet-block 3300000 --mainnet-time '2026-11-28' --short-notice true

# Every problem prints, in one run.
run --tag v2.7 --target ../x --mainnet required --mainnet-time '2026-11-28 12:00' \
	--testnet-block 16300000 --summary $'two\nlines' --short-notice maybe
is 'many problems: exit' "$status" 1
is 'many problems: prints nothing' "$out" ""
for part in "The tag 'v2.7'" "The target '../x'" "Short notice is 'maybe'" 'The summary must be one line' \
	'Mainnet is required, so give the mainnet block' 'The mainnet time 2026-11-28 12:00 UTC is less than 7 days away' \
	"The testnet block is '16300000', but testnet is optional" 'The form has 7 problem(s).'; do
	has "many problems: $part" "$err" "$part"
done
refuses 'a summary of two lines' 'The summary must be one line of text.' --tag v2.7.0 --summary $'two\nlines'
refuses 'a summary with a carriage return' 'The summary must be one line of text.' --tag v2.7.0 --summary $'one\rtwo'

# In Actions, problems are annotations.
settings=(GITHUB_ACTIONS=true)
run --tag v2.7
has 'Actions: error annotation' "$err" "::error::The tag 'v2.7' is not of the form vX.Y.Z"
run --tag v2.7.0 --mainnet required --mainnet-block 3300000 --mainnet-time '2026-11-28 12:00' --short-notice true
has 'Actions: warning annotation' "$err" '::warning::The mainnet time 2026-11-28 12:00 UTC is less than 7 days away.'
settings=()

# The height check reads each required network's height and only warns.
rpc=(--mainnet-rpc https://rpc.mainnet.example/rpc/v1 --testnet-rpc https://rpc.testnet.example/rpc/v1)
reply() { printf '{"jsonrpc":"2.0","id":1,"result":{"chain_id":"x","block_height":%s,"block_hash":"ab","gas":false}}' "$1"; }
settings=(FAKE_MAINNET="$(reply 3235250)" FAKE_TESTNET="$(reply 16169260)")
accepts 'blocks above the heights' "$line_both"$'\n' --tag v2.7.0 "${mainnet[@]}" "${testnet[@]}" "${rpc[@]}"
has 'height read: JSON-RPC method' "$(cat "$tmp/curl-argv")" '"method":"user.chain_info"'
has 'height read: mainnet URL' "$(cat "$tmp/curl-argv")" 'https://rpc.mainnet.example/rpc/v1'
has 'height read: testnet URL' "$(cat "$tmp/curl-argv")" 'https://rpc.testnet.example/rpc/v1'
settings=(FAKE_MAINNET="$(reply 3300000)" FAKE_TESTNET="$(reply 16400000)")
warns 'mainnet block at the height' warning 'The mainnet block 3300000 is not above the current mainnet height 3300000, read from https://rpc.mainnet.example/rpc/v1.' \
	--tag v2.7.0 "${mainnet[@]}" "${testnet[@]}" "${rpc[@]}"
has 'testnet block below the height' "$err" 'warning: The testnet block 16300000 is not above the current testnet height 16400000'
settings=(FAKE_MAINNET="$(reply 9999999)")
accepts 'a block with more digits than the height' 'Upgrade: required on mainnet from block 10000000 (about 2026-12-10 12:00 UTC), optional on testnet.'$'\n' \
	--tag v2.7.0 --mainnet required --mainnet-block 10000000 --mainnet-time '2026-12-10 12:00' "${rpc[@]}"
settings=(FAKE_MAINNET="$(reply 10000000)")
warns 'a block with fewer digits than the height' warning 'The mainnet block 9999999 is not above the current mainnet height 10000000' \
	--tag v2.7.0 --mainnet required --mainnet-block 9999999 --mainnet-time '2026-12-10 12:00' "${rpc[@]}"
settings=()
warns 'unreachable node' notice 'Could not read the current mainnet height from https://rpc.mainnet.example/rpc/v1, so the mainnet block is not compared with it.' \
	--tag v2.7.0 "${mainnet[@]}" "${rpc[@]}"
settings=(FAKE_MAINNET='<html>502 Bad Gateway</html>')
warns 'an answer without a height' notice 'Could not read the current mainnet height' --tag v2.7.0 "${mainnet[@]}" "${rpc[@]}"
settings=(FAKE_MAINNET="$(reply 3235250)")
run --tag v2.7.0 "${testnet[@]}" --mainnet-rpc https://rpc.mainnet.example/rpc/v1
is 'optional network: height not read' "$(cat "$tmp/curl-argv" 2>/dev/null)" ""
run --tag v2.7.0 "${mainnet[@]}"
is 'no URL: height not read' "$(cat "$tmp/curl-argv" 2>/dev/null)" ""
run --tag v2.7.0 --mainnet required --mainnet-block 3,300,000 --mainnet-time '2026-12-10 12:00' "${rpc[@]}"
is 'bad block: height not read' "$(cat "$tmp/curl-argv" 2>/dev/null)" ""
settings=()

# Wrong arguments.
for args in '--bogus 1' '--tag' 'v2.7.0' '-- v2.7.0' '--tag v2.7.0 extra' '--now 2026-11-25'; do
	read -r -a argv <<<"$args"
	run "${argv[@]}"
	is "arguments '$args': exit" "$status" 2
	has "arguments '$args': usage" "$err" 'usage:'
done

if [ "$fails" -ne 0 ]; then
	echo "$fails check(s) failed"
	exit 1
fi
echo "all checks passed"
