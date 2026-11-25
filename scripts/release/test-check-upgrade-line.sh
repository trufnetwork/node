#!/usr/bin/env bash
# Tests scripts/release/check-upgrade-line.sh against the forms in
# docs/release-process.md. Run: scripts/release/test-check-upgrade-line.sh
set -euo pipefail

check="$(dirname "$0")/check-upgrade-line.sh"
failed=0

expect() {
	local want=$1 name=$2 body=$3 got=accepted
	printf '%s' "$body" | GITHUB_ACTIONS='' "$check" >/dev/null 2>&1 || got=rejected
	if [ "$got" = "$want" ]; then
		echo "ok   $want: $name"
	else
		echo "FAIL want $want, got $got: $name"
		failed=$((failed + 1))
	fi
}

notes=$'\n## What\'s Changed\n* chore: an example change by @someone in https://github.com/trufnetwork/node/pull/1\n'
both='Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).'
mainnet='Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), optional on testnet.'
testnet='Upgrade: optional on mainnet, required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).'

expect accepted 'optional, alone' 'Upgrade: optional.'
expect accepted 'optional, with a trailing newline' $'Upgrade: optional.\n'
expect accepted 'optional, then the notes' "Upgrade: optional.$notes"
expect accepted 'optional, with CRLF line ends' $'Upgrade: optional.\r\n## What\'s Changed\r\n'
expect accepted 'required on both networks' "$both$notes"
expect accepted 'required on mainnet only' "$mainnet$notes"
expect accepted 'required on testnet only' "$testnet$notes"
expect accepted 'earliest valid time' 'Upgrade: optional on mainnet, required on testnet from block 1 (about 2026-01-01 00:00 UTC).'
expect accepted 'latest valid time' 'Upgrade: optional on mainnet, required on testnet from block 1 (about 2026-12-31 23:59 UTC).'

expect rejected 'empty body' ''
expect rejected 'blank first line' $'\nUpgrade: optional.'
expect rejected 'generated notes only' "${notes#$'\n'}"
expect rejected 'line in the second line' "## Notes"$'\n''Upgrade: optional.'
expect rejected 'lower case' 'upgrade: optional.'
expect rejected 'capital Optional' 'Upgrade: Optional.'
expect rejected 'no full stop' 'Upgrade: optional'
expect rejected 'leading space' ' Upgrade: optional.'
expect rejected 'trailing space' 'Upgrade: optional. '
expect rejected 'bold' '**Upgrade: optional.**'
expect rejected 'heading' '## Upgrade: optional.'
expect rejected 'text after the line' 'Upgrade: optional. Nothing to do.'
expect rejected 'optional, long form' 'Upgrade: optional on mainnet, optional on testnet.'
expect rejected 'required without blocks' 'Upgrade: required.'
expect rejected 'required on mainnet, testnet left out' 'Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC).'
expect rejected 'testnet before mainnet' 'Upgrade: required on testnet from block 16300000 (about 2026-12-03 12:00 UTC), optional on mainnet.'
expect rejected 'second required left out' 'Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), on testnet from block 16300000 (about 2026-12-03 12:00 UTC).'
expect rejected 'unknown network' 'Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), optional on devnet.'
expect rejected 'block with separators' 'Upgrade: required on mainnet from block 3,300,000 (about 2026-12-10 12:00 UTC), optional on testnet.'
expect rejected 'block with a leading zero' 'Upgrade: required on mainnet from block 03300000 (about 2026-12-10 12:00 UTC), optional on testnet.'
expect rejected 'block 0' 'Upgrade: required on mainnet from block 0 (about 2026-12-10 12:00 UTC), optional on testnet.'
expect rejected 'no block' 'Upgrade: required on mainnet from block (about 2026-12-10 12:00 UTC), optional on testnet.'
expect rejected 'no time' 'Upgrade: required on mainnet from block 3300000, optional on testnet.'
expect rejected 'time without about' 'Upgrade: required on mainnet from block 3300000 (2026-12-10 12:00 UTC), optional on testnet.'
expect rejected 'time without UTC' 'Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00), optional on testnet.'
expect rejected 'ISO time' 'Upgrade: required on mainnet from block 3300000 (about 2026-12-10T12:00Z), optional on testnet.'
expect rejected 'time with seconds' 'Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00:00 UTC), optional on testnet.'
expect rejected 'month 13' 'Upgrade: required on mainnet from block 3300000 (about 2026-13-10 12:00 UTC), optional on testnet.'
expect rejected 'day 32' 'Upgrade: required on mainnet from block 3300000 (about 2026-12-32 12:00 UTC), optional on testnet.'
expect rejected 'hour 24' 'Upgrade: required on mainnet from block 3300000 (about 2026-12-10 24:00 UTC), optional on testnet.'
expect rejected 'one-digit hour' 'Upgrade: required on mainnet from block 3300000 (about 2026-12-10 9:00 UTC), optional on testnet.'

if [ "$failed" -ne 0 ]; then
	echo "$failed case(s) failed"
	exit 1
fi
echo "all cases passed"
