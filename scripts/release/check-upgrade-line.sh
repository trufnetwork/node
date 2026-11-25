#!/usr/bin/env bash
# Checks that a release body, read from stdin, opens with the upgrade line.
# The forms are in docs/release-process.md:
#
#   Upgrade: optional.
#   Upgrade: required on mainnet from block <N> (about <YYYY-MM-DD HH:MM> UTC), required on testnet from block <M> (about <YYYY-MM-DD HH:MM> UTC).
#   Upgrade: required on mainnet from block <N> (about <YYYY-MM-DD HH:MM> UTC), optional on testnet.
#   Upgrade: optional on mainnet, required on testnet from block <M> (about <YYYY-MM-DD HH:MM> UTC).
#
# Usage: scripts/release/check-upgrade-line.sh < release-notes.md
set -euo pipefail

block='[1-9][0-9]*'
time='[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) ([01][0-9]|2[0-3]):[0-5][0-9] UTC'
required() { printf 'required on %s from block %s \\(about %s\\)' "$1" "$block" "$time"; }
optional() { printf 'optional on %s' "$1"; }

forms=(
	'Upgrade: optional\.'
	"Upgrade: $(required mainnet), $(required testnet)\\."
	"Upgrade: $(required mainnet), $(optional testnet)\\."
	"Upgrade: $(optional mainnet), $(required testnet)\\."
)

line=''
IFS= read -r line || true
# GitHub's release editor saves CRLF line ends.
line=${line%$'\r'}

for form in "${forms[@]}"; do
	if [[ $line =~ ^${form}$ ]]; then
		echo "Upgrade line: $line"
		exit 0
	fi
done

error() {
	if [ "${GITHUB_ACTIONS:-}" = true ]; then
		echo "::error::$1" >&2
	else
		echo "error: $1" >&2
	fi
}

if [ -z "$line" ]; then
	error "the release body must open with the upgrade line, but its first line is empty"
else
	# Shown for the reader; control characters are replaced so the line prints as one line.
	error "the release body must open with the upgrade line, but its first line is: ${line//[[:cntrl:]]/?}"
fi
cat >&2 <<'EOF'
The first line must be exactly one of these, with your blocks and times:
  Upgrade: optional.
  Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).
  Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), optional on testnet.
  Upgrade: optional on mainnet, required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).
See docs/release-process.md.
EOF
exit 1
