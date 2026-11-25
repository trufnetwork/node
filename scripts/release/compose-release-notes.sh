#!/usr/bin/env bash
# Checks the answers of the Draft release form (.github/workflows/draft-release.yaml)
# and prints the top of the release notes: the upgrade line, in one of the forms
# that check-upgrade-line.sh accepts (docs/release-process.md), then, when there
# is a summary, a blank line and the summary. The workflow adds a blank line and
# the notes that GitHub generates.
#
# Usage: scripts/release/compose-release-notes.sh --tag vX.Y.Z [OPTION VALUE]... > head.md
#
#   --tag vX.Y.Z                       the new release's tag
#   --target REF                       the branch or commit to release (default
#                                      main); only its spelling is checked here
#   --mainnet optional|required        default optional
#   --mainnet-block N                  only when mainnet is required
#   --mainnet-time 'YYYY-MM-DD HH:MM'  in UTC, only when mainnet is required
#   --testnet optional|required        default optional
#   --testnet-block N                  only when testnet is required
#   --testnet-time 'YYYY-MM-DD HH:MM'  in UTC, only when testnet is required
#   --summary TEXT                     one line under the upgrade line
#   --short-notice true|false          allow a mainnet time less than 7 days
#                                      away, with a warning (default false)
#   --mainnet-rpc URL                  read the mainnet height from this JSON-RPC
#                                      URL and warn when the mainnet block is not
#                                      above it; skipped when it does not answer
#   --testnet-rpc URL                  the same for testnet
#   --now 'YYYY-MM-DD HH:MM'           for tests: the time to count from
#
# An empty block, time, summary or URL is the same as leaving it out. Every
# answer is checked before anything is printed, and each problem prints as one
# error. Exits 1 when an answer is refused, and 2 on wrong arguments.
set -euo pipefail

usage() {
	echo "usage: $0 --tag vX.Y.Z [OPTION VALUE]...; the options are listed at the top of the script" >&2
	exit 2
}

declare -A opt=(
	[tag]='' [target]=main
	[mainnet]=optional [mainnet-block]='' [mainnet-time]='' [mainnet-rpc]=''
	[testnet]=optional [testnet-block]='' [testnet-time]='' [testnet-rpc]=''
	[summary]='' [short-notice]=false [now]=''
)
while [ $# -gt 0 ]; do
	name=${1#--}
	[ "$1" != "$name" ] && [ -n "$name" ] && [ -n "${opt[$name]+set}" ] && [ $# -ge 2 ] || usage
	opt[$name]=$2
	shift 2
done

errors=0
say() {
	if [ "${GITHUB_ACTIONS:-}" = true ]; then
		echo "::$1::$2" >&2
	else
		echo "$1: $2" >&2
	fi
}
error() {
	errors=$((errors + 1))
	say error "$1"
}

# A value as it prints in a message: control characters become ?, so that it
# prints as one line.
show() { printf '%s' "${1//[[:cntrl:]]/?}"; }

trim() {
	local s=$1
	s=${s#"${s%%[![:space:]]*}"}
	s=${s%"${s##*[![:space:]]}"}
	printf '%s' "$s"
}

# Prints the seconds from 1970-01-01 00:00 UTC to a 'YYYY-MM-DD HH:MM' time in
# UTC. Fails when the text is not such a time or the date does not exist.
epoch() {
	local re='^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2})$'
	[[ $1 =~ $re ]] || return 1
	local y=$((10#${BASH_REMATCH[1]})) m=$((10#${BASH_REMATCH[2]})) d=$((10#${BASH_REMATCH[3]}))
	local hh=$((10#${BASH_REMATCH[4]})) mm=$((10#${BASH_REMATCH[5]}))
	local last=(31 28 31 30 31 30 31 31 30 31 30 31)
	if ((y % 4 == 0 && (y % 100 != 0 || y % 400 == 0))); then
		last[1]=29
	fi
	((y >= 1970 && m >= 1 && m <= 12 && d >= 1 && d <= last[m - 1] && hh <= 23 && mm <= 59)) || return 1
	# Days from 1970-01-01, by Howard Hinnant's days_from_civil.
	if ((m <= 2)); then
		y=$((y - 1))
	fi
	local era=$((y / 400))
	local yoe=$((y - era * 400))
	local doy=$(((153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1))
	local doe=$((yoe * 365 + yoe / 4 - yoe / 100 + doy))
	echo $(((era * 146097 + doe - 719468) * 86400 + hh * 3600 + mm * 60))
}

# Reads a network's current height from a node's JSON-RPC URL.
height() {
	local reply re='"block_height":([0-9]+)'
	reply=$(curl -sS -m 20 -X POST -H 'content-type: application/json' \
		--data '{"jsonrpc":"2.0","id":1,"method":"user.chain_info","params":{}}' "$1" 2>/dev/null) || return 1
	[[ $reply =~ $re ]] || return 1
	printf '%s' "${BASH_REMATCH[1]}"
}

# True when the whole number $1 is greater than the whole number $2, at any length.
above() {
	((${#1} > ${#2})) || { ((${#1} == ${#2})) && [[ $1 > $2 ]]; }
}

if [ -n "${opt[now]}" ]; then
	now=$(epoch "${opt[now]}") || usage
else
	printf -v now '%(%s)T' -1
fi
week=$((7 * 24 * 3600))

tag=${opt[tag]}
if [ -z "$tag" ]; then
	error "The tag is missing. Give the new release's tag, such as v2.7.0."
elif ! [[ $tag =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
	error "The tag '$(show "$tag")' is not of the form vX.Y.Z, such as v2.7.0."
fi

target=${opt[target]}
if [ -z "$target" ]; then
	error "The target is empty. Give the branch or the commit to release, such as main."
elif ! [[ $target =~ ^[A-Za-z0-9][A-Za-z0-9._/-]*$ ]] || [[ $target == *..* || $target == *//* || $target == */ ]]; then
	error "The target '$(show "$target")' is not a branch name or a commit. Give the branch or the commit to release, such as main."
fi

short_notice=${opt[short-notice]}
case $short_notice in
true | false) ;;
*) error "Short notice is '$(show "$short_notice")'; it is true or false." ;;
esac

summary=$(trim "${opt[summary]}")
if [[ $summary == *[[:cntrl:]]* ]]; then
	error "The summary must be one line of text."
fi

declare -A part=()
for net in mainnet testnet; do
	choice=${opt[$net]}
	example=3300000
	if [ "$net" = testnet ]; then
		example=16300000
	fi
	block=$(trim "${opt[$net-block]}")
	when=$(trim "${opt[$net-time]}")
	when=${when% UTC}
	case $choice in
	optional)
		part[$net]="optional on $net"
		# A block or a time for an optional network is a mistake in the form:
		# either the network is required, or the value is stray.
		if [ -n "$block" ]; then
			error "The $net block is '$(show "$block")', but $net is optional. Choose required for $net, or leave the $net block empty."
		fi
		if [ -n "$when" ]; then
			error "The $net time is '$(show "$when")', but $net is optional. Choose required for $net, or leave the $net time empty."
		fi
		continue
		;;
	required) ;;
	*)
		error "The $net choice is '$(show "$choice")'; it is optional or required."
		continue
		;;
	esac

	part[$net]="required on $net from block $block (about $when UTC)"
	if [ -z "$block" ]; then
		error "${net^} is required, so give the $net block from which it is required, such as $example."
		block_ok=no
	elif ! [[ $block =~ ^[1-9][0-9]*$ ]]; then
		error "The $net block '$(show "$block")' is not a block height. Write it as a whole number with no separators, such as $example."
		block_ok=no
	else
		block_ok=yes
	fi

	if [ -z "$when" ]; then
		error "${net^} is required, so give the time of the $net block, in UTC as YYYY-MM-DD HH:MM, such as 2026-12-10 12:00."
	elif ! at=$(epoch "$when"); then
		error "The $net time '$(show "$when")' is not a UTC time of the form YYYY-MM-DD HH:MM, such as 2026-12-10 12:00."
	elif [ "$net" = mainnet ] && ((at < now + week)); then
		# docs/release-process.md: publish a required release at least 7 days
		# before its mainnet block.
		if [ "$short_notice" = true ]; then
			if ((at < now)); then
				say warning "The mainnet time $when UTC has passed. Short notice is ticked, so the draft goes ahead; check the block and the time."
			else
				say warning "The mainnet time $when UTC is less than 7 days away. Short notice is ticked, so the draft goes ahead, without the 7 days of notice that docs/release-process.md asks for."
			fi
		else
			TZ=UTC printf -v earliest '%(%Y-%m-%d %H:%M)T' $(((now + week + 59) / 60 * 60))
			error "The mainnet time $when UTC is less than 7 days away. A required release goes out at least 7 days before its mainnet block, so pick a block at $earliest UTC or later, or tick short notice for an urgent release."
		fi
	elif [ "$net" = mainnet ] && [ "$short_notice" = true ]; then
		say notice "Short notice is ticked, but the mainnet time is 7 days away or more, so it changes nothing."
	elif [ "$net" = testnet ] && ((at < now)); then
		say warning "The testnet time $when UTC has passed. Check the testnet block and time."
	fi

	rpc=$(trim "${opt[$net-rpc]}")
	if [ "$block_ok" = yes ] && [ -n "$rpc" ]; then
		if current=$(height "$rpc"); then
			if ! above "$block" "$current"; then
				say warning "The $net block $block is not above the current $net height $current, read from $rpc. Nodes are past it already; check the block."
			fi
		else
			say notice "Could not read the current $net height from $rpc, so the $net block is not compared with it."
		fi
	fi
done

if [ "${opt[mainnet]}" != required ] && [ "$short_notice" = true ]; then
	say notice "Short notice is ticked, but mainnet is not required, so it changes nothing."
fi

if [ "$errors" -ne 0 ]; then
	echo "The form has $errors problem(s). Fix them and run it again." >&2
	exit 1
fi

if [ "${opt[mainnet]}" = optional ] && [ "${opt[testnet]}" = optional ]; then
	line='Upgrade: optional.'
else
	line="Upgrade: ${part[mainnet]}, ${part[testnet]}."
fi
# The same check that release.yaml and publish-node-image.yaml run on the
# release page. Its "Upgrade line:" output goes to stderr.
printf '%s\n' "$line" | "$(dirname "$0")/check-upgrade-line.sh" >&2

printf '%s\n' "$line"
if [ -n "$summary" ]; then
	printf '\n%s\n' "$summary"
fi
