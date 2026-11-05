#!/usr/bin/env bash
#
# Stores the node keys and the gateway session secret of one stack as SSM
# SecureString parameters, under the names the CDK app reads (lib/secrets):
#
#   <prefix>/node/<i>/nodekey      nodekey.json of node i (0-based, node-1 is 0)
#   <prefix>/kgw/session_secret    gateway session secret
#
# <prefix> defaults to /tsn/<stage>, or /tsn/<stage>/<dev prefix> when the
# stack has a devPrefix, the same default as the CDK app.
#
# Values are read from files or stdin and are never printed. They reach the
# AWS CLI through a temporary file, never through the command line.

set -euo pipefail
umask 077

usage() {
  cat <<'EOF'
Usage: scripts/ssm-secrets.sh [options] <command> [args]

Commands:
  gen-node-keys <count>          Generate <count> node keys with kwild and store
                                 key i as <prefix>/node/<i>/nodekey (i = 0 for
                                 node-1). Prints NODE_PUBLIC_KEYS for synth.
  put-node-key <index> <file|->  Store one existing node key as
                                 <prefix>/node/<index>/nodekey. The input is a
                                 nodekey.json file (kwild key gen --key-file) or
                                 a private key in hex with an optional
                                 #secp256k1 or #ed25519 suffix. Prints the node ID.
  put-session-secret <file|->    Store the gateway session secret as
                                 <prefix>/kgw/session_secret.

Give '-' as the file to read the value from stdin. One trailing newline is dropped.

Options:
  --stage <prod|dev>       The stack's stage context. Required unless --prefix is set.
  --dev-prefix <name>      The stack's devPrefix context, if it has one.
  --prefix </path>         The stack's secretsPrefix context, if it has one.
                           Default: /tsn/<stage> or /tsn/<stage>/<dev prefix>.
  --kms-key-arn <arn>      The stack's secretsKmsKeyArn context, if it has one.
                           Default: the AWS managed aws/ssm key.
  --region <region>        The stack's region. Default: the AWS CLI's region.
  --profile <name>         AWS CLI profile.
  --key-type <type>        gen-node-keys only: secp256k1 (default) or ed25519.
  --overwrite              Replace parameters that already exist. Without it
                           the script stops before writing anything.
  --dry-run                Check the arguments and the input, then print only
                           the parameter names. Makes no AWS calls and
                           generates no keys.
  -h, --help               Show this help.

Environment:
  KWILD_CLI_PATH           kwild binary for gen-node-keys and put-node-key
                           (default: kwild on PATH).

Examples:
  scripts/ssm-secrets.sh --stage dev --dev-prefix alice gen-node-keys 2
  scripts/ssm-secrets.sh --stage prod put-node-key 0 ./node-1/nodekey.json
  openssl rand -hex 32 | scripts/ssm-secrets.sh --stage prod put-session-secret -
EOF
}

die() {
  echo "ssm-secrets: $*" >&2
  exit 1
}

# Same rules as ValidatePrefix and ValidateKmsKeyArn in lib/secrets.
PREFIX_RE='^(/[A-Za-z0-9_.-]+)+$'
KMS_KEY_ARN_RE='^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]+$'
# Same rule as the devPrefix context in config.
DEV_PREFIX_RE='^[A-Za-z0-9-]*$'

STAGE=""
DEV_PREFIX=""
PREFIX=""
KMS_KEY_ARN=""
REGION=""
PROFILE=""
KEY_TYPE="secp256k1"
OVERWRITE=false
DRY_RUN=false

need_value() {
  if [ "$#" -lt 2 ] || [ -z "$2" ]; then
    die "$1 needs a value"
  fi
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --stage) need_value "$@"; STAGE="$2"; shift 2 ;;
    --dev-prefix) need_value "$@"; DEV_PREFIX="$2"; shift 2 ;;
    --prefix) need_value "$@"; PREFIX="$2"; shift 2 ;;
    --kms-key-arn) need_value "$@"; KMS_KEY_ARN="$2"; shift 2 ;;
    --region) need_value "$@"; REGION="$2"; shift 2 ;;
    --profile) need_value "$@"; PROFILE="$2"; shift 2 ;;
    --key-type) need_value "$@"; KEY_TYPE="$2"; shift 2 ;;
    --overwrite) OVERWRITE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h | --help) usage; exit 0 ;;
    --) shift; break ;;
    -*) die "unknown option $1 (see --help)" ;;
    *) break ;;
  esac
done

[ "$#" -ge 1 ] || { usage >&2; exit 1; }
COMMAND="$1"
shift

# --- Where the parameters go -------------------------------------------------

if [ -z "$PREFIX" ]; then
  case "$STAGE" in
    prod | dev) ;;
    "") die "--stage is required (prod or dev) unless --prefix is set" ;;
    *) die "--stage must be prod or dev" ;;
  esac
  [[ "$DEV_PREFIX" =~ $DEV_PREFIX_RE ]] || die "--dev-prefix may hold only letters, digits and '-'"
  PREFIX="/tsn/$STAGE"
  if [ -n "$DEV_PREFIX" ]; then
    PREFIX="$PREFIX/$DEV_PREFIX"
  fi
fi
[[ "$PREFIX" =~ $PREFIX_RE ]] || die "SSM prefix $PREFIX must look like /a/b, using letters, digits, '.', '_' and '-'"
prefix_lower="$(printf '%s' "$PREFIX" | tr '[:upper:]' '[:lower:]')"
for reserved in /aws /ssm; do
  case "$prefix_lower" in
    "$reserved" | "$reserved"/*) die "SSM prefix $PREFIX is under the reserved $reserved path" ;;
  esac
done
if [ -n "$KMS_KEY_ARN" ] && ! [[ "$KMS_KEY_ARN" =~ $KMS_KEY_ARN_RE ]]; then
  die "--kms-key-arn must be a KMS key ARN (arn:aws:kms:<region>:<account>:key/<id>), not an alias"
fi

node_key_parameter() { printf '%s/node/%d/nodekey' "$PREFIX" "$1"; }
session_secret_parameter() { printf '%s/kgw/session_secret' "$PREFIX"; }

# --- Helpers -----------------------------------------------------------------

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

AWS_ARGS=()
if [ -n "$REGION" ]; then AWS_ARGS+=(--region "$REGION"); fi
if [ -n "$PROFILE" ]; then AWS_ARGS+=(--profile "$PROFILE"); fi

aws_cli() {
  aws ${AWS_ARGS[@]+"${AWS_ARGS[@]}"} "$@"
}

KWILD="${KWILD_CLI_PATH:-kwild}"

require_tools() {
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool is required but was not found"
  done
}

is_count() { [[ "$1" =~ ^[1-9][0-9]*$ ]]; }
is_index() { [[ "$1" =~ ^(0|[1-9][0-9]*)$ ]]; }

# read_input <file|-> <dest>: copies the value to dest without printing it.
read_input() {
  local src="$1" dest="$2"
  if [ "$src" = "-" ]; then
    if [ -t 0 ]; then
      echo "Paste the value, then press Ctrl-D." >&2
    fi
    cat >"$dest"
  else
    if [ ! -r "$src" ] || [ -d "$src" ]; then
      die "cannot read $src"
    fi
    cat -- "$src" >"$dest"
  fi
  [ -s "$dest" ] || die "the input is empty"
}

# normalize_node_key <in> <out>: writes the nodekey.json kwild reads,
# {"key":"<hex>","type":"<key type>"}, from either input form. jq errors are
# dropped because they may quote the input.
normalize_node_key() {
  local in="$1" out="$2"
  if jq -e 'type == "object"' "$in" >/dev/null 2>&1; then
    jq -ce '
      if (.key | type) == "string"
        and (.key | test("^(0x)?[0-9a-fA-F]+$"))
        and (.type == "secp256k1" or .type == "ed25519")
      then {key: (.key | ltrimstr("0x") | ascii_downcase), type: .type}
      else error("not a node key file") end' "$in" >"$out" 2>/dev/null ||
      die "the input is JSON but not a nodekey.json with a hex key and a secp256k1 or ed25519 type"
  else
    jq -Rsce '
      gsub("^\\s+|\\s+$"; "")
      | capture("^(0x)?(?<key>[0-9a-fA-F]+)(#(?<type>secp256k1|ed25519))?$")
      | {key: (.key | ascii_downcase), type: (.type // "secp256k1")}' "$in" >"$out" 2>/dev/null ||
      die "the input is neither a nodekey.json nor a hex private key with an optional #secp256k1 or #ed25519 suffix"
  fi
}

# node_id <nodekey.json>: prints the node ID (<public key hex>#<key type>).
# kwild's output also holds the private key, so it goes to a file and only
# the node ID is read back.
node_id() {
  local info="$WORKDIR/key-info.json" id
  "$KWILD" key info --key-file "$1" --output json >"$info" 2>/dev/null ||
    die "kwild could not read the node key"
  id="$(jq -r '.result.node_id // empty' "$info" 2>/dev/null)" || true
  rm -f "$info"
  [ -n "$id" ] || die "kwild did not return a node ID for the node key"
  printf '%s' "$id"
}

# stop_if_any_exists <name>...: without --overwrite, stops before any write
# when a parameter is already there.
stop_if_any_exists() {
  local name count found=()
  if [ "$OVERWRITE" = true ]; then
    return 0
  fi
  for name in "$@"; do
    count="$(aws_cli ssm describe-parameters \
      --parameter-filters "Key=Name,Option=Equals,Values=$name" \
      --query 'length(Parameters)' --output text)" ||
      die "could not check whether $name exists"
    if [ "$count" != "0" ]; then
      found+=("$name")
    fi
  done
  if [ "${#found[@]}" -gt 0 ]; then
    die "already in SSM: ${found[*]}. Pass --overwrite to replace them."
  fi
}

# put_secure <name> <value file> <description>
put_secure() {
  local name="$1" value_file="$2" description="$3" input="$WORKDIR/put-parameter.json"
  jq -n \
    --arg name "$name" \
    --arg description "$description" \
    --arg kms "$KMS_KEY_ARN" \
    --argjson overwrite "$OVERWRITE" \
    --rawfile value "$value_file" '
      {Name: $name, Description: $description, Type: "SecureString",
       Value: ($value | rtrimstr("\n")), Overwrite: $overwrite}
      + (if $kms == "" then {} else {KeyId: $kms} end)' >"$input" 2>/dev/null ||
    die "could not build the request for $name"
  aws_cli ssm put-parameter --cli-input-json "file://$input" --output text >/dev/null ||
    die "could not store $name"
  rm -f "$input"
}

dry_run_print() {
  echo "Dry run: these parameters would be written (no AWS calls made):" >&2
  printf '%s\n' "$@"
}

# --- Commands ----------------------------------------------------------------

cmd_gen_node_keys() {
  [ "$#" -eq 1 ] || die "usage: gen-node-keys <count>"
  local count="$1" i names=() ids=() key_file id
  is_count "$count" || die "<count> must be a positive whole number"
  case "$KEY_TYPE" in
    secp256k1 | ed25519) ;;
    *) die "--key-type must be secp256k1 or ed25519" ;;
  esac
  for ((i = 0; i < count; i++)); do
    names+=("$(node_key_parameter "$i")")
  done

  if [ "$DRY_RUN" = true ]; then
    dry_run_print "${names[@]}"
    return 0
  fi

  require_tools aws jq "$KWILD"
  stop_if_any_exists "${names[@]}"
  for ((i = 0; i < count; i++)); do
    key_file="$WORKDIR/nodekey-$i.json"
    "$KWILD" key gen "$KEY_TYPE" --key-file "$key_file" >/dev/null 2>&1 ||
      die "kwild key gen failed for node $((i + 1))"
    id="$(node_id "$key_file")"
    put_secure "${names[$i]}" "$key_file" "TN node-$((i + 1)) node key"
    rm -f "$key_file"
    ids+=("$id")
    echo "Stored ${names[$i]} (node-$((i + 1)), node ID $id)" >&2
  done

  local joined
  joined="$(IFS=,; printf '%s' "${ids[*]}")"
  echo "NODE_PUBLIC_KEYS=$joined"
}

cmd_put_node_key() {
  [ "$#" -eq 2 ] || die "usage: put-node-key <index> <file|->"
  local index="$1" src="$2" name raw="$WORKDIR/node-key-input" key_file="$WORKDIR/nodekey.json" id
  is_index "$index" || die "<index> must be 0 or a positive whole number (0 is node-1)"
  name="$(node_key_parameter "$index")"
  require_tools jq
  read_input "$src" "$raw"
  normalize_node_key "$raw" "$key_file"
  rm -f "$raw"

  if [ "$DRY_RUN" = true ]; then
    dry_run_print "$name"
    return 0
  fi

  require_tools aws "$KWILD"
  id="$(node_id "$key_file")"
  stop_if_any_exists "$name"
  put_secure "$name" "$key_file" "TN node-$((index + 1)) node key"
  echo "Stored $name (node-$((index + 1)))" >&2
  echo "$id"
}

cmd_put_session_secret() {
  [ "$#" -eq 1 ] || die "usage: put-session-secret <file|->"
  local src="$1" name raw="$WORKDIR/session-secret"
  name="$(session_secret_parameter)"
  require_tools jq
  read_input "$src" "$raw"
  jq -Rse 'rtrimstr("\n") | length > 0 and (test("[\r\n]") | not)' "$raw" >/dev/null 2>&1 ||
    die "the session secret must be one non-empty line"

  if [ "$DRY_RUN" = true ]; then
    dry_run_print "$name"
    return 0
  fi

  require_tools aws
  stop_if_any_exists "$name"
  put_secure "$name" "$raw" "TN gateway session secret"
  echo "Stored $name" >&2
}

case "$COMMAND" in
  gen-node-keys) cmd_gen_node_keys "$@" ;;
  put-node-key) cmd_put_node_key "$@" ;;
  put-session-secret) cmd_put_session_secret "$@" ;;
  help) usage ;;
  *) die "unknown command $COMMAND (see --help)" ;;
esac
