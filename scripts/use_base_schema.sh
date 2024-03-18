#!/usr/bin/env bash

cd "$(dirname "$0")"

# usage: use_base_schema <WHITELIST_WALLETS>
# will echo the content of the base_schema file with the WHITELIST_WALLETS replaced by provided value

# it may also be empty, in this case only the owner will be able to read the DB

# if you want to use the content as a file, you may use the <( ... ) syntax
# example:

# ../../.build/kwil-cli database deploy -p=<(./use_base_schema.sh  "0x123,0x456") --name="my_db"

# expect 1 or less parameters
if [ $# -gt 1 ]; then
  echo "Illegal number of parameters"
  exit 1
fi

base_schema_content=$(cat ./../internal/schemas/base_schema.kf)
TO_BE_REPLACED="\$WHITELIST_WALLETS\$"
# should be replaced by $1 or an empty string
REPLACED_FOR=${1:-""}
content=${base_schema_content//$TO_BE_REPLACED/$REPLACED_FOR}
echo "$content"
