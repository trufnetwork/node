#!/usr/bin/env bash

cd "$(dirname "$0")"

# usage: use_primitive_contract <WHITELIST_WALLETS>
# will echo the content of the primitive_stream.kf contract file with the WHITELIST_WALLETS replaced by provided value

# it may also be empty, in this case only the owner will be able to read the DB

# if you want to use the content as a file, you may use the <( ... ) syntax
# example:

# ../../.build/kwil-cli database deploy -p=<(./use_primitive_contract.sh  "0x123,0x456") --name="my_db"

# expect 1 or less parameters
if [ $# -gt 1 ]; then
  echo "Illegal number of parameters"
  exit 1
fi

primitive_contract_content=$(cat ./../internal/contracts/primitive_stream.kf)
TO_BE_REPLACED="\$WHITELIST_WALLETS\$"
TO_BE_REPLACED_WRITE="\$WRITE_WHITELIST_WALLETS\$"
# should be replaced by $1 or an empty string
variable=${1:-""}
# inside variable, there's a comma separated list of wallets
REPLACED_FOR=$(echo $variable | cut -d';' -f1)
REPLACED_FOR_WRITE=$(echo $variable | cut -d';' -f2)
content=${primitive_contract_content//$TO_BE_REPLACED/$REPLACED_FOR}
content=${content//$TO_BE_REPLACED_WRITE/$REPLACED_FOR_WRITE}

echo "$content"
