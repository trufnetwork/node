#!/usr/bin/env bash

cd "$(dirname "$0")"

# file created to be used during CI tests actions. So the running node should be created from docker image

# on error, exit
set -e

SUCCESS=0
FAILURE=1

function got_generic_error() {
  echo "$1" | grep -i -E "error|fail|invalid" &> /dev/null
  if [[ $? -eq 0 ]]; then
    return $SUCCESS # found an error
  fi

  return $FAILURE
}

function expect_error() {
  echo -e "Expecting error in output: \n$1\n"

  if got_generic_error "$1"; then
    echo -e "✅ Expected error found in output\n"
    return $SUCCESS
  fi

  echo -e "❌ Expected error not found in output\n"
  return $FAILURE
}

function expect_success() {
  echo -e "Expecting success in output: \n$1\n"

  if ! got_generic_error "$1"; then
    echo -e "✅ Success found in output\n"
    return $SUCCESS
  fi

  echo -e "❌ Success not found in output (error detected)\n"
  return $FAILURE
}


# it's address is 0x304e893AdB2Ad8E8C37F4884Ad1EC3df8bA9bDcf
allowed_private_key="26aff20bde5606467627557793ebbb6162e9faf9f2d0830fd98a6f207dcf605d"
not_allowed_private_key="26aff20bde5606467627557793ebbb6162e9faf9f2d0830fd98a6f207dcf605e"
# from the docker image
owner_private_key="0000000000000000000000000000000000000000000000000000000000000001"
owner_address="7e5f4552091a69125d5dfcb7b8c2659029395bdf"

# in gh actions we can't target localhost
PROVIDER="http://127.0.0.1:8080"

echo -e "❓ Making sure we're able to call the database from own private key\n"
expect_success "$(../../.build/kwil-cli database call -a=get_index date:"2023-12-25" date_to:"2023-12-31" -n=cpi --private-key="$owner_private_key" --owner="$owner_address" 2>&1)"

echo -e "❓ Making sure we're able to call the database from an allowed private key\n"
expect_success "$(../../.build/kwil-cli database call -a=get_index date:"2023-12-25" date_to:"2023-12-31" -n=cpi --owner="$owner_address" --private-key="$allowed_private_key" 2>&1)"

echo -e "❓ Making sure we're not able to call the database from a different private key\n"
expect_error "$(../../.build/kwil-cli database call -a=get_index date:"2023-12-25" date_to:"2023-12-31" -n=cpi --owner="$owner_address" --private-key="$not_allowed_private_key" 2>&1)"