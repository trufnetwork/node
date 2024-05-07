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

echo -e "❓ Making sure we're able to ping the TSN node\n"
expect_success "$(../.build/kwil-cli utils ping 2>&1)"