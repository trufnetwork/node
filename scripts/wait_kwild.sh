#!/usr/bin/env bash

set -e
cd "$(dirname "$0")"

grpc_url=${GRPC_URL:-"http://localhost:8080"}

# to make sure kwild is ready
for i in {1..10}; do
#  check kwil-cli is exist
  if ../.build/kwil-cli utils ping --kwil-provider=$grpc_url &> /dev/null; then
    break
  fi
  echo "Waiting for kwild to be ready"
  sleep 5
done
