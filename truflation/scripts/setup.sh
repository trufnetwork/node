#!/usr/bin/env bash

# on error, exit
set -e

cd "$(dirname "$0")"

# run it in parallel, assign process pid to kwild_pid
../../.build/kwild --autogen & kwild_pid=$!

# to make sure kwild is ready
sleep 5;

mkdir -p ~/.kwil_cli
echo "{\"private_key\":\"$PRIVATE_KEY\",\"grpc_url\":\"http://localhost:8080\",\"chain_id\":\"\"}" > ~/.kwil_cli/config.json

# smoke test about kwil-cli
echo "SMOKE TEST"
test_content=$(../../.build/kwil-cli database list --self)

# if contains Error, error out
# or if contains "must have a configured wallet"
if [[ $test_content == *"Error"* ]] || [[ $test_content == *"must have a configured wallet"* ]]; then
  echo "kwil-cli error: $test_content"
  exit 1
fi

# if we kwild is not running, error out
if ! kill -0 $kwild_pid; then
  echo "kwild process is not running"
  exit 1
fi


python ./produce_source_maps/process_all.py
./generate_clean_csv_from_raw.sh
./generate_composed_schemas.sh
./database_deploy.sh --skip-drop
./database_add_primitives.sh

echo "Killing kwild process $kwild_pid"
kill -9 $kwild_pid