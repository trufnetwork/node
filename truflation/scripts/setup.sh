#!/usr/bin/env bash

# on error, exit
set -e

cd "$(dirname "$0")"

# run it in parallel, assign process pid to kwild_pid
../../.build/kwild --autogen &> /dev/null & kwild_pid=$!

function cleanup {
  echo "Killing kwild process $kwild_pid"
  kill -9 $kwild_pid
}

# trap the cleanup function to SIGINT, SIGTERM and EXIT signals
trap cleanup SIGINT SIGTERM EXIT

# to make sure kwild is ready
sleep 5;


# if $PRIVATE_KEY is setup and config does not exist, we create with
if [ -n "$PRIVATE_KEY" ] && [ ! -f ~/.kwil_cli/config.json ]; then
  mkdir -p ~/.kwil_cli
  echo "{\"private_key\":\"$PRIVATE_KEY\",\"grpc_url\":\"http://localhost:8080\",\"chain_id\":\"\"}" > ~/.kwil_cli/config.json
fi

# smoke test about kwil-cli
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

sleep 10;

# smoke test
output=$(../../.build/kwil-cli database call -a=get_index date:"2023-01-01" date_to:"2023-12-31" -n=com_numbeo_us_bread_3m_avg)
# output should contain `2023-12-01` to be sure it worked
if [[ $output != *"2023-12-01"* ]]; then
  echo "Smoke test failed: $output"
  exit 1
fi

echo "Success!"