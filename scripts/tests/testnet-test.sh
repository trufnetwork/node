#!/usr/bin/env bash

set -e # error out on any failed command
set -x # print all executed commands

# ensure both are pingable
PROVIDER1=http://localhost:8080
PROVIDER2=http://localhost:8081

# go to script dir
cd "$(dirname "$0")"
cd ../develop_experiments

# drop all. Don't error out
../../.build/kwil-cli database drop stream_a --sync --kwil-provider=$PROVIDER1 || true

# deploy & add records
../../.build/kwil-cli database deploy --sync -p=<(exec ../use_primitive_contract.sh) --name=stream_a --sync --kwil-provider=$PROVIDER1
../../.build/kwil-cli database batch --sync --path ./test_samples/stream_a.csv --action add_record --name=stream_a --sync --kwil-provider=$PROVIDER1

# test query on both
../../.build/kwil-cli database call -a=get_index date:"" date_to:"" -n=stream_a --kwil-provider=$PROVIDER1
../../.build/kwil-cli database call -a=get_index date:"" date_to:"" -n=stream_a --kwil-provider=$PROVIDER2
