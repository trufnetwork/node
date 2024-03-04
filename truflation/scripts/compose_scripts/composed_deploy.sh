#!/usr/bin/env bash

cd "$(dirname "$0")"

# for each file in temp_composed_schemas/*.json
# drop the db, then run

for file in ../temp_composed_schemas/*.json; do
    filename=$(basename "$file")
    filename="${filename%.*}"
    echo "Dropping $filename"
    ../../../.build/kwil-cli database drop "$filename"
    echo "Deploying $filename"
    ../../../.build/kwil-cli database deploy -p="$file" --type json --name "$filename" --sync
    echo "Done"
    echo " "
done