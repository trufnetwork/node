#!/usr/bin/env bash

cd "$(dirname "$0")"

echo "Deploying primitive schemas"

# fore each csv file in temp_csv
# drop the db, then run the deploy command
for file in ./temp_csv/*.csv; do
    filename=$(basename "$file")
    filename="${filename%.*}"
    echo "Dropping $filename"
    ../../.build/kwil-cli database drop "$filename" --sync
    echo "Deploying $filename"
    ../../.build/kwil-cli database deploy -p=../base_schema/base_schema.kf --sync --name="$filename" --sync
    echo "Done"
done

echo "Done deploying primitive schemas"

echo "Deploying composed schemas"

# for each file in temp_composed_schemas/*.json
# drop the db, then run the deploy command
for file in ./temp_composed_schemas/*.json; do
    filename=$(basename "$file")
    filename="${filename%.*}"
    echo "Dropping $filename"
    ../../.build/kwil-cli database drop "$filename" --sync
    echo "Deploying $filename"
    ../../.build/kwil-cli database deploy -p="$file" --type json --name "$filename" --sync
    echo "Done"
    echo "-"
done

echo "All done"
