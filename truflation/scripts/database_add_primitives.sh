#!/usr/bin/env bash

cd "$(dirname "$0")"

files=($(ls ./temp_csv/*.csv))
files_count=${#files[@]}
echo "Adding primitive schemas, total files: $files_count"


for file in "${files[@]}"; do
  echo "Processing file: $file"
  db_name=$(basename "$file")
  db_name="${db_name%.*}"
  ../../.build/kwil-cli database batch --sync --path "$file" --action add_record --name=$db_name --values created_at:$(date +%s) --sync
  echo "Done processing file: $file. More $((files_count--)) to go"
done
