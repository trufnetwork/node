#!/usr/bin/env bash

cd "$(dirname "$0")"

files=($(ls ./temp_csv/*.csv))
files_count=${#files[@]}
echo "Adding primitive schemas, total files: $files_count"


for file in "${files[@]}"; do
  echo "Processing file: $file"
  db_name=$(basename "$file")
  db_name="${db_name%.*}"

  # if we're able to query the database already, we may skip this file
  error_logs=$(../../.build/kwil-cli database call -a=get_index date:"" date_to:"" -n="$db_name" 2>&1)

  if [[ $error_logs != *"error"* ]]; then
    echo "Skipping file: $file"
    continue
  fi


  ../../.build/kwil-cli database batch --path "$file" --action add_record --name=$db_name --values created_at:$(date +%s)
  echo "Done processing file: $file. More $((files_count--)) to go"
done
