#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"

files=($(ls ./temp_csv/*.csv))
files_count=${#files[@]}
echo "Adding primitive schemas, total files: $files_count"

pending_files=("${files[@]}")
max_retries=5
retries=0

while [[ $retries -lt $max_retries ]]; do
  files=("${pending_files[@]}")
  pending_files=()
  for file in "${files[@]}"; do
    echo "Processing file: $file"
    db_name=$(basename "$file")
    db_name="${db_name%.*}"

    # if we're able to query the database already, we may skip this file
    error_logs=$(../../.build/kwil-cli database call -a=get_index date:"" date_to:"" -n="$db_name" 2>&1 || true)

    if [[ $error_logs != *"error"* ]]; then
      echo "Skipping file: $file"
      continue
    fi


    output=$(../../.build/kwil-cli database batch --path "$file" --action add_record --name=$db_name --values created_at:$(date +%s))
    echo "Output: $output"
    # if output contains error, add to pending_files
    if [[ $output == "error"* ]]; then
      echo "Error processing file: $file"
      pending_files+=("$file")
    fi

    echo "Done processing file: $file."
  done

  # if pending_files is empty, we're done
  if [[ ${#pending_files[@]} -eq 0 ]]; then
    echo "All files processed"
    break
  fi

  echo "Retrying $retries/$max_retries"
done


# if there's still pending files, we error out
if [[ ${#pending_files[@]} -gt 0 ]]; then
  echo "Error: There are still ${#pending_files[@]} pending files"
  exit 1
fi