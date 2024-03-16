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

    while true; do
      # if we're able to query the database already, we may skip this file
      error_logs=$(./../.build/kwil-cli database call -a=get_index date:"" date_to:"" -n="$db_name" 2>&1 || true)
      if [[ $error_logs != *"error"* ]]; then
        echo "Skipping file: $file"
        break
      fi

      output=$(./../.build/kwil-cli database batch --path "$file" --action add_record --name="$db_name" --values created_at:$(date +%s) 2>&1 || true)
      echo "$output"
      if [[ $output =~ "invalid nonce" ]]; then
        echo "Error nonce, retrying file immediately with expected nonce: $file"
        expected_nonce=$(echo "$output" | grep -oP 'expected \K[0-9]+')
        ../../.build/kwil-cli database batch --path "$file" --action add_record --name=$db_name --values created_at:$(date +%s) --nonce "$expected_nonce"
      elif [[ $output =~ "error" ]]; then
        echo "Error deploying file: $file"
        pending_files+=("$file")
      else
        break
      fi
    done

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
