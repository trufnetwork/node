#!/usr/bin/env bash

for file in ./temp_csv/*.csv; do
  echo "Processing file: $file"
  # ../../.build/kwil-cli database batch --sync --path ./test_samples/transformed/com_yahoo_finance_corn_futures.csv -
  # -action add_record --name=com_yahoo_finance_corn_futures --sync
  db_name=$(basename "$file")
  db_name="${db_name%.*}"
  ../../.build/kwil-cli database batch --sync --path "$file" --action add_record --name=$db_name --values created_at:$(date +%s) --sync
  echo "Done processing file: $file"
done
