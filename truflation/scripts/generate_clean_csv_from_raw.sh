#!/usr/bin/env bash

cd "$(dirname "$0")"

# delete the temp_csv folder
rm -rf ./temp_csv

# create the temp_csv folder
mkdir -p ./temp_csv

# for each file on ./raw_from_db, create temp file that have cleaned data
# The header will be id, date_value, value for each file
# for each column, multiply the value by 1000, make it an int
# save the file to ./temp_csv
for file in ./raw_from_db/*.csv; do
  # if file exist, delete it
  if [ -f ./temp_csv/$(basename $file) ]; then
    rm ./temp_csv/$(basename $file)
  fi
  echo "Processing $file"
  awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, int($2*1000)} {close("uuidgen")}' "$file" > ./temp_csv/$(basename $file)
done