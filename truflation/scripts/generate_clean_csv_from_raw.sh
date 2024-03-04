#!/usr/bin/env bash

cd "$(dirname "$0")"

# delete the temp_csv folder
rm -rf ./temp_csv

# create the temp_csv folder
mkdir -p ./temp_csv

files_list=($(ls ./raw_from_db/*.csv))
files_count_left=${#files_list[@]}

# for each file on ./raw_from_db, create temp file that have cleaned data
# The header will be id, date_value, value for each file
# for each column, multiply the value by 1000, make it an int
# save the file to ./temp_csv
for file in "${files_list[@]}"; do
  echo "Processing $file"
  table_name=$(basename "$file" .csv)
  awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, int($2*1000)} {close("uuidgen")}' "$file" > ./temp_csv/"$table_name".csv
  files_count_left=$(($files_count_left-1))
  echo "Done, $files_count_left to go"
done