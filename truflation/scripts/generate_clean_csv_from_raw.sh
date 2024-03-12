#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"

# delete the temp_csv folder
rm -rf ./temp_csv

# create the temp_csv folder
mkdir -p ./temp_csv

## FILTER BY TABLE FILE
# We do this to have more fine-grained control over which tables we want to process, deploy, etc

tables_file="./produce_source_maps/all_tables.csv"

# First, find the column numbers for database_name and is_primitive
header=$(head -1 "$tables_file")
database_name_col=$(echo $header | awk -F, '{for(i=1;i<=NF;i++) if($i=="database_name") print i}')
is_primitive_col=$(echo $header | awk -F, '{for(i=1;i<=NF;i++) if($i=="is_primitive") print i}')

db_names=()

# Then, filter the rows that have is_primitive=True and extract the database_name column, pushing to db_names
IFS=$'\n' read -r -d '' -a db_names < <(awk -F, -v db="$database_name_col" -v prim="$is_primitive_col" -v OFS=',' '{if($prim=="True") print $db}' "$tables_file" && printf '\0')

# Now, we have the list of database names that we want to process

# we build our list of files to process
files_list=()
for db_name in "${db_names[@]}"; do
  # if the file doesn't exist, we error out
  db_file="./raw_from_db/$db_name.csv"
  if [ ! -f $db_file ]; then
    echo "$db_file does not exist"
    exit 1
  fi
  files_list+=($db_file)
done

files_count_left=${#files_list[@]}

# for each file on ./raw_from_db, create temp file that have cleaned data
# The header will be id, date_value, value for each file
# for each column, multiply the value by 1000, make it an int
# save the file to ./temp_csv
for file in "${files_list[@]}"; do
  echo "Processing $file"
  table_name=$(basename "$file" .csv)

  awk -F, 'BEGIN {OFS=","}
  {
    if (NR == 1) {
      print "id","date_value","value" # our expected header
    } else {
      # TODO this is a hack, we should handle multiple values for a date
      # and we should remove it when we support it
      if (!seen[$1]++) {
        # Generate a UUID for each unique row and output the modified row
        "uuidgen" | getline uuid
        print uuid, $1, int($2*1000)
        close("uuidgen")
      }
    }
  }' "$file" > "./temp_csv/$table_name.csv" &  # this is the original line
  #  awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, int($2*1000)} {close("uuidgen")}' "$file" > ./temp_csv/"$table_name".csv

  files_count_left=$(($files_count_left-1))

  # for each time the count is divisible by 10, we wait
if [ $(($files_count_left % 50)) -eq 0 ]; then
    wait
  echo "Done, $files_count_left to go"
  fi

done