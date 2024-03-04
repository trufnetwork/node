#!/usr/bin/env bash

cd "$(dirname "$0")"

# delete the temp_csv folder
rm -rf ./temp_csv

# comes from --filter-by-all-tables flag
# It should permit us to filter the csv files that we want to process, extracting it from ./produce_source_maps/all_tables.csv
filter_by_all_tables=false

# set necessary flags to variables
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --filter-by-all-tables)
            filter_by_all_tables=true
            shift
            ;;
        *)
            echo "Unknown flag: $key"
            exit 1
            ;;
    esac
done


# create the temp_csv folder
mkdir -p ./temp_csv

files_list=($(ls ./raw_from_db/*.csv))

if [ "$filter_by_all_tables" = true ]; then
  echo "Filtering by all_tables.csv"

  tables_file="./produce_source_maps/all_tables.csv"

  # First, find the column numbers for database_name and is_primitive
  header=$(head -1 "$tables_file")
  database_name_col=$(echo $header | awk -F, '{for(i=1;i<=NF;i++) if($i=="database_name") print i}')
  is_primitive_col=$(echo $header | awk -F, '{for(i=1;i<=NF;i++) if($i=="is_primitive") print i}')

  # Then, filter the rows that have is_primitive=True and extract the database_name column
  IFS=$'\n' read -r -d '' -a to_be_filtered < <(awk -F, -v db="$database_name_col" -v prim="$is_primitive_col" -v OFS=',' '{if($prim=="True") print $db}' "$tables_file" && printf '\0')

  # Assuming files_list is an array of filenames you want to filter
  # Example: files_list=("table1.csv" "table2.csv" ...)

  # Then, filter the files_list that have the to_be_filtered
  filtered_files_list=()
  for file in "${files_list[@]}"; do
    for table in "${to_be_filtered[@]}"; do
      if [[ "$file" == *"$table.csv"* ]]; then
        filtered_files_list+=("$file")
        break # Break the inner loop if a match is found
      fi
    done
  done
fi




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
  }' "$file" > "./temp_csv/$table_name.csv"  # this is the original line
  #  awk -F, 'BEGIN {OFS=","} ("uuidgen" | getline uuid) > 0 {if (NR==1) print "id,date_value,value"; else print uuid, $1, int($2*1000)} {close("uuidgen")}' "$file" > ./temp_csv/"$table_name".csv

  files_count_left=$(($files_count_left-1))
  echo "Done, $files_count_left to go"
done