#!/usr/bin/env bash

-set e

cd "$(dirname "$0")"

function check_database_for_list {
    expected_list=("$@")
    # remove path and extension, any that might be there
    # filename="${fullfile##*/}"
    expected_list=($(for file in "${expected_list[@]}"; do filename="${file##*/}"; echo "${filename%.*}"; done))

    # with ../../.build/kwil-cli database list --self
    # output for each is this:
    #   DBID: <dbid>
    #          Name: <name>
    #          Owner: <owner-address>
    # we want <name>

    actual_list=($(../../.build/kwil-cli database list --self | grep "Name:" | awk '{print $2}'))


    # for each expected_list
    # check if it's in actual_list
    for expected in "${expected_list[@]}"; do
        if [[ ! " ${actual_list[@]} " =~ " ${expected} " ]]; then
            echo "Database $expected not found"
            exit 1
        fi
    done

    echo "All databases found"
}

# should come from --skip-drop flag
skip_drop=false

# set necessary flags to variables
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --skip-drop)
            skip_drop=true
            shift
            ;;
        *)
            echo "Unknown flag: $key"
            exit 1
            ;;
    esac
done

echo "Deploying primitive schemas"

primitive_files_list=($(ls ./temp_csv/*.csv))
primitive_count_left=${#primitive_files_list[@]}

composed_files_list=($(ls ./temp_composed_schemas/*.json))
composed_count_left=${#composed_files_list[@]}


# if --skip-drop is not set
# we loop through both list and drop the db
# then run the deploy command
all_files_list=("${primitive_files_list[@]}" "${composed_files_list[@]}")
if [ "$skip_drop" = false ]; then
    for file in "${all_files_list[@]}"; do
        filename=$(basename "$file")
        filename="${filename%.*}"
        echo "Dropping $filename"
        ../../.build/kwil-cli database drop "$filename"
    done

    sleep 10
fi

# fore each csv file in temp_csv
# drop the db, then run the deploy command
for file in "${primitive_files_list[@]}"; do
    filename=$(basename "$file")
    filename="${filename%.*}"
    echo "Deploying $filename"
    ../../.build/kwil-cli database deploy -p=../base_schema/base_schema.kf --name="$filename"

    primitive_count_left=$(($primitive_count_left-1))
    echo "Done, $primitive_count_left to go"
done

echo "Done deploying primitive schemas"

echo "Deploying composed schemas"

# for each file in temp_composed_schemas/*.json
# drop the db, then run the deploy command
for file in "${composed_files_list[@]}"; do
    filename=$(basename "$file")
    filename="${filename%.*}"
    echo "Deploying $filename"
    ../../.build/kwil-cli database deploy -p="$file" --type json --name "$filename"

    composed_count_left=$(($composed_count_left-1))
    echo "Done, $composed_count_left to go"
done

sleep 10

echo "Checking deployed databases"
check_database_for_list "${primitive_files_list[@]}"
check_database_for_list "${composed_files_list[@]}"

echo "All done"