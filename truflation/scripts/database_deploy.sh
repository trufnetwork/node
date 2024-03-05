
#!/usr/bin/env bash

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

cd "$(dirname "$0")"

echo "Deploying primitive schemas"

primitive_files_list=($(ls ./temp_csv/*.csv))
primitive_count_left=${#primitive_files_list[@]}

# fore each csv file in temp_csv
# drop the db, then run the deploy command
for file in "${primitive_files_list[@]}"; do
    filename=$(basename "$file")
    filename="${filename%.*}"
    if [ "$skip_drop" = false ]; then
        echo "Dropping $filename"
        ../../.build/kwil-cli database drop "$filename" --sync
    fi
    echo "Deploying $filename"
    ../../.build/kwil-cli database deploy -p=../base_schema/base_schema.kf --sync --name="$filename" --sync

    primitive_count_left=$(($primitive_count_left-1))
    echo "Done, $primitive_count_left to go"
done

echo "Done deploying primitive schemas"

echo "Deploying composed schemas"

composed_files_list=($(ls ./temp_composed_schemas/*.json))
composed_count_left=${#composed_files_list[@]}

# for each file in temp_composed_schemas/*.json
# drop the db, then run the deploy command
for file in "${composed_files_list[@]}"; do
    filename=$(basename "$file")
    filename="${filename%.*}"
    if [ "$skip_drop" = false ]; then
        echo "Dropping $filename"
        ../../.build/kwil-cli database drop "$filename" --sync
    fi
    echo "Deploying $filename"
    ../../.build/kwil-cli database deploy -p="$file" --type json --name "$filename" --sync

    composed_count_left=$(($composed_count_left-1))
    echo "Done, $composed_count_left to go"
done

echo "All done"