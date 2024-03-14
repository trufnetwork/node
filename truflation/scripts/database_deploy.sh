#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"

check_database_for_list_result=()
function check_database_for_list {
    expected_file_list=("$@")
    # remove path and extension, any that might be there
    # filename="${fullfile##*/}"
    expected_db_list=($(for file in "${expected_file_list[@]}"; do filename="${file##*/}"; echo "${filename%.*}"; done))

    # with ../../.build/kwil-cli database list --self
    # output for each is this:
    #   DBID: <dbid>
    #          Name: <name>
    #          Owner: <owner-address>
    # we want <name>

    actual_list=($(../../.build/kwil-cli database list --self | grep "Name:" | awk '{print $2}'))

    missing_files=()

    # for each expected_db_list
    # check if it's in actual_list
    for index in "${!expected_db_list[@]}"; do
        expected=${expected_db_list[index]}
        if [[ ! " ${actual_list[@]} " =~ " ${expected} " ]]; then
            missing_files+=("${expected_file_list[index]}")
        fi
    done

    # return missing databases from function
    check_database_for_list_result=("${missing_files[@]}")
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

max_retries=3
retry=0

# if --skip-drop is not set
# we loop through both list and drop the db
# then run the deploy command
all_files_list=("${primitive_files_list[@]}" "${composed_files_list[@]}")
if [ "$skip_drop" = false ]; then
    for file in "${all_files_list[@]}"; do
        filename=$(basename "$file")
        filename="${filename%.*}"
        echo "Dropping $filename"
        while true; do
            output=$(../../.build/kwil-cli database drop "$filename" 2>&1 || true)
            echo $output
            if [[ $output =~ "invalid nonce" ]]; then
              echo "Error nonce, retrying file immediately: $file"
              expected_nonce=$(echo $output | grep -oP 'expected \K[0-9]+')
              ../../.build/kwil-cli database drop "$filename" --nonce $expected_nonce
            elif [[ $output =~ "error" ]]; then
              echo "Error dropping file: $file"
            else
                break
            fi
        done
    done

    sleep 10
fi


# we define here so we avoid running the command many times. One schema fits all
transformed_base_schema=$(exec ./use_base_schema.sh)

function deploy_primitives {
  echo "Deploying primitive schemas"

  # if there are no primitive schemas, return
  if [ ${#primitive_files_list[@]} -eq 0 ]; then
      return
  fi

  # fore each csv file in temp_csv
  # drop the db, then run the deploy command
  for file in "${primitive_files_list[@]}"; do
      filename=$(basename "$file")
      filename="${filename%.*}"
      echo "Deploying $filename"
      while true; do
          output=$(../../.build/kwil-cli database deploy -p=<(echo "$transformed_base_schema") --name="$filename" 2>&1 || true)
          echo $output
          if [[ $output =~ "invalid nonce" ]]; then
            echo "Error nonce, retrying file immediately: $file"
            expected_nonce=$(echo $output | grep -oP 'expected \K[0-9]+')
            ../../.build/kwil-cli database deploy -p=<(echo "$transformed_base_schema") --name="$filename" --nonce $expected_nonce
          elif [[ $output =~ "error" ]]; then
            echo "Error deploying file: $file"
          else
              break
          fi
      done

      primitive_count_left=$(($primitive_count_left-1))
      echo "Done, $primitive_count_left to go"
  done
}

function deploy_composed {
  # if there are no composed schemas, return
  if [ ${#composed_files_list[@]} -eq 0 ]; then
      return
  fi
  echo "Deploying composed schemas"
  # for each file in temp_composed_schemas/*.json
  # drop the db, then run the deploy command
  for file in "${composed_files_list[@]}"; do
      filename=$(basename "$file")
      filename="${filename%.*}"
      echo "Deploying $filename"
      while true; do
          output=$(../../.build/kwil-cli database deploy -p="$file" --type json --name "$filename" 2>&1 || true)
          echo $output
          if [[ $output =~ "invalid nonce" ]]; then
            echo "Error nonce, retrying file immediately: $file"
            expected_nonce=$(echo $output | grep -oP 'expected \K[0-9]+')
            ../../.build/kwil-cli database deploy -p="$file" --type json --name "$filename" --nonce $expected_nonce
          elif [[ $output =~ "error" ]]; then
            echo "Error deploying file: $file"
          else
              break
          fi
      done

      composed_count_left=$(($composed_count_left-1))
      echo "Done, $composed_count_left to go"
  done
}

while [ $retry -lt $max_retries ]; do
  deploy_primitives
  deploy_composed

  sleep 10

  echo "Checking deployed databases"

  check_database_for_list "${primitive_files_list[@]}"
  primitive_missing=("${check_database_for_list_result[@]}")

  check_database_for_list "${composed_files_list[@]}"
  composed_missing=("${check_database_for_list_result[@]}")

  if [ ${#primitive_missing[@]} -eq 0 ] && [ ${#composed_missing[@]} -eq 0 ]; then
    echo "All databases deployed successfully"
    break
  else
    retry=$(($retry+1))
    echo "Some databases are missing, retrying for the $retry time"

    if [ ${#primitive_missing[@]} -ne 0 ]; then
      echo "Missing primitive databases: ${#primitive_missing[@]}"
      primitive_files_list=("${primitive_missing[@]}")
      primitive_count_left=${#primitive_files_list[@]}
    fi

    if [ ${#composed_missing[@]} -ne 0 ]; then
      echo "Missing composed databases: ${#composed_missing[@]}"
      composed_files_list=("${composed_missing[@]}")
      composed_count_left=${#composed_files_list[@]}
    fi
  fi
done

if [ $retry -eq $max_retries ]; then
  echo "Max retries reached, some databases are still missing"
  echo "Missing primitive databases: ${primitive_files_list[@]}"
  echo "Missing composed databases: ${composed_files_list[@]}"

  exit 1
fi


echo "All done"
