#!/bin/bash
script_dir=$(dirname "$0")
file="$script_dir/04migrate-structure_2.sql"
PRIVATE_KEY="0000000000000000000000000000000000000000000000000000000000000001"
csv_path="$script_dir/times.csv"

# times has `filename,duration,start_time` columns

runcount=0

# TODO: make this script a little bit smarter
# - consider target busy time. For example, if 10% busy, it should take a median of last 5 runs, and wait 9x that time
# - make the script stop if the median is over 30 seconds (better to keep our network operational)


while true; do
    filename=$(basename $file)
    initial_time=$(date +%s)
    # start_time is the time when the script started in the format YYYY-MM-DD HH:MM:SS (UTC)
    start_time=$(date -u +%Y-%m-%d\ %H:%M:%S)
    echo "Running $runcount: $filename"
    kwil-cli exec-sql --file $file --private-key $PRIVATE_KEY --provider http://localhost:8484 --sync
    end_time=$(date +%s)
    duration=$((end_time - initial_time))
    echo "Completed in $duration seconds"
    echo "$filename,$duration,$start_time" >> $csv_path
    runcount=$((runcount + 1))
done

