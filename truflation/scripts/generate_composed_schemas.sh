#!/usr/bin/env bash


# for each line ./composed_streams.csv
# parse  columns being: 1: parent_stream, 2: stream, 3: weight with presence of the header

# for each parent_stream, create a group of streams
# then for each parent_stream group, run

# run: go run ../cli.go -name <parent_stream> -import <1st stream>:<1st weight>,<2nd stream>:<2nd weight> -out temp_composed_schemas/<parent_stream>.json

cd "$(dirname "$0")"

declare -A stream_groups

# Read the CSV file, skipping the header
while IFS=, read -r parent_stream stream weight; do
    # to avoid incomplete lines
    if [[ -n $parent_stream ]]; then
      # Append the stream and weight to the parent stream's entry in the associative array
      stream_groups["$parent_stream"]+="/$stream:$weight,"
    fi
done < <(tail -n +2 ../composed_streams.csv | grep .)

# make sure it is clean
rm -rf ./temp_composed_schemas

# Creates the directory if it doesn't exist
mkdir -p ./temp_composed_schemas


# Iterate over the associative array to run commands for each parent stream
for parent_stream in "${!stream_groups[@]}"; do
    # Remove the trailing comma from the list of streams and weights
    streams_weights=${stream_groups[$parent_stream]%,}
    # Run the command, substituting the placeholders with the actual values
    go run ../schema_gen/cli/cli.go -name "$parent_stream" -import "$streams_weights" -out "./temp_composed_schemas/$parent_stream.json"
done

echo "Composed Schemas Generated"