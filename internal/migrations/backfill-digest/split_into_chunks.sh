#!/bin/bash

# High-performance script to split CSV into SQL INSERT chunks
# Creates ready-to-execute SQL files with complete INSERT statements
# Usage: ./split_into_chunks.sh [input_file] [chunk_size]

set -e  # Exit on any error

# Default values
INPUT_FILE="${1:-out.csv}"
CHUNK_SIZE="${2:-100000}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/chunk"

INPUT_FILE="$SCRIPT_DIR/$INPUT_FILE"

# Check if input file exists
if [[ ! -f "$INPUT_FILE" ]]; then
    echo "Error: Input file '$INPUT_FILE' does not exist"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Clean up any existing chunks
rm -f "$OUTPUT_DIR"/chunk_*.sql

# Count total lines
TOTAL_LINES=$(wc -l < "$INPUT_FILE")
DATA_LINES=$((TOTAL_LINES - 1))

if [[ $DATA_LINES -le 0 ]]; then
    echo "Error: Input file appears to be empty or contains only header"
    exit 1
fi

echo "High-performance SQL file generation using system tools..."
echo "Input file: $INPUT_FILE"
echo "Total lines: $TOTAL_LINES (header + $DATA_LINES data lines)"
echo "Chunk size: $CHUNK_SIZE data lines per chunk"
echo "Output directory: $OUTPUT_DIR"

# Method: Generate SQL INSERT statements for each chunk using awk (much faster)
echo "Generating SQL INSERT statements using awk..."

# Use awk to process the entire file efficiently
awk -F, -v chunk_size="$CHUNK_SIZE" -v output_dir="$OUTPUT_DIR" -v data_lines="$DATA_LINES" '
BEGIN {
    chunk_num = 1
    line_num = 0
    values = ""
}

NR > 1 {  # Skip header
    line_num++
    stream_ref = $1
    day_index = $2

    # Add to current chunk values
    if (values == "") {
        values = "(" stream_ref "," day_index ")"
    } else {
        values = values ",(" stream_ref "," day_index ")"
    }

    # Write chunk when it reaches the size limit or is the last line
    if (line_num % chunk_size == 0 || line_num == data_lines) {
        chunk_file = output_dir "/chunk_" sprintf("%04d", chunk_num) ".sql"

        # Write the complete SQL statement
        print "INSERT INTO pending_prune_days (stream_ref, day_index)" > chunk_file
        print "VALUES " values > chunk_file
        print "ON CONFLICT (stream_ref, day_index) DO NOTHING;" > chunk_file

        close(chunk_file)

        print "  ✅ Created SQL chunk " chunk_num ": chunk_" sprintf("%04d", chunk_num) ".sql (" (line_num % chunk_size == 0 ? chunk_size : line_num % chunk_size) " rows)"
        chunk_num++
        values = ""
    }
}
' "$INPUT_FILE"

# Calculate statistics
TOTAL_CHUNKS=$(ls "$OUTPUT_DIR"/chunk_*.sql 2>/dev/null | wc -l)
EXPECTED_CHUNKS=$(( (DATA_LINES + CHUNK_SIZE - 1) / CHUNK_SIZE ))  # Ceiling division

echo ""
echo "✅ Successfully created $TOTAL_CHUNKS SQL chunks"
echo "📁 Output directory: $OUTPUT_DIR"
echo "📊 Each chunk contains up to $CHUNK_SIZE INSERT statements"
echo ""
echo "Created SQL chunk files:"
ls -lah "$OUTPUT_DIR"/chunk_*.sql | head -10
if [[ $TOTAL_CHUNKS -gt 10 ]]; then
    echo "... and $((TOTAL_CHUNKS - 10)) more files"
fi

echo ""
echo "💡 Each SQL file contains a complete INSERT statement ready to execute"
echo "💡 Use: kwil-cli exec-sql --stmt \"\$(cat chunk_0001.sql)\" --provider <url> --private-key <key>"
