#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_make_compressed_blob

cd "$(dirname "$0")"

echo "Testing grebi_make_compressed_blob..."

# Test making compressed blobs
OUTPUT=$(cat input.jsonl | $PROG 2>&1)

# Check stderr for expected message
if ! echo "$OUTPUT" | grep -q "saw 2 lines"; then
    echo "ERROR: Expected 'saw 2 lines' in stderr output"
    exit 1
fi

# The actual output is binary, so just check it produced something
# We can't easily validate binary output in a shell test
# Just verify the program runs successfully
echo "✓ grebi_make_compressed_blob tests passed"
