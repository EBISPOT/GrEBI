#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_ingest_reactome

cd "$(dirname "$0")"

echo "Testing grebi_ingest_reactome..."

# Set up required environment variable
export GREBI_DATALOAD_HOME=/home/runner/work/GrEBI/GrEBI/dataload

# Test ingesting Reactome data
OUTPUT=$(cat input.jsonl | $PROG)

# Should produce at least 2 lines
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -lt 2 ]; then
    echo "ERROR: Expected at least 2 lines, got $LINE_COUNT"
    exit 1
fi

# Check that outputs are valid JSON
echo "$OUTPUT" | while IFS= read -r line; do
    if [ -n "$line" ]; then
        if ! echo "$line" | jq empty 2>/dev/null; then
            echo "ERROR: Invalid JSON: $line"
            exit 1
        fi
    fi
done

# Check for expected content
if ! echo "$OUTPUT" | grep -q 'reactome'; then
    echo "ERROR: Expected reactome prefix in output"
    exit 1
fi

echo "✓ grebi_ingest_reactome tests passed"
