#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_link_results

cd "$(dirname "$0")"

echo "Testing grebi_link_results..."

# Test linking results
OUTPUT=$(cat input.jsonl | $PROG \
    --in-metadata-jsonl metadata.jsonl \
    --groups-txt groups.txt)

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

echo "✓ grebi_link_results tests passed"
