#!/usr/bin/env bash
set -e

GREBI_NORMALISE=$(dirname "$0")/../../../target/release/grebi_normalise_prefixes

cd "$(dirname "$0")"

echo "Testing grebi_normalise_prefixes..."

# Test normalising prefixes
OUTPUT=$(cat input.jsonl | $GREBI_NORMALISE prefix_map.json)

# Should produce 2 lines
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -ne 2 ]; then
    echo "ERROR: Expected 2 lines, got $LINE_COUNT"
    exit 1
fi

# Check that all outputs are valid JSON
echo "$OUTPUT" | while IFS= read -r line; do
    if ! echo "$line" | jq empty 2>/dev/null; then
        echo "ERROR: Invalid JSON: $line"
        exit 1
    fi
done

echo "✓ grebi_normalise_prefixes tests passed"
