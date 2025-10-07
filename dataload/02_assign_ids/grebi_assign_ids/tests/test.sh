#!/usr/bin/env bash
set -e

GREBI_ASSIGN_IDS=$(dirname "$0")/../../../target/release/grebi_assign_ids

cd "$(dirname "$0")"

echo "Testing grebi_assign_ids..."

# Test assigning canonical IDs
OUTPUT=$(cat input.jsonl | $GREBI_ASSIGN_IDS --groups-txt groups.txt --identifier-properties id)

# Should produce 2 lines
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -ne 2 ]; then
    echo "ERROR: Expected 2 lines, got $LINE_COUNT"
    exit 1
fi

# test:1a should be replaced with test:1 (canonical ID from group)
if ! echo "$OUTPUT" | grep -q '"id":"test:1"'; then
    echo "ERROR: Expected test:1 as canonical ID"
    exit 1
fi

# Check that all outputs are valid JSON
echo "$OUTPUT" | while IFS= read -r line; do
    if ! echo "$line" | jq empty 2>/dev/null; then
        echo "ERROR: Invalid JSON: $line"
        exit 1
    fi
done

echo "✓ grebi_assign_ids tests passed"
