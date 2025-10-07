#!/usr/bin/env bash
set -e

GREBI_UNWIND=$(dirname "$0")/../../../target/release/grebi_unwind

cd "$(dirname "$0")"

echo "Testing grebi_unwind..."

# Test unwinding an array field
OUTPUT=$(cat input.jsonl | $GREBI_UNWIND --unwind-field tags)

# Should produce 4 lines (3 from test:1, 1 from test:2)
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -ne 4 ]; then
    echo "ERROR: Expected 4 lines, got $LINE_COUNT"
    exit 1
fi

# Check that tag1 appears exactly once
TAG1_COUNT=$(echo "$OUTPUT" | grep -c '"tags":"tag1"' || true)
if [ "$TAG1_COUNT" -ne 1 ]; then
    echo "ERROR: Expected 1 occurrence of tag1, got $TAG1_COUNT"
    exit 1
fi

# Check that all outputs are valid JSON
echo "$OUTPUT" | while IFS= read -r line; do
    if ! echo "$line" | jq empty 2>/dev/null; then
        echo "ERROR: Invalid JSON: $line"
        exit 1
    fi
done

echo "✓ grebi_unwind tests passed"
