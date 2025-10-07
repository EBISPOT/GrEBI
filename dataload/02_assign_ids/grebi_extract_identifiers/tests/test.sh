#!/usr/bin/env bash
set -e

GREBI_EXTRACT_IDS=$(dirname "$0")/../../../target/release/grebi_extract_identifiers

cd "$(dirname "$0")"

echo "Testing grebi_extract_identifiers..."

# Test extracting identifiers
OUTPUT=$(cat input.jsonl | $GREBI_EXTRACT_IDS --identifier-properties id,sameAs)

# Should produce 2 lines (one per input object)
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -ne 2 ]; then
    echo "ERROR: Expected 2 lines, got $LINE_COUNT"
    exit 1
fi

# First line should have 3 IDs: test:1, test:1a, test:1b
FIRST_LINE=$(echo "$OUTPUT" | head -1)
ID_COUNT=$(echo "$FIRST_LINE" | tr '\t' '\n' | wc -l)
if [ "$ID_COUNT" -ne 3 ]; then
    echo "ERROR: Expected 3 IDs in first line, got $ID_COUNT"
    exit 1
fi

# Check that test:1a is present
if ! echo "$FIRST_LINE" | grep -q 'test:1a'; then
    echo "ERROR: Expected test:1a in output"
    exit 1
fi

echo "✓ grebi_extract_identifiers tests passed"
