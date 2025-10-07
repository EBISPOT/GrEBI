#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_make_neo_ids_csv

cd "$(dirname "$0")"

echo "Testing grebi_make_neo_ids_csv..."

# Test making Neo4j ID CSV
OUTPUT=$(cat ids.txt | $PROG)

# Should produce at least 4 lines (header + 3 data)
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -ne 4 ]; then
    echo "ERROR: Expected 4 lines, got $LINE_COUNT"
    exit 1
fi

# Check for CSV header
if ! echo "$OUTPUT" | head -1 | grep -q "id:ID"; then
    echo "ERROR: Expected CSV header with id:ID"
    exit 1
fi

# Check for expected content
if ! echo "$OUTPUT" | grep -q 'test:1'; then
    echo "ERROR: Expected test:1 in output"
    exit 1
fi

echo "✓ grebi_make_neo_ids_csv tests passed"
