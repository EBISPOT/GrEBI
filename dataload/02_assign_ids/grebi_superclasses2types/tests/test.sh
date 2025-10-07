#!/usr/bin/env bash
set -e

GREBI_SUPERCLASSES2TYPES=$(dirname "$0")/../../../target/release/grebi_superclasses2types

cd "$(dirname "$0")"

echo "Testing grebi_superclasses2types..."

# Test converting superclasses to types
OUTPUT=$(cat input.jsonl | $GREBI_SUPERCLASSES2TYPES --groups-txt groups.txt --type-superclasses rdfs:subClassOf 2>&1 | grep -v "loaded" | grep -v "completed")

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

echo "✓ grebi_superclasses2types tests passed"
