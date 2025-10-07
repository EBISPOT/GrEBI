#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_rdf2jsonl

cd "$(dirname "$0")"

echo "Testing grebi_rdf2jsonl..."

# Test converting RDF/XML to JSONL
OUTPUT=$(cat input.rdf | $PROG --rdf-type rdf_triples_xml)

# Should produce at least 2 lines (one for each entity)
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
if ! echo "$OUTPUT" | grep -q 'test1\|test2'; then
    echo "ERROR: Expected test entities in output"
    exit 1
fi

echo "✓ grebi_rdf2jsonl tests passed"
