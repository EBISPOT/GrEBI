#!/usr/bin/env bash
set -e

GREBI_INGEST_KGX=$(dirname "$0")/../../../target/release/grebi_ingest_kgx_edges

cd "$(dirname "$0")"

echo "Testing grebi_ingest_kgx_edges..."

# Test ingesting KGX edges
OUTPUT=$(cat input.jsonl | $GREBI_INGEST_KGX)

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
if ! echo "$OUTPUT" | grep -q 'gene:1'; then
    echo "ERROR: Expected gene:1 in output"
    exit 1
fi

echo "✓ grebi_ingest_kgx_edges tests passed"
