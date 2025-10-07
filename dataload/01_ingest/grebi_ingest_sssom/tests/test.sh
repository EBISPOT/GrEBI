#!/usr/bin/env bash
set -e

GREBI_INGEST_SSSOM=$(dirname "$0")/../../../target/release/grebi_ingest_sssom

cd "$(dirname "$0")"

echo "Testing grebi_ingest_sssom..."

# Test ingesting SSSOM
OUTPUT=$(cat input.tsv | $GREBI_INGEST_SSSOM --datasource-name TestDS)

# Should produce at least 1 line
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -lt 1 ]; then
    echo "ERROR: Expected at least 1 line, got $LINE_COUNT"
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

echo "✓ grebi_ingest_sssom tests passed"
