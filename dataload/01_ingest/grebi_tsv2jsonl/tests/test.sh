#!/usr/bin/env bash
set -e

GREBI_TSV2JSONL=$(dirname "$0")/../../../target/release/grebi_tsv2jsonl

cd "$(dirname "$0")"

echo "Testing grebi_tsv2jsonl..."

# Test converting TSV to JSONL
OUTPUT=$(cat input.tsv | $GREBI_TSV2JSONL)

# Should produce 2 lines
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -ne 2 ]; then
    echo "ERROR: Expected 2 lines, got $LINE_COUNT"
    exit 1
fi

# Check that outputs contain expected fields
if ! echo "$OUTPUT" | grep -q '"id":\["test:1"\]'; then
    echo "ERROR: Expected test:1 in output"
    exit 1
fi

if ! echo "$OUTPUT" | grep -q '"name":\["Test Name 1"\]'; then
    echo "ERROR: Expected Test Name 1 in output"
    exit 1
fi

# Check that all outputs are valid JSON
echo "$OUTPUT" | while IFS= read -r line; do
    if ! echo "$line" | jq empty 2>/dev/null; then
        echo "ERROR: Invalid JSON: $line"
        exit 1
    fi
done

echo "✓ grebi_tsv2jsonl tests passed"
