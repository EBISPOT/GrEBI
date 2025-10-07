#!/usr/bin/env bash
set -e

GREBI_TRANSFORM=$(dirname "$0")/../../../target/release/grebi_transform_jsonl

cd "$(dirname "$0")"

echo "Testing grebi_transform_jsonl..."

# Test removing keys
OUTPUT=$(cat input.jsonl | $GREBI_TRANSFORM --json-remove-keys unwanted)

# Check that unwanted key is removed
if echo "$OUTPUT" | grep -q '"unwanted"'; then
    echo "ERROR: unwanted key should be removed"
    exit 1
fi

# Check that other keys are preserved
if ! echo "$OUTPUT" | grep -q '"name"'; then
    echo "ERROR: name key should be preserved"
    exit 1
fi

# Test injecting type
OUTPUT=$(cat input.jsonl | $GREBI_TRANSFORM --json-inject-type TestType)

# Check that type is injected
if ! echo "$OUTPUT" | grep -q '"grebi:type":\["TestType"\]'; then
    echo "ERROR: type should be injected"
    exit 1
fi

# Check that all outputs are valid JSON
echo "$OUTPUT" | while IFS= read -r line; do
    if ! echo "$line" | jq empty 2>/dev/null; then
        echo "ERROR: Invalid JSON: $line"
        exit 1
    fi
done

echo "✓ grebi_transform_jsonl tests passed"
