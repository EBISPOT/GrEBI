#!/usr/bin/env bash
set -e

GREBI_IDS2GROUPS=$(dirname "$0")/../../../target/release/grebi_identifiers2groups

cd "$(dirname "$0")"

echo "Testing grebi_identifiers2groups..."

# Test grouping identifiers
OUTPUT=$(cat input.txt | $GREBI_IDS2GROUPS)

# Should produce 2 groups (test:1 family merged, test:2 family standalone)
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -ne 2 ]; then
    echo "ERROR: Expected 2 lines, got $LINE_COUNT"
    exit 1
fi

# The test:1 group should contain all variants: test:1, test:1a, test:1b, test:1c
MERGED_GROUP=$(echo "$OUTPUT" | grep 'test:1' | head -1)
for id in test:1 test:1a test:1b test:1c; do
    if ! echo "$MERGED_GROUP" | grep -q "$id"; then
        echo "ERROR: Expected $id in merged group"
        exit 1
    fi
done

echo "✓ grebi_identifiers2groups tests passed"
