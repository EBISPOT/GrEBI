#!/usr/bin/env bash
set -e

GREBI_NODES2EDGES=$(dirname "$0")/../../../target/release/grebi_nodes2edges

cd "$(dirname "$0")"

echo "Testing grebi_nodes2edges..."

# Test converting nodes to edges
OUTPUT=$(cat input.jsonl | $GREBI_NODES2EDGES --from-field subject --to-field object --edge-type associated_with)

# Should produce 3 edges (1 from gene:1, 2 from gene:2)
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -ne 3 ]; then
    echo "ERROR: Expected 3 lines, got $LINE_COUNT"
    exit 1
fi

# Check that gene:1 appears once
GENE1_COUNT=$(echo "$OUTPUT" | grep -c '"id":"gene:1"' || true)
if [ "$GENE1_COUNT" -ne 1 ]; then
    echo "ERROR: Expected 1 occurrence of gene:1, got $GENE1_COUNT"
    exit 1
fi

# Check that all outputs are valid JSON
echo "$OUTPUT" | while IFS= read -r line; do
    if ! echo "$line" | jq empty 2>/dev/null; then
        echo "ERROR: Invalid JSON: $line"
        exit 1
    fi
done

echo "✓ grebi_nodes2edges tests passed"
