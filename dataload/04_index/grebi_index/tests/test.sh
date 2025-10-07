#!/usr/bin/env bash
set -e

GREBI_INDEX=$(dirname "$0")/../../../target/release/grebi_index

cd "$(dirname "$0")"

rm -f graph_metadata.json entity_metadata.jsonl names.txt ids.txt

echo "Testing grebi_index..."

# Test indexing
cat input.jsonl | $GREBI_INDEX \
    --subgraph-name test_subgraph \
    --out-graph-metadata-json-path graph_metadata.json \
    --out-entity-metadata-jsonl-path entity_metadata.jsonl \
    --out-names-txt names.txt \
    --out-ids-txt ids.txt

# Check that output files were created
if [ ! -f graph_metadata.json ]; then
    echo "ERROR: graph_metadata.json not created"
    exit 1
fi

if [ ! -f entity_metadata.jsonl ]; then
    echo "ERROR: entity_metadata.jsonl not created"
    exit 1
fi

# Check that entity_metadata.jsonl has 2 lines
LINE_COUNT=$(wc -l < entity_metadata.jsonl)
if [ "$LINE_COUNT" -ne 2 ]; then
    echo "ERROR: Expected 2 lines in entity_metadata.jsonl, got $LINE_COUNT"
    exit 1
fi

# Check that names.txt contains expected names
if ! grep -q "Test Name 1" names.txt; then
    echo "ERROR: Expected 'Test Name 1' in names.txt"
    exit 1
fi

# Check that graph_metadata.json is valid JSON
if ! jq empty graph_metadata.json 2>/dev/null; then
    echo "ERROR: graph_metadata.json is not valid JSON"
    exit 1
fi

rm -f graph_metadata.json entity_metadata.jsonl names.txt ids.txt

echo "✓ grebi_index tests passed"
