#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_link

cd "$(dirname "$0")"

echo "Testing grebi_link..."

rm -f edges.jsonl out_graph_metadata.json

# Test linking
cat input.jsonl | $PROG \
    --in-metadata-jsonl metadata.jsonl \
    --in-graph-metadata-json graph_metadata.json \
    --out-edges-jsonl edges.jsonl \
    --out-graph-metadata-json out_graph_metadata.json \
    --groups-txt groups.txt \
    --exclude "" \
    --exclude-self-referential "" > output_nodes.jsonl

# Check that output files were created
if [ ! -f edges.jsonl ]; then
    echo "ERROR: edges.jsonl not created"
    exit 1
fi

if [ ! -f out_graph_metadata.json ]; then
    echo "ERROR: out_graph_metadata.json not created"
    exit 1
fi

# Check that out_graph_metadata.json is valid JSON
if ! jq empty out_graph_metadata.json 2>/dev/null; then
    echo "ERROR: out_graph_metadata.json is not valid JSON"
    exit 1
fi

rm -f edges.jsonl out_graph_metadata.json output_nodes.jsonl

echo "✓ grebi_link tests passed"
