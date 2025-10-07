#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_make_solr

cd "$(dirname "$0")"

echo "Testing grebi_make_solr..."

rm -f out_nodes.jsonl out_edges.jsonl

# Test making Solr JSONL
$PROG \
    --in-nodes-jsonl nodes.jsonl \
    --in-edges-jsonl edges.jsonl \
    --out-nodes-jsonl-path out_nodes.jsonl \
    --out-edges-jsonl-path out_edges.jsonl

# Check that output files were created
if [ ! -f out_nodes.jsonl ]; then
    echo "ERROR: out_nodes.jsonl not created"
    exit 1
fi

if [ ! -f out_edges.jsonl ]; then
    echo "ERROR: out_edges.jsonl not created"
    exit 1
fi

# Check that out_nodes.jsonl has content
LINE_COUNT=$(wc -l < out_nodes.jsonl)
if [ "$LINE_COUNT" -lt 2 ]; then
    echo "ERROR: Expected at least 2 lines in out_nodes.jsonl"
    exit 1
fi

# Check that outputs are valid JSON
cat out_nodes.jsonl | while IFS= read -r line; do
    if [ -n "$line" ]; then
        if ! echo "$line" | jq empty 2>/dev/null; then
            echo "ERROR: Invalid JSON in out_nodes.jsonl: $line"
            exit 1
        fi
    fi
done

rm -f out_nodes.jsonl out_edges.jsonl

echo "✓ grebi_make_solr tests passed"
