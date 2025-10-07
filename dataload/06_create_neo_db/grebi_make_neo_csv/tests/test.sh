#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_make_neo_csv

cd "$(dirname "$0")"

echo "Testing grebi_make_neo_csv..."

rm -f nodes.csv edges.csv id_edges.csv

# Test making Neo4j CSV files
$PROG \
    --in-nodes-jsonl nodes.jsonl \
    --in-edges-jsonl edges.jsonl \
    --in-graph-metadata-jsons graph_metadata.json \
    --out-nodes-csv-path nodes.csv \
    --out-edges-csv-path edges.csv \
    --out-id-edges-csv-path id_edges.csv \
    --add-prefix "test:"

# Check that output files were created
if [ ! -f nodes.csv ]; then
    echo "ERROR: nodes.csv not created"
    exit 1
fi

if [ ! -f edges.csv ]; then
    echo "ERROR: edges.csv not created"
    exit 1
fi

if [ ! -f id_edges.csv ]; then
    echo "ERROR: id_edges.csv not created"
    exit 1
fi

# Check that nodes.csv has content
LINE_COUNT=$(wc -l < nodes.csv)
if [ "$LINE_COUNT" -lt 2 ]; then
    echo "ERROR: Expected at least 2 lines in nodes.csv (header + data)"
    exit 1
fi

rm -f nodes.csv edges.csv id_edges.csv

echo "✓ grebi_make_neo_csv tests passed"
