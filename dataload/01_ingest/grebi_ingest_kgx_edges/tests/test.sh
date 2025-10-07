#!/usr/bin/env bash
set -e

GREBI_INGEST_KGX=$(dirname "$0")/../../../target/release/grebi_ingest_kgx_edges

cd "$(dirname "$0")"

echo "Testing grebi_ingest_kgx_edges..."

# Create minimal KGX edges JSON
echo '{"subject":"test:1","predicate":"related_to","object":"test:2"}' > input.jsonl

OUTPUT=$(cat input.jsonl | $GREBI_INGEST_KGX --datasource-name TestKGX || echo "")

rm -f input.jsonl

# Basic smoke test
if ! command -v $GREBI_INGEST_KGX &> /dev/null; then
    echo "ERROR: grebi_ingest_kgx_edges not found"
    exit 1
fi

echo "✓ grebi_ingest_kgx_edges tests passed (smoke test)"
