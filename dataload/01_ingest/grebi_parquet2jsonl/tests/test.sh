#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_parquet2jsonl

echo "Testing grebi_parquet2jsonl..."

# Basic smoke test - check program exists
if ! command -v $PROG &> /dev/null; then
    echo "ERROR: grebi_parquet2jsonl not found"
    exit 1
fi

echo "✓ grebi_parquet2jsonl tests passed (smoke test)"
