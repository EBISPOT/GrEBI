#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_rdf2jsonl

echo "Testing grebi_rdf2jsonl..."

# Basic smoke test - check program exists
if ! command -v $PROG &> /dev/null; then
    echo "ERROR: grebi_rdf2jsonl not found"
    exit 1
fi

echo "✓ grebi_rdf2jsonl tests passed (smoke test)"
