#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_ingest_reactome

echo "Testing grebi_ingest_reactome..."

# Basic smoke test - check program exists
if ! command -v $PROG &> /dev/null; then
    echo "ERROR: grebi_ingest_reactome not found"
    exit 1
fi

echo "✓ grebi_ingest_reactome tests passed (smoke test)"
