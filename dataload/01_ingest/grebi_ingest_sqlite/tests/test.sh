#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_ingest_sqlite

echo "Testing grebi_ingest_sqlite..."

# Basic smoke test - check program exists
if ! command -v $PROG &> /dev/null; then
    echo "ERROR: grebi_ingest_sqlite not found"
    exit 1
fi

echo "✓ grebi_ingest_sqlite tests passed (smoke test)"
