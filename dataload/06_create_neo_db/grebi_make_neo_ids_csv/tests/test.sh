#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_make_neo_ids_csv

echo "Testing grebi_make_neo_ids_csv..."

# Basic smoke test - check program exists
if ! command -v $PROG &> /dev/null; then
    echo "ERROR: grebi_make_neo_ids_csv not found"
    exit 1
fi

echo "✓ grebi_make_neo_ids_csv tests passed (smoke test)"
