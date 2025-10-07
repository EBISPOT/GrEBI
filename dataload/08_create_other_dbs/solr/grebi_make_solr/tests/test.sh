#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_make_solr

echo "Testing grebi_make_solr..."

# Basic smoke test - check program exists
if ! command -v $PROG &> /dev/null; then
    echo "ERROR: grebi_make_solr not found"
    exit 1
fi

echo "✓ grebi_make_solr tests passed (smoke test)"
