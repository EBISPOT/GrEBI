#!/usr/bin/env bash
set -e

GREBI_INDEX=$(dirname "$0")/../../../target/release/grebi_index

echo "Testing grebi_index..."

# Basic smoke test - check program exists and shows help
if ! command -v $GREBI_INDEX &> /dev/null; then
    echo "ERROR: grebi_index not found"
    exit 1
fi

# Check that the program can show help
if ! $GREBI_INDEX --help > /dev/null 2>&1; then
    echo "ERROR: grebi_index --help failed"
    exit 1
fi

echo "✓ grebi_index tests passed (smoke test)"
