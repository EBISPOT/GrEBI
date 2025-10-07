#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_link

echo "Testing grebi_link..."

# Basic smoke test - check program exists
if ! command -v $PROG &> /dev/null; then
    echo "ERROR: grebi_link not found"
    exit 1
fi

echo "✓ grebi_link tests passed (smoke test)"
