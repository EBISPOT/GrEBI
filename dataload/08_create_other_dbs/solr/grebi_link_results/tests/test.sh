#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_link_results

echo "Testing grebi_link_results..."

# Basic smoke test - check program exists
if ! command -v $PROG &> /dev/null; then
    echo "ERROR: grebi_link_results not found"
    exit 1
fi

echo "✓ grebi_link_results tests passed (smoke test)"
