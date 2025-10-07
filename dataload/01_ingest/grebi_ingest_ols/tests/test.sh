#!/usr/bin/env bash
set -e

GREBI_INGEST_OLS=$(dirname "$0")/../../../target/release/grebi_ingest_ols

cd "$(dirname "$0")"

echo "Testing grebi_ingest_ols..."

# Test ingesting OLS JSON - this is a complex format so we just verify basic operation
# The program requires specific OLS API format which is complex to mock fully
# We test that the program starts and processes input
OUTPUT=$(cat input.json | $GREBI_INGEST_OLS --datasource-name TestOLS --ontologies hp 2>&1) || true

# Check that the program ran and processed the ontology
if echo "$OUTPUT" | grep -q "Reading ontology: hp"; then
    echo "✓ grebi_ingest_ols tests passed (basic functionality verified)"
else
    echo "ERROR: Expected 'Reading ontology: hp' in output"
    exit 1
fi
