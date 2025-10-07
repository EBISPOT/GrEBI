#!/usr/bin/env bash
set -e

GREBI_INGEST_GWAS=$(dirname "$0")/../../../target/release/grebi_ingest_gwas

cd "$(dirname "$0")"

echo "Testing grebi_ingest_gwas..."

# Test ingesting GWAS studies
OUTPUT=$(cat gwas-catalog-studies-test.tsv | $GREBI_INGEST_GWAS --datasource-name TestGWAS --filename gwas-catalog-studies-test.tsv)

# Should produce at least 2 lines (one per study)
LINE_COUNT=$(echo "$OUTPUT" | wc -l)
if [ "$LINE_COUNT" -lt 2 ]; then
    echo "ERROR: Expected at least 2 lines, got $LINE_COUNT"
    exit 1
fi

# Check that outputs are valid JSON
echo "$OUTPUT" | while IFS= read -r line; do
    if [ -n "$line" ]; then
        if ! echo "$line" | jq empty 2>/dev/null; then
            echo "ERROR: Invalid JSON: $line"
            exit 1
        fi
    fi
done

# Check for expected content
if ! echo "$OUTPUT" | grep -q 'GCST000001'; then
    echo "ERROR: Expected GCST000001 in output"
    exit 1
fi

echo "✓ grebi_ingest_gwas tests passed"
