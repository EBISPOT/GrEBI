#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_make_sqlite

cd "$(dirname "$0")"

echo "Testing grebi_make_sqlite..."

# First create compressed blob input
cat > test_input.jsonl << 'EOF'
{"id":"test:1","name":"Test 1"}
{"id":"test:2","name":"Test 2"}
EOF

# Create compressed blobs
BLOB_DATA=$(cat test_input.jsonl | ../grebi_make_compressed_blob/target/release/grebi_make_compressed_blob 2>/dev/null || \
            cat test_input.jsonl | ../../../../target/release/grebi_make_compressed_blob 2>/dev/null)

rm -f test.db

# Test making SQLite database
echo "$BLOB_DATA" | $PROG \
    --db-path test.db \
    --batch-size 100 \
    --page-size 4096 \
    --cache-size 10000 2>&1 | grep -q "." || true

# Check that database was created
if [ ! -f test.db ]; then
    echo "ERROR: test.db not created"
    exit 1
fi

# Verify it's a valid SQLite database
if ! sqlite3 test.db "SELECT count(*) FROM entities" > /dev/null 2>&1; then
    echo "ERROR: test.db is not a valid SQLite database or doesn't have entities table"
    exit 1
fi

rm -f test.db test_input.jsonl

echo "✓ grebi_make_sqlite tests passed"
