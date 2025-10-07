#!/usr/bin/env bash
set -e

PROG=$(dirname "$0")/../../../target/release/grebi_ingest_sqlite

cd "$(dirname "$0")"

echo "Testing grebi_ingest_sqlite..."

# Create a test SQLite database
sqlite3 test.db <<'EOF'
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT
);
INSERT INTO users (id, name, email) VALUES (1, 'Test User 1', 'test1@example.com');
INSERT INTO users (id, name, email) VALUES (2, 'Test User 2', 'test2@example.com');
EOF

# Test ingesting SQLite data
OUTPUT=$($PROG --datasource-name TestDB --filename test.db)

# Should produce at least 2 lines (one per row)
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
if ! echo "$OUTPUT" | grep -q 'Test User 1'; then
    echo "ERROR: Expected 'Test User 1' in output"
    exit 1
fi

rm -f test.db

echo "✓ grebi_ingest_sqlite tests passed"
