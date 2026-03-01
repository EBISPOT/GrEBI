#!/usr/bin/env python3
"""Export PostgreSQL edges table to JSONL snapshot file."""

import json
import os
import subprocess
import sys


def export_edges(subgraph, output_file):
    """Export edges from PostgreSQL to a JSONL snapshot file."""
    table_name = f"edges_{subgraph}"
    pg_host = os.environ.get("GREBI_POSTGRES_HOST", "localhost")
    pg_port = os.environ.get("GREBI_POSTGRES_PORT", "5432")
    pg_user = os.environ.get("GREBI_POSTGRES_USER", "grebi")
    pg_db = os.environ.get("GREBI_POSTGRES_DB", "grebi")

    # Query all edges as JSON objects, sorted by edge ID for stable snapshots
    query = f"""
        SELECT row_to_json(t) FROM (
            SELECT * FROM "{table_name}" ORDER BY "grebi:edgeId"
        ) t;
    """

    result = subprocess.run(
        ["psql", "-h", pg_host, "-p", pg_port, "-U", pg_user, "-d", pg_db,
         "-t", "-A", "-c", query],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        print(f"Error querying PostgreSQL: {result.stderr}", file=sys.stderr)
        return 0

    all_docs = []
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            doc = json.loads(line)
            # Remove the _json field from snapshot (it's the full blob,
            # would be redundant and make diffs harder to read)
            doc.pop("_json", None)
            # Remove null values for cleaner snapshots
            doc = {k: v for k, v in doc.items() if v is not None}
            all_docs.append(doc)
        except json.JSONDecodeError:
            continue

    with open(output_file, "w") as f:
        for doc in sorted(all_docs, key=lambda d: json.dumps(d, sort_keys=True)):
            print(json.dumps(doc, sort_keys=True, ensure_ascii=False), file=f)

    return len(all_docs)


if __name__ == "__main__":
    subgraph = sys.argv[1]

    n = export_edges(subgraph, f"{subgraph}_snapshot_postgres_edges.jsonl")
    print(f"Exported {n} PostgreSQL edges")
