#!/usr/bin/env python3
"""Export PostgreSQL edges and nodes tables to JSONL snapshot files."""

import json
import os
import re
import subprocess
import sys


def export_table(subgraph, table_name, sort_key, output_file):
    """Export a PostgreSQL table to a JSONL snapshot file."""
    pg_host = os.environ.get("GREBI_POSTGRES_HOST", "localhost")
    pg_port = os.environ.get("GREBI_POSTGRES_PORT", "5432")
    pg_user = os.environ.get("GREBI_POSTGRES_USER", "grebi")
    pg_db = os.environ.get("GREBI_POSTGRES_DB", "grebi")

    query = f"""
        SELECT row_to_json(t) FROM (
            SELECT * FROM "{table_name}" ORDER BY "{sort_key}"
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

    embedding_col_re = re.compile(r'^embedding__')

    all_docs = []
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            doc = json.loads(line)
            # Remove embedding vector columns (large, not useful in snapshots)
            doc = {k: v for k, v in doc.items()
                   if v is not None and not embedding_col_re.match(k)}
            all_docs.append(doc)
        except json.JSONDecodeError:
            continue

    with open(output_file, "w") as f:
        for doc in sorted(all_docs, key=lambda d: json.dumps(d, sort_keys=True)):
            print(json.dumps(doc, sort_keys=True, ensure_ascii=False), file=f)

    return len(all_docs)


if __name__ == "__main__":
    subgraph = sys.argv[1]

    n = export_table(subgraph, f"edges_{subgraph}", "grebi:edgeId",
                     f"{subgraph}_snapshot_postgres_edges.jsonl")
    print(f"Exported {n} PostgreSQL edges")

    n = export_table(subgraph, f"nodes_{subgraph}", "grebi:nodeId",
                     f"{subgraph}_snapshot_postgres_nodes.jsonl")
    print(f"Exported {n} PostgreSQL nodes")
