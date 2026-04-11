#!/usr/bin/env python3
"""
Populate an existing (managed) external PostgreSQL database with grebi data.

Uses COPY ... WITH (FORMAT binary, FREEZE) by running CREATE TABLE and all
COPY commands for each table in a single transaction.  FREEZE marks rows as
already frozen, skipping future VACUUM passes.

Connects using standard libpq environment variables (PGHOST, PGDATABASE, etc.).
"""

import os
import sys
from pathlib import Path

# Allow importing load_postgres from the same directory
sys.path.insert(0, str(Path(__file__).resolve().parent))

from load_postgres import load_all, run_psql


def main():
    # Validate required env vars
    for var in ("PGHOST", "PGDATABASE", "PGUSER"):
        if not os.environ.get(var):
            sys.exit(f"ERROR: {var} must be set")

    os.environ.setdefault("PGPORT", "5432")
    os.environ.setdefault("PGPASSWORD", "")
    os.environ.setdefault("PGSSLMODE", "")

    pghost = os.environ["PGHOST"]
    pgport = os.environ["PGPORT"]
    pgdb = os.environ["PGDATABASE"]
    pguser = os.environ["PGUSER"]

    print(f"=== Connecting to {pguser}@{pghost}:{pgport}/{pgdb} ===", flush=True)
    try:
        run_psql("SELECT 1;", "connection test")
    except Exception as e:
        sys.exit(f"ERROR: Cannot connect to PostgreSQL: {e}")
    print("Connection OK.", flush=True)

    load_all(drop_existing=True)

    # Write status file (Nextflow output)
    from datetime import datetime
    os.makedirs("postgres_external_done", exist_ok=True)
    with open("postgres_external_done/status.txt", "w") as f:
        f.write(f"Populated {pguser}@{pghost}:{pgport}/{pgdb} at {datetime.now()}\n")

    print(f"=== Done ===", flush=True)
    print(f"External database {pgdb} on {pghost} has been populated.", flush=True)


if __name__ == "__main__":
    main()
