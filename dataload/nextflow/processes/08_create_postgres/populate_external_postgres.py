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
import argparse
from pathlib import Path

# Allow importing load_postgres from the same directory
sys.path.insert(0, str(Path(__file__).resolve().parent))

from load_postgres import load_all, run_psql


def main():
    parser = argparse.ArgumentParser(description="Populate an existing external PostgreSQL database with grebi data")
    parser.add_argument("--parallel-workers", type=int, default=0,
                        help="Per-table parallel_workers and max_parallel_maintenance_workers for index builds")
    parser.add_argument("--maintenance-work-mem", default="",
                        help="Session maintenance_work_mem for index builds")
    args = parser.parse_args()

    # Validate required env vars
    for var in ("PGHOST", "PGDATABASE", "PGUSER"):
        if not os.environ.get(var):
            sys.exit(f"ERROR: {var} must be set")

    os.environ.setdefault("PGPORT", "5432")
    os.environ.setdefault("PGPASSWORD", "")

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

    load_all(
        drop_existing=True,
        parallel_workers=args.parallel_workers,
        maintenance_work_mem=args.maintenance_work_mem,
    )

    # Write status file (Nextflow output)
    from datetime import datetime
    os.makedirs("postgres_external_done", exist_ok=True)
    with open("postgres_external_done/status.txt", "w") as f:
        f.write(f"Populated {pguser}@{pghost}:{pgport}/{pgdb} at {datetime.now()}\n")

    print(f"=== Done ===", flush=True)
    print(f"External database {pgdb} on {pghost} has been populated.", flush=True)


if __name__ == "__main__":
    main()
