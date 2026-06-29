#!/usr/bin/env python3
"""
Core PostgreSQL loading logic shared by both local (create_postgres.nf) and
external (populate_external_postgres) paths.

Uses COPY ... WITH (FORMAT binary, FREEZE) by wrapping CREATE TABLE and all
COPY commands for each table in a single transaction.  FREEZE marks rows as
already frozen, skipping future VACUUM passes.

The actual COPY is done by piping psql scripts to `psql` subprocesses (no
Python DB driver needed).
"""
from __future__ import annotations

import glob
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_psql(script: str, label: str = "", psql_base: list[str] | None = None):
    """Pipe a SQL/psql script to psql -v ON_ERROR_STOP=1."""
    cmd = list(psql_base) if psql_base else ["psql", "-v", "ON_ERROR_STOP=1"]
    proc = subprocess.run(cmd, input=script, text=True, capture_output=True)
    if proc.returncode != 0:
        msg = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"psql failed ({label}): {msg}")
    if label:
        print(f"  {label} done.", flush=True)


def sizeof_fmt(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num) < 1024:
            return f"{num:.1f}{unit}"
        num /= 1024
    return f"{num:.1f}PB"


# ---------------------------------------------------------------------------
# Table loader: CREATE + \copy FREEZE in one psql transaction
# ---------------------------------------------------------------------------

def load_table(
    table_name: str,
    create_sql: str,
    pgbin_files: list[Path],
    psql_base: list[str] | None = None,
) -> None:
    """Load a single table: BEGIN, CREATE TABLE, \\copy FREEZE all files, COMMIT."""
    if not pgbin_files:
        print(f"  {table_name}: no files, skipping.", flush=True)
        return

    total_bytes = sum(f.stat().st_size for f in pgbin_files)
    print(f"  {table_name}: {len(pgbin_files)} files, {sizeof_fmt(total_bytes)} total", flush=True)

    t0 = time.time()

    # Build a psql script: single transaction with CREATE + \copy FREEZE
    lines = ["BEGIN;", create_sql]
    for pgbin in pgbin_files:
        abs_path = str(pgbin.resolve())
        lines.append(
            f'\\copy "{table_name}" FROM \'{abs_path}\' WITH (FORMAT binary, FREEZE)'
        )
    lines.append("COMMIT;")
    script = "\n".join(lines)

    for i, pgbin in enumerate(pgbin_files, 1):
        size = sizeof_fmt(pgbin.stat().st_size)
        print(f"    [{i}/{len(pgbin_files)}] {pgbin.name} ({size})", flush=True)

    cmd = list(psql_base) if psql_base else ["psql", "-v", "ON_ERROR_STOP=1"]
    proc = subprocess.run(cmd, input=script, text=True, capture_output=True)
    if proc.returncode != 0:
        msg = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(f"{table_name} load failed: {msg}")

    elapsed = time.time() - t0
    rate = total_bytes / elapsed / 1024 / 1024 if elapsed > 0 else 0
    print(f"  {table_name}: committed in {elapsed:.1f}s ({rate:.1f} MB/s)", flush=True)


# ---------------------------------------------------------------------------
# Discover subgraphs
# ---------------------------------------------------------------------------

def discover_subgraphs() -> list[str]:
    """Discover subgraph names from postgres_edges_*.pgbin filenames."""
    pattern = re.compile(r'^postgres_edges_(.*)_\d+\.pgbin$')
    subgraphs = set()
    for f in glob.glob("postgres_edges_*.pgbin"):
        m = pattern.match(os.path.basename(f))
        if m:
            subgraphs.add(m.group(1))
    result = sorted(subgraphs)
    if not result:
        raise RuntimeError("No postgres_edges_*.pgbin files found")
    return result


# ---------------------------------------------------------------------------
# Index creation
# ---------------------------------------------------------------------------

def create_indexes_for_subgraph(
    sg: str,
    nodes_cols_file: str,
    psql_base: list[str] | None = None,
    parallel_workers: int = 0,
    maintenance_work_mem: str = "",
):
    """Create all indexes for a subgraph (edges, nodes, blobs, autocomplete, mat_queries)."""

    stmts = []

    # Edge indexes
    stmts.append(f'CREATE INDEX "idx_edges_{sg}_edgeId" ON "edges_{sg}" USING btree ("grebi:edgeId");')
    stmts.append(f'CREATE INDEX "idx_edges_{sg}_fromNodeId" ON "edges_{sg}" USING btree ("grebi:fromNodeId");')
    stmts.append(f'CREATE INDEX "idx_edges_{sg}_toNodeId" ON "edges_{sg}" USING btree ("grebi:toNodeId");')
    stmts.append(f'CREATE INDEX "idx_edges_{sg}_type" ON "edges_{sg}" USING btree ("grebi:type");')
    stmts.append(f'CREATE INDEX "idx_edges_{sg}_datasources_gin" ON "edges_{sg}" USING gin ("grebi:datasources");')

    # Node indexes
    stmts.append(f'CREATE INDEX "idx_nodes_{sg}_nodeId" ON "nodes_{sg}" USING btree ("grebi:nodeId");')
    stmts.append(f'CREATE INDEX "idx_nodes_{sg}_curie" ON "nodes_{sg}" USING btree ("ols:curie");')
    stmts.append(f'CREATE INDEX "idx_nodes_{sg}_grebi_curie" ON "nodes_{sg}" USING btree ("grebi:curie");')
    stmts.append(f'CREATE INDEX "idx_nodes_{sg}_name" ON "nodes_{sg}" USING btree ("grebi:name");')
    stmts.append(f'CREATE INDEX "idx_nodes_{sg}_name_trgm" ON "nodes_{sg}" USING gin ("grebi:name" gin_trgm_ops);')
    stmts.append(f'CREATE INDEX "idx_nodes_{sg}_type_gin" ON "nodes_{sg}" USING gin ("grebi:type");')
    stmts.append(f'CREATE INDEX "idx_nodes_{sg}_datasources_gin" ON "nodes_{sg}" USING gin ("grebi:datasources");')
    stmts.append(f'CREATE INDEX "idx_nodes_{sg}_sourceIds_gin" ON "nodes_{sg}" USING gin ("grebi:sourceIds");')

    # Embedding indexes from column definitions
    with open(nodes_cols_file) as f:
        for line in f:
            line = line.strip()
            if line.startswith('"embedding:'):
                col_name = line.split('"')[1]  # extract name between first quotes
                safe_model = col_name.replace("embedding:", "").replace("-", "_").replace(".", "_")
                stmts.append(
                    f'CREATE INDEX "idx_nodes_{sg}_embedding_{safe_model}" ON "nodes_{sg}" '
                    f'USING hnsw ("{col_name}" vector_cosine_ops);'
                )

    # Blobs primary key
    stmts.append(f'ALTER TABLE "blobs_{sg}" ADD PRIMARY KEY (id);')

    # Autocomplete index
    stmts.append(f'CREATE INDEX "idx_autocomplete_{sg}_trgm" ON "autocomplete_{sg}" USING gin (label gin_trgm_ops);')

    # Materialised queries indexes
    stmts.append(f'CREATE INDEX "idx_mat_queries_{sg}_query_id" ON "materialised_queries_{sg}" USING btree (query_id);')
    stmts.append(f'CREATE INDEX "idx_mat_queries_{sg}_query_id_row" ON "materialised_queries_{sg}" USING btree (query_id, row_number);')

    print(f"  Creating {len(stmts)} indexes for {sg}...", flush=True)
    t0 = time.time()
    tables = [
        f'"edges_{sg}"',
        f'"nodes_{sg}"',
        f'"blobs_{sg}"',
        f'"autocomplete_{sg}"',
        f'"materialised_queries_{sg}"',
    ]

    if parallel_workers > 0:
        print(f"  Setting parallel_workers={parallel_workers} on {len(tables)} tables for {sg}", flush=True)
        for table in tables:
            run_psql(f"ALTER TABLE {table} SET (parallel_workers = {parallel_workers});", f"parallel_workers_{table}", psql_base)

    set_prefix = ""
    if maintenance_work_mem:
        set_prefix += f"SET maintenance_work_mem = '{maintenance_work_mem}';\n"
        print(f"  Setting maintenance_work_mem={maintenance_work_mem} per index session", flush=True)
    if parallel_workers > 0:
        set_prefix += f"SET max_parallel_maintenance_workers = {parallel_workers};\n"
        print(f"  Setting max_parallel_maintenance_workers={parallel_workers} per index session", flush=True)

    try:
        for idx_i, stmt in enumerate(stmts, 1):
            short = stmt.split("(")[0].strip() if "(" in stmt else stmt.strip()
            print(f"    [{idx_i}/{len(stmts)}] {short} ...", flush=True)
            stmt_t0 = time.time()
            run_psql(set_prefix + stmt, f"index_{sg}_{idx_i}", psql_base)
            print(f"    [{idx_i}/{len(stmts)}] done ({time.time() - stmt_t0:.1f}s)", flush=True)
    finally:
        if parallel_workers > 0:
            for table in tables:
                run_psql(f"ALTER TABLE {table} RESET (parallel_workers);", f"reset_parallel_workers_{table}", psql_base)

    elapsed = time.time() - t0
    print(f"  Indexes for {sg} created in {elapsed:.1f}s", flush=True)


# ---------------------------------------------------------------------------
# Main loading function
# ---------------------------------------------------------------------------

def load_all(
    psql_base: list[str] | None = None,
    drop_existing: bool = False,
    parallel_workers: int = 0,
    maintenance_work_mem: str = "",
):
    """
    Load all data into PostgreSQL using COPY FREEZE.

    Args:
        psql_base: Base psql command list (e.g. ["psql", "-h", sock, "-p", port, ...])
        drop_existing: If True, DROP tables before creating (for external postgres)
        parallel_workers: Per-table parallel_workers and max_parallel_maintenance_workers for index builds
        maintenance_work_mem: Session maintenance_work_mem to use for index builds
    """
    subgraphs = discover_subgraphs()
    print(f"Discovered subgraphs: {' '.join(subgraphs)}", flush=True)

    # Extensions
    print("=== Creating extensions ===", flush=True)
    run_psql(
        "CREATE EXTENSION IF NOT EXISTS vector;\n"
        "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
        "extensions",
        psql_base,
    )

    # === Process each subgraph ===
    for sg in subgraphs:
        print(f"=== Processing subgraph: {sg} ===", flush=True)

        # --- Read column definitions ---
        edges_cols_files = sorted(glob.glob(f"postgres_edges_columns_{sg}_*.txt"))
        edges_cols_file = edges_cols_files[0]
        with open(edges_cols_file) as f:
            edges_cols = ",".join(line.strip() for line in f if line.strip())

        nodes_cols_files = sorted(glob.glob(f"postgres_nodes_columns_{sg}_*.txt"))
        nodes_cols_file = nodes_cols_files[0]
        with open(nodes_cols_file) as f:
            nodes_cols = ",".join(line.strip() for line in f if line.strip())

        # --- Collect pgbin files ---
        edges_pgbins = sorted(Path(p) for p in glob.glob(f"postgres_edges_{sg}_*.pgbin"))
        nodes_pgbins = sorted(Path(p) for p in glob.glob(f"postgres_nodes_{sg}_*.pgbin"))
        blobs_pgbins = sorted(Path(p) for p in glob.glob(f"postgres_blobs_{sg}_*.pgbin") if os.path.getsize(p) > 0)
        autocomplete_pgbins = sorted(Path(p) for p in glob.glob(f"autocomplete_{sg}_*.pgbin") if os.path.getsize(p) > 0)
        mat_queries_pgbins = sorted(Path(p) for p in glob.glob(f"mat_queries_{sg}_*.pgbin") if os.path.getsize(p) > 0)

        drop_prefix = f'DROP TABLE IF EXISTS "{{table}}" CASCADE;\n' if drop_existing else ""

        # --- EDGES TABLE ---
        create_edges = drop_prefix.format(table=f"edges_{sg}") + \
            f'CREATE TABLE "edges_{sg}" ({edges_cols}) WITH (fillfactor=100);'
        load_table(f"edges_{sg}", create_edges, edges_pgbins, psql_base)

        # --- NODES TABLE ---
        # SET STORAGE EXTERNAL on embedding vectors to prevent TOAST compression
        nodes_storage_stmts = ""
        with open(nodes_cols_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith('"embedding:'):
                    col_name = line.split('"')[1]
                    nodes_storage_stmts += f'\nALTER TABLE "nodes_{sg}" ALTER COLUMN "{col_name}" SET STORAGE EXTERNAL;'
        create_nodes = drop_prefix.format(table=f"nodes_{sg}") + \
            f'CREATE TABLE "nodes_{sg}" ({nodes_cols}) WITH (fillfactor=100);' + \
            nodes_storage_stmts
        load_table(f"nodes_{sg}", create_nodes, nodes_pgbins, psql_base)

        # --- BLOBS TABLE ---
        # SET STORAGE EXTERNAL on json column: data is already zlib-compressed
        create_blobs = drop_prefix.format(table=f"blobs_{sg}") + \
            f'CREATE TABLE "blobs_{sg}" (id bytea NOT NULL, json bytea NOT NULL);' + \
            f'\nALTER TABLE "blobs_{sg}" ALTER COLUMN json SET STORAGE EXTERNAL;'
        load_table(f"blobs_{sg}", create_blobs, blobs_pgbins, psql_base)

        # --- AUTOCOMPLETE TABLE ---
        create_autocomplete = drop_prefix.format(table=f"autocomplete_{sg}") + \
            f'CREATE TABLE "autocomplete_{sg}" (label TEXT NOT NULL) WITH (fillfactor=100);'
        load_table(f"autocomplete_{sg}", create_autocomplete, autocomplete_pgbins, psql_base)

        # --- MATERIALISED QUERIES TABLE ---
        create_mat = drop_prefix.format(table=f"materialised_queries_{sg}") + \
            f'CREATE TABLE "materialised_queries_{sg}" (query_id TEXT NOT NULL, row_number INT NOT NULL, data JSONB NOT NULL) WITH (fillfactor=100);'
        load_table(f"materialised_queries_{sg}", create_mat, mat_queries_pgbins, psql_base)

        # --- INDEXES ---
        create_indexes_for_subgraph(
            sg,
            nodes_cols_file,
            psql_base,
            parallel_workers=parallel_workers,
            maintenance_work_mem=maintenance_work_mem,
        )

        # --- ANALYZE ---
        analyze_stmts = "\n".join([
            f'ANALYZE "edges_{sg}";',
            f'ANALYZE "nodes_{sg}";',
            f'ANALYZE "blobs_{sg}";',
            f'ANALYZE "autocomplete_{sg}";',
            f'ANALYZE "materialised_queries_{sg}";',
        ])
        run_psql(analyze_stmts, f"analyze_{sg}", psql_base)

    # --- GRAPH METADATA TABLE ---
    print("=== Loading graph metadata ===", flush=True)
    drop_meta = 'DROP TABLE IF EXISTS graph_metadata CASCADE;\n' if drop_existing else ""
    create_meta = (
        f"{drop_meta}"
        "CREATE TABLE graph_metadata (graph TEXT PRIMARY KEY, metadata JSONB NOT NULL);"
    )

    metadata_files = sorted(glob.glob("*_metadata.json"))
    if metadata_files:
        # Write TSV for COPY
        with open("graph_metadata.tsv", "w") as f:
            import csv
            w = csv.writer(f, delimiter="\t")
            for mf in metadata_files:
                sg = mf.removesuffix("_metadata.json")
                with open(mf) as fi:
                    w.writerow([sg, json.dumps(json.load(fi), separators=(",", ":"))])

        abs_tsv = str(Path("graph_metadata.tsv").resolve())
        run_psql(
            f"{create_meta}\n"
            f"\\copy graph_metadata FROM '{abs_tsv}' WITH (FORMAT csv, DELIMITER E'\\t')",
            "graph_metadata",
            psql_base,
        )
    else:
        run_psql(create_meta, "graph_metadata", psql_base)

    run_psql("ANALYZE graph_metadata;", "analyze_metadata", psql_base)

    # --- PREFIX MAP TABLE ---
    # The runtime prefix-normalisation helper (grebi_reprefix, spawned by the
    # Java backend) reads its prefix map from this table instead of an NFS file.
    # Stored as a single BYTEA blob holding the whole prefix_map_normalise.json,
    # mirroring how OLS stores its PCA models in postgres.
    print("=== Loading prefix map ===", flush=True)
    prefix_map_path = os.environ.get(
        "GREBI_PREFIX_MAP_PATH",
        "/opt/grebi_dataload/prefix_maps/prefix_map_normalise.json",
    )
    prefix_map_hex = Path(prefix_map_path).read_bytes().hex()
    drop_prefix_map = 'DROP TABLE IF EXISTS grebi_prefix_map CASCADE;\n' if drop_existing else ""
    run_psql(
        f"{drop_prefix_map}"
        "CREATE TABLE grebi_prefix_map (name TEXT PRIMARY KEY, data BYTEA NOT NULL);\n"
        "DELETE FROM grebi_prefix_map WHERE name = 'prefix_map_normalise';\n"
        f"INSERT INTO grebi_prefix_map (name, data) VALUES ('prefix_map_normalise', '\\x{prefix_map_hex}');",
        "grebi_prefix_map",
        psql_base,
    )

    print("=== All data loaded ===", flush=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load grebi data into PostgreSQL using COPY FREEZE")
    parser.add_argument("--local", action="store_true",
                        help="Local mode: tables are freshly created (no DROP)")
    parser.add_argument("--parallel-workers", type=int, default=0,
                        help="Per-table parallel_workers and max_parallel_maintenance_workers for index builds")
    parser.add_argument("--maintenance-work-mem", default="",
                        help="Session maintenance_work_mem for index builds")
    args = parser.parse_args()

    load_all(
        drop_existing=not args.local,
        parallel_workers=args.parallel_workers,
        maintenance_work_mem=args.maintenance_work_mem,
    )
