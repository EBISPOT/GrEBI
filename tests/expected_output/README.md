# E2E Test Expected Output

This directory contains subdirectories for each test subgraph with expected
snapshot files used by the E2E tests to verify pipeline output hasn't changed.

## Structure

```
expected_output/
  test_clique_merge/
    test_clique_merge_snapshot_neo4j_nodes.jsonl
    test_clique_merge_snapshot_neo4j_edges.jsonl
    test_clique_merge_snapshot_postgres_nodes.jsonl
    test_clique_merge_snapshot_postgres_edges.jsonl
    test_clique_merge_api_snapshot.json        (optional)
  test_edge_linking/
    ...
  test_multi_datasource/
    ...
  test_type_hierarchy/
    ...
```

## How It Works

When `--export_snapshots true` is passed to the pipeline, the integration test
process (which already has Neo4j, Postgres, and the API running) will:

1. Export DB snapshots (Neo4j nodes/edges, Postgres nodes/edges) to JSONL files
2. Export an API response snapshot (`<subgraph>_api_snapshot.json`) by querying
   the running API for every node's details and incoming/outgoing edges
3. Compare the DB snapshots against expected output in this directory (if present)
4. Compare the API snapshot against expected output (if present)

Both the DB snapshots and the API snapshot are always exported and published to
the pipeline output directory (`out/<subgraph>/`) for initial population; the
comparisons in steps 3–4 are skipped when no expected output exists yet.

## Updating Expected Output

When the pipeline output intentionally changes (e.g. adding new data or 
changing processing logic), update the expected output by running the pipeline
with snapshot export and copying the new output:

```bash
# Run pipeline for a test subgraph with snapshot export
export GREBI_SUBGRAPHS=test_clique_merge
export GREBI_NF_EXTRA_ARGS="--export_snapshots true"
./dataload/scripts/dataload_local.sh

# Copy new DB snapshots to expected
cp out/test_clique_merge/test_clique_merge_snapshot_*.jsonl \
   tests/expected_output/test_clique_merge/

# Commit the changes
git add tests/expected_output/
git commit -m "Update expected E2E output for test_clique_merge"
```
