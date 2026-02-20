# E2E Test Expected Output

This directory contains subdirectories for each test subgraph with expected
snapshot files used by the E2E tests to verify pipeline output hasn't changed.

## Structure

```
expected_output/
  test_clique_merge/
    test_clique_merge_snapshot_neo4j_nodes.jsonl
    test_clique_merge_snapshot_neo4j_edges.jsonl
    test_clique_merge_snapshot_solr_nodes.jsonl
    test_clique_merge_snapshot_solr_edges.jsonl
  test_edge_linking/
    ...
  test_multi_datasource/
    ...
  test_type_hierarchy/
    ...
```

## Updating Expected Output

When the pipeline output intentionally changes (e.g. adding new data or 
changing processing logic), update the expected output by running the pipeline
with snapshot export and copying the new output:

```bash
# Run pipeline for a test subgraph with snapshot export
export GREBI_SUBGRAPH=test_clique_merge
export GREBI_NF_EXTRA_ARGS="--export_snapshots true"
./dataload/scripts/dataload_local.sh

# Copy new output to expected
cp out/test_clique_merge/test_clique_merge_snapshot_*.jsonl \
   e2e_tests/expected_output/test_clique_merge/

# Commit the changes
git add e2e_tests/expected_output/
git commit -m "Update expected E2E output for test_clique_merge"
```
