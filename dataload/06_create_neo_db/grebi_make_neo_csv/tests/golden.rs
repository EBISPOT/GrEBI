use grebi_golden::GoldenCase;

/// Node, edge and id-edge CSVs for the Neo4j import, as the pipeline recorded them.
#[test]
fn writes_the_neo4j_import_csvs() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_make_neo_csv"), "tests/golden/test_clique_merge")
        .args(["--in-graph-metadata-jsons", "$CASE/graph_metadata.json", "--in-nodes-jsonl", "$CASE/linked_nodes_test_clique_merge_000000.jsonl", "--in-edges-jsonl", "$CASE/linked_edges_test_clique_merge_000000.jsonl", "--out-nodes-csv-path", "neo_nodes_test_clique_merge_000000.csv", "--out-edges-csv-path", "neo_edges_test_clique_merge_000000.csv", "--out-id-edges-csv-path", "neo_edges_ids_test_clique_merge_000000.csv", "--add-prefix", "test_clique_merge:"]).output("neo_nodes_test_clique_merge_000000.csv").output("neo_edges_test_clique_merge_000000.csv").output("neo_edges_ids_test_clique_merge_000000.csv")
        .run();
}
