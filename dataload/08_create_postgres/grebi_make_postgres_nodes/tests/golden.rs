use grebi_golden::GoldenCase;

/// The nodes COPY BINARY file and column list, as the pipeline recorded them.
#[test]
fn writes_the_nodes_copy_file_and_its_columns() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_make_postgres_nodes"), "tests/golden/test_clique_merge")
        .args(["--in-nodes-jsonl", "$CASE/linked_nodes_test_clique_merge_000000.jsonl", "--in-graph-metadata-json", "$CASE/test_clique_merge_metadata_merged.json", "--out-nodes-pgbin-path", "postgres_nodes_test_clique_merge_000000.pgbin", "--out-columns-path", "postgres_nodes_columns_test_clique_merge_000000.txt"]).output("postgres_nodes_test_clique_merge_000000.pgbin").output("postgres_nodes_columns_test_clique_merge_000000.txt")
        .run();
}
