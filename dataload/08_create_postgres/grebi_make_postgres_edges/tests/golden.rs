use grebi_golden::GoldenCase;

/// The edges COPY BINARY file and column list, as the pipeline recorded them.
#[test]
fn writes_the_edges_copy_file_and_its_columns() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_make_postgres_edges"), "tests/golden/test_clique_merge")
        .args(["--in-edges-jsonl", "$CASE/linked_edges_test_clique_merge_000000.jsonl", "--in-graph-metadata-json", "$CASE/graph_metadata.json", "--out-edges-pgbin-path", "postgres_edges_test_clique_merge_000000.pgbin", "--out-columns-path", "postgres_edges_columns_test_clique_merge_000000.txt"]).output("postgres_edges_test_clique_merge_000000.pgbin").output("postgres_edges_columns_test_clique_merge_000000.txt")
        .run();
}
