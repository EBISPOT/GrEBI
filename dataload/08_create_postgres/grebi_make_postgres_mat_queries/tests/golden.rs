use grebi_golden::GoldenCase;

/// The COPY BINARY file and the column and index DDL of a materialised query, as the pipeline recorded them.
#[test]
fn writes_the_materialised_query_table_and_its_ddl() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_make_postgres_mat_queries"), "tests/golden/hello_world_tester")
        .args(["--in-metadata-json", "$CASE/hello_world_tester.json"]).stdin("hello_world_tester.linked_results.jsonl").output("matq_test_clique_merge_hello_world_tester.columns").output("matq_test_clique_merge_hello_world_tester.indexes").output("matq_test_clique_merge_hello_world_tester.pgbin")
        .run();
}
