use grebi_golden::GoldenCase;

/// The Id node CSV, as the pipeline recorded it.
#[test]
fn writes_the_id_nodes_csv() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_make_neo_ids_csv"), "tests/golden/test_clique_merge")
        .stdin("ids.txt").stdout("neo_nodes_ids.csv")
        .run();
}
