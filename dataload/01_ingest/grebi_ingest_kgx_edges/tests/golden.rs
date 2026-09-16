use grebi_golden::GoldenCase;

/// The KGX edges of the clique-merge test subgraph, as its datasource config ingests them.
#[test]
fn kgx_edges_become_reified_subject_properties() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_kgx_edges"), "tests/golden/test_clique_merge")
        .args(["--kgx-inject-key-prefix", "test_cm:"]).stdin("edges.jsonl").stdout("output.jsonl")
        .run();
}
