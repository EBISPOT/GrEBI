use grebi_golden::GoldenCase;

/// The compressed node blobs, as the pipeline recorded them.
#[test]
fn compresses_each_node_blob() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_make_compressed_blob"), "tests/golden/test_clique_merge")
        .stdin("nodes.jsonl").stdout("nodes_compressed.blob")
        .run();
}
