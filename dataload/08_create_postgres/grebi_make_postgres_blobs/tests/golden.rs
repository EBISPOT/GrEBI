use grebi_golden::GoldenCase;

/// The blobs COPY BINARY file, as the pipeline recorded it.
#[test]
fn writes_the_blobs_copy_file() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_make_postgres_blobs"), "tests/golden/test_clique_merge")
        .stdin("nodes_compressed.blob").stdout("postgres_blobs.pgbin")
        .run();
}
