use grebi_golden::GoldenCase;

/// The autocomplete COPY BINARY file of the GWAS test subgraph, as the pipeline recorded it.
#[test]
fn writes_the_autocomplete_copy_file() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_make_postgres_autocomplete"), "tests/golden/test_gwas")
        .stdin("names_sorted.txt").stdout("autocomplete.pgbin")
        .run();
}
