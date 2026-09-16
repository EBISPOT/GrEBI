use grebi_golden::GoldenCase;

/// Matched as JSON against what the Python script this replaced produced.
#[test]
fn matches_the_python_ingest_it_replaced() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_mondo_efo_mappings"), "tests/golden/mappings")
        .args(["--datasource-name", "MONDO_EFO"])
        .stdin("mappings.tsv")
        .stdout("output.jsonl")
        .jsonl()
        .run();
}
