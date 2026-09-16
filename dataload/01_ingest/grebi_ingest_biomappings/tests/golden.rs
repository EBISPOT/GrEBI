use grebi_golden::GoldenCase;

/// Matched as JSON against what the Python script this replaced produced.
#[test]
fn matches_the_python_ingest_it_replaced() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_biomappings"), "tests/golden/mappings")
        .env("GREBI_INGEST_FILENAME", "$CASE/mappings.tsv")
        .stdout("output.jsonl")
        .jsonl()
        .run();
}
