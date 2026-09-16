use grebi_golden::GoldenCase;

/// The PrimeKG test rows, matched as JSON against what the Python ingest this
/// replaced produced for them (recorded before the port).
#[test]
fn matches_the_python_ingest_it_replaced() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_primekg"), "tests/golden/test_primekg")
        .stdin("kg.csv")
        .stdout("output.jsonl")
        .jsonl()
        .run();
}
