use grebi_golden::GoldenCase;

/// The first six classes, three properties and one individual of the EFO OLS
/// dump, as the ontology configs ingest them.
#[test]
fn ingests_an_ols_dump() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_ols"), "tests/golden/efo_sample")
        .args(["--ontologies", "efo", "--skip-obsolete"])
        .stdin("efo.json")
        .stdout("output.jsonl")
        .run();
}

/// An ontology that is not asked for produces nothing.
#[test]
fn other_ontologies_are_skipped() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_ols"), "tests/golden/efo_sample")
        .args(["--ontologies", "mondo"])
        .stdin("efo.json")
        .stdout("empty.jsonl")
        .run();
}
