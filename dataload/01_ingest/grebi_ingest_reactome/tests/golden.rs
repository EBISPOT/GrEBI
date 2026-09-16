use grebi_golden::GoldenCase;

/// The Reactome dump objects of the test subgraph, with the DOID-annotated
/// disease that must clique-merge into the ontology term. The ingester reads
/// the prefix map from the dataload home, as the pipeline's environment gives it.
#[test]
fn ingests_reactome_objects() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_reactome"), "tests/golden/test_reactome")
        .env("GREBI_DATALOAD_HOME", concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .env("GREBI_DATASOURCE_ID", "TestReactome")
        .env("GREBI_INGEST_DATASOURCE_NAME", "TestReactome")
        .stdin("input.jsonl")
        .stdout("output.jsonl")
        .run();
}
