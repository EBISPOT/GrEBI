use grebi_golden::GoldenCase;

/// Query result rows with their node references resolved, as the pipeline recorded them.
#[test]
fn links_query_result_rows_to_entities() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_link_results"), "tests/golden/hello_world_tester")
        .args(["--in-metadata-jsonl", "$CASE/entity_metadata.jsonl", "--groups-txt", "$CASE/groups.txt"]).stdin("hello_world_tester.results.jsonl").stdout("hello_world_tester.linked_results.jsonl")
        .run();
}
