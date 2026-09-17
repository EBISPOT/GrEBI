use grebi_golden::GoldenCase;

/// Entity metadata, graph metadata, names and ids of the merged clique-merge entities, as the pipeline recorded them.
#[test]
fn indexes_the_merged_entities() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_index"), "tests/golden/test_clique_merge")
        .args(["--subgraph-name", "test_clique_merge", "--subgraph-config-json-path", "$CASE/subgraph_config.json", "--out-entity-metadata-jsonl-path", "entity_metadata.jsonl", "--out-graph-metadata-json-path", "graph_metadata.json", "--out-names-txt", "names.txt", "--out-ids-txt", "ids_test_clique_merge.txt"]).stdin("merged.jsonl").output("entity_metadata.jsonl").output("graph_metadata.json").output("names.txt").output("ids_test_clique_merge.txt")
        .run();
}

/// Translations (reified values carrying grebi:lang) are neither entity
/// metadata names nor autocomplete names: both stay in the graph's language.
#[test]
fn translations_are_not_names() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_index"), "tests/golden/translated_labels")
        .args(["--subgraph-name", "test_translations", "--subgraph-config-json-path", "$CASE/subgraph_config.json",
               "--out-entity-metadata-jsonl-path", "entity_metadata.jsonl", "--out-graph-metadata-json-path", "graph_metadata.json",
               "--out-names-txt", "names.txt", "--out-ids-txt", "ids.txt"])
        .stdin("merged.jsonl")
        .output("entity_metadata.jsonl").output("graph_metadata.json").output("names.txt").output("ids.txt")
        .run();
}
