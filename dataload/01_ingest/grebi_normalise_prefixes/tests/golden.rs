use grebi_golden::GoldenCase;

/// Every JSON string, keys included, goes through the prefix map; the longest
/// matching prefix wins and anything unmapped is left as it is.
#[test]
fn rewrites_every_string_through_the_prefix_map() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_normalise_prefixes"), "tests/golden/basic")
        .arg("$CASE/prefix_map.json")
        .stdin("input.jsonl")
        .stdout("output.jsonl")
        .run();
}

/// The ingested clique-merge edges through the real prefix map, matching what the pipeline recorded for them.
#[test]
fn normalises_with_the_pipelines_prefix_map() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_normalise_prefixes"), "tests/golden/real_prefix_map")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../../prefix_maps/prefix_map_normalise.json")).stdin("kgx_edges.jsonl").stdout("expected.jsonl")
        .run();
}
