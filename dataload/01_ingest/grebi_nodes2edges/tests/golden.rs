use grebi_golden::GoldenCase;

/// Each line becomes one edge per (from, to) pair, the other keys travelling
/// as the edge's reified properties.
#[test]
fn makes_an_edge_per_from_to_pair() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_nodes2edges"), "tests/golden/basic")
        .args(["--from-field", "subject", "--to-field", "object", "--edge-type", "ex:relatedTo"])
        .stdin("input.jsonl")
        .stdout("output.jsonl")
        .run();
}

/// A long table with a predicate column: the edge type is the line's own
/// `column` value behind the prefix and is not repeated as metadata, and a
/// line with no target (an unmapped row) or no type is skipped, not an edge.
#[test]
fn takes_the_edge_type_from_a_field_and_skips_incomplete_lines() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_nodes2edges"), "tests/golden/type_from_field")
        .args(["--from-field", "id", "--to-field", "mapping_iri", "--edge-type-field", "column", "--edge-type-prefix", "ai_anno:"])
        .stdin("input.jsonl")
        .stdout("output.jsonl")
        .stderr("stderr.txt")
        .run();
}
