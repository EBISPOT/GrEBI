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
