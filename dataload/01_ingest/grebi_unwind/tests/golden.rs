use grebi_golden::GoldenCase;

/// One output line per element of the unwound array; a line with an empty
/// array produces nothing.
#[test]
fn unwinds_an_array_field_into_one_line_per_element() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_unwind"), "tests/golden/basic")
        .args(["--unwind-field", "genes"])
        .stdin("input.jsonl")
        .stdout("output.jsonl")
        .run();
}
