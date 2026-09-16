use grebi_golden::GoldenCase;

/// A mapping row becomes a subject with a reified predicate value whose
/// properties are the remaining columns; the curie map in the YAML header
/// expands the prefixes of every id.
#[test]
fn turns_mapping_rows_into_reified_predicate_values() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_sssom"), "tests/golden/basic")
        .stdin("input.sssom.tsv")
        .stdout("output.jsonl")
        .run();
}
