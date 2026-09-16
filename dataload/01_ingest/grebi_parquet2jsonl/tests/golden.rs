use grebi_golden::GoldenCase;

/// Rows come out as JSON lines; nulls are omitted and lists stay lists.
#[test]
fn converts_parquet_rows_to_json_lines() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_parquet2jsonl"), "tests/golden/basic")
        .stdin("input.parquet")
        .stdout("output.jsonl")
        .run();
}
