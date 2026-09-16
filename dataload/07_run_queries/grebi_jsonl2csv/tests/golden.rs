use grebi_golden::GoldenCase;

/// The rows the Python exporter was pinned with. Compared with python.csv in the
/// same directory, the differences are deliberate: null and a missing key are
/// empty cells rather than "None", booleans are lower case, numbers keep their
/// JSON spelling and objects are compact JSON.
#[test]
fn rows_become_csv_with_the_union_of_keys_as_header() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_jsonl2csv"), "tests/golden/results")
        .stdin("results.jsonl")
        .stdout("results.csv")
        .run();
}

/// No rows at all gives an empty file rather than a header.
#[test]
fn no_rows_no_output() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_jsonl2csv"), "tests/golden/empty")
        .stdin("empty.jsonl")
        .stdout("empty.csv")
        .run();
}
