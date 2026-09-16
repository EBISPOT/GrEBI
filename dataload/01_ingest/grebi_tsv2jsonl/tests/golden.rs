use grebi_golden::GoldenCase;

/// Header row as keys, every value an array; a delimiter splits cells into
/// arrays and empty fields are dropped on request.
#[test]
fn header_row_with_array_cells() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_tsv2jsonl"), "tests/golden/header_row")
        .args(["--tsv-array-delimiter", "|", "--tsv-ignore-empty-fields"])
        .stdin("input.tsv")
        .stdout("output.jsonl")
        .run();
}

/// Column names given on the command line, comment lines skipped, and the
/// characters that need escaping in JSON.
#[test]
fn named_columns_without_a_header() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_tsv2jsonl"), "tests/golden/named_columns")
        .args(["--tsv-columns", "id,label,note"])
        .stdin("input.tsv")
        .stdout("output.jsonl")
        .run();
}
