use grebi_golden::GoldenCase;

/// Every table becomes a node type named after the datasource and the
/// singular table name; primary keys become ids, foreign keys become ids of
/// the referenced table, nulls are dropped and blobs are hex.
#[test]
fn dumps_every_table_as_nodes() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_sqlite"), "tests/golden/two_tables")
        .args(["--datasource-name", "TestDb", "--filename", "$CASE/input.sqlite"])
        .stdout("output.jsonl")
        .run();
}
