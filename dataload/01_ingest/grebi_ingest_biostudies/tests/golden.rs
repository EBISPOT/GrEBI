use grebi_golden::GoldenCase;

/// The BioStudies test subgraph's tree, matched as JSON against what the Python
/// script this replaced produced for it.
#[test]
fn matches_the_python_ingest_on_the_test_subgraph() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_biostudies"), "tests/golden/test_biostudies")
        .args(["--", "$CASE/fire"])
        .stdout("output.jsonl")
        .jsonl()
        .run();
}

/// A tree exercising the rest of the rules: collections, aliases, section
/// types, publications, every kind of link, ontology term qualifiers, nested
/// subsections, the directories the walk skips, a duplicate accession, a
/// half-written file and a vanished symlink the walk must warn about and skip;
/// plus a single PageTab file given directly and a Europe PMC one to exclude.
#[test]
fn matches_the_python_ingest_on_every_branch() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_biostudies"), "tests/golden/branches")
        .args(["--", "$CASE/tree", "$CASE/single.json", "$CASE/S-EPMC9.json"])
        .stdout("output.jsonl")
        .jsonl()
        .run();
}

/// Unlike a file found by the walk, a file named on the command line must parse.
#[test]
fn a_corrupt_file_named_directly_is_an_error() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_biostudies"), "tests/golden/named_corrupt_file")
        .args(["--", "$CASE/S-BSST2.json"])
        .expect_failure()
        .run();
}
