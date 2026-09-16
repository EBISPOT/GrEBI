use grebi_golden::GoldenCase;

fn case(name: &str) -> GoldenCase {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_pride"), format!("tests/golden/{}", name))
        .args(["--", "$CASE/projects.json"])
        .stdout("output.jsonl")
        .jsonl()
}

/// The PRIDE test subgraph's export, matched as JSON against what the Python
/// script this replaced produced for it.
#[test]
fn matches_the_python_ingest_on_the_test_subgraph() {
    case("test_pride").run();
}

/// Projects exercising the rest of the rules: accession kinds, DOI spellings,
/// ontology terms with and without labels, sample attributes, ORCIDs,
/// references and the omics links that are kept or dropped.
#[test]
fn matches_the_python_ingest_on_every_branch() {
    case("branches").run();
}

/// An export that is not an array of projects is refused.
#[test]
fn refuses_anything_but_a_projects_array() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_pride"), "tests/golden/not_an_array")
        .args(["--", "$CASE/projects.json"])
        .expect_failure()
        .run();
}
