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

fn refused(name: &str) -> GoldenCase {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_ingest_pride"), format!("tests/golden/{}", name))
        .args(["--", "$CASE/projects.json"])
        .expect_failure()
}

/// An export that is not an array of projects is refused.
#[test]
fn refuses_anything_but_a_projects_array() {
    refused("not_an_array").run();
}

/// The metadata whitelist and cross-reference rules from the ingest's original
/// unit tests: file inventories, download counters and contact emails never
/// enter the graph; sample annotations and other-omics links are mapped; legacy
/// PRD and affinity PAD accessions get no ProteomeXchange alias; and titles with
/// brackets, quotes and non-ASCII text stream through intact.
#[test]
fn matches_the_python_ingest_on_excluded_fields_and_cross_references() {
    case("excluded_fields").stdout("output.jsonl").jsonl().run();
}

#[test]
fn refuses_an_empty_export() {
    refused("empty_export").run();
}

#[test]
fn refuses_a_duplicate_project() {
    refused("duplicate_export").run();
}

#[test]
fn refuses_a_truncated_export() {
    refused("truncated_export").run();
}

#[test]
fn refuses_content_after_the_array() {
    refused("trailing_content").run();
}
