use grebi_golden::GoldenCase;

/// Turtle triples grouped by subject: types become grebi:type, a predicate is
/// renamed, the objects of one predicate are nested into their subject, and the
/// objects of another are not written as subjects of their own.
#[test]
fn turtle_subjects_with_nesting_and_renaming() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_rdf2jsonl"), "tests/golden/turtle")
        .args(["--rdf-type", "rdf_triples_turtle", "--rdf-types-are-grebi-types",
               "--nest-objects-of-predicate", "http://example.org/hasName",
               "--exclude-objects-of-predicate", "http://example.org/hasSecret",
               "--map-predicate", "http://www.w3.org/2000/01/rdf-schema#label=grebi:name"])
        .stdin("input.ttl")
        .stdout("output.jsonl")
        .run();
}

/// N-Quads: only the named graph asked for is loaded.
#[test]
fn nquads_from_one_named_graph() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_rdf2jsonl"), "tests/golden/nquads")
        .args(["--rdf-type", "rdf_quads_nq", "--rdf-graph", "http://example.org/graphs/keep"])
        .stdin("input.nq")
        .stdout("output.jsonl")
        .run();
}
