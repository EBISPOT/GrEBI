use grebi_golden::GoldenCase;

/// Nodes whose ancestors include a configured type superclass get it as a type. The input is the assign_ids golden output.
#[test]
fn adds_the_type_superclasses() {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_superclasses2types"), "tests/golden/test_clique_merge")
        .args(["--type-superclasses", "mondo:0000001,efo:0000408,chebi:36080,chebi:24431,biolink:ChemicalEntity", "--groups-txt", "$CASE/groups.txt"]).stdin("input.jsonl").stdout("output.jsonl")
        .run();
}
