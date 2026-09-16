use grebi_golden::GoldenCase;

fn case(name: &str) -> GoldenCase {
    GoldenCase::new(env!("CARGO_BIN_EXE_grebi_transform_jsonl"), format!("tests/golden/{}", name))
        .stdin("input.jsonl")
        .stdout("output.jsonl")
}

/// The shape the datasource configs use for KGX-style node files: every key
/// except id gets the datasource prefix and category becomes the type.
#[test]
fn prefixes_keys_and_renames() {
    case("rename_prefix")
        .args(["--json-rename", "category:grebi:type", "--json-inject-key-prefix", "hgnc:"])
        .run();
}

/// Selecting columns, prefixing values, nesting a field's sub-object into a
/// reified value and adding a content hash id.
#[test]
fn selects_prefixes_values_and_hashes() {
    case("select_denest_hash")
        .args(["--json-select-keys", "PDB,CHAIN,SP_PRIMARY,xref", "--json-rename", "PDB:id",
               "--json-inject-key-prefix", "pdbe:", "--json-inject-value-prefix", "SP_PRIMARY:uniprot:",
               "--json-de-nest-field", "xref.id", "--json-inject-hashid"])
        .run();
}

/// Only lines matching the value filter pass, with a type injected and keys removed.
#[test]
fn filters_lines_by_value_and_injects_a_type() {
    case("select_by_value")
        .args(["--json-select-by-value", "kind:gene", "--json-inject-type", "biolink:Gene", "--json-remove-keys", "junk,kind"])
        .run();
}
