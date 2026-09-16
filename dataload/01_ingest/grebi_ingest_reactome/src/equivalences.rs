//! The identifiers a Reactome node should be clique-merged with.

use grebi_shared::prefix_map::PrefixMap;
use serde_json::{Map, Value};

/// Normalised CURIEs for the external identifiers a Reactome node carries.
///
/// * `stId`: the stable Reactome id (`R-HSA-...`), always kept.
/// * `url`: compacted when it is a known identifier URL (UniProt, Ensembl, ...).
/// * `identifier`: on its own when it already is a CURIE, otherwise joined
///   with `databaseName` (`DOID` + `8893` -> `doid:8893`). Reactome's disease
///   terms only carry an OLS search URL, which does not compact, so this join
///   is what merges them with the ontology nodes (#26). The join is only
///   trusted when the database name is itself a known prefix, so that
///   `ENSEMBL_homo_sapiens_GENE` cannot prefix-match `ensembl_` and yield the
///   junk id `ensembl:homo_sapiens_GENE:ENSG1`.
/// * `taxId`: NCBI taxonomy.
pub fn equivalences(properties: &Map<String, Value>, normalise: &PrefixMap) -> Vec<String> {
    let text = |key: &str| properties.get(key).and_then(Value::as_str);
    let mut out: Vec<String> = Vec::new();
    let mut push = |curie: String| {
        if !out.contains(&curie) {
            out.push(curie);
        }
    };

    if let Some(st_id) = text("stId") {
        push("reactome:".to_owned() + st_id);
    }
    if let Some(url) = text("url") {
        if let Some(curie) = normalise.maybe_reprefix(&url.to_owned()) {
            push(curie);
        }
    }
    if let Some(identifier) = text("identifier") {
        let curie = normalise.maybe_reprefix(&identifier.to_owned()).or_else(|| {
            text("databaseName")
                .filter(|db| is_known_prefix(db, normalise))
                .and_then(|db| normalise.maybe_reprefix(&(db.to_owned() + ":" + identifier)))
        });
        if let Some(curie) = curie {
            push(curie);
        }
    }
    if let Some(tax_id) = text("taxId") {
        push("ncbitaxon:".to_owned() + tax_id);
    }
    out
}

/// Whether `DB:` as a whole is a prefix the map knows. Every mapped prefix
/// normalises to a bare `name:`, so anything left over after the rewrite means
/// only part of the database name matched.
fn is_known_prefix(db: &str, normalise: &PrefixMap) -> bool {
    match normalise.maybe_reprefix(&(db.to_owned() + ":")) {
        Some(rewritten) => rewritten.ends_with(':') && rewritten.matches(':').count() == 1,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grebi_shared::prefix_map::PrefixMapBuilder;
    use serde_json::json;
    use std::collections::HashMap;

    /// The real normalisation map, as the pipeline loads it.
    fn normalise() -> PrefixMap {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../prefix_maps/prefix_map_normalise.json");
        let map: HashMap<String, String> =
            serde_json::from_reader(std::fs::File::open(path).unwrap()).unwrap();
        let mut builder = PrefixMapBuilder::new();
        for (from, to) in map {
            builder.add_mapping(from, to);
        }
        builder.build()
    }

    fn eq(props: Value) -> Vec<String> {
        equivalences(props.as_object().unwrap(), &normalise())
    }

    #[test]
    fn disease_merges_through_database_name_and_identifier() {
        // as exported for psoriasis (dbId 9009814); the OLS url does not compact
        assert_eq!(
            eq(json!({"schemaClass": "Disease", "databaseName": "DOID", "identifier": "8893",
                      "name": "psoriasis",
                      "url": "https://www.ebi.ac.uk/ols/ontologies/doid/terms?obo_id=DOID:8893"})),
            vec!["doid:8893"]
        );
    }

    #[test]
    fn go_term_merges_the_same_way() {
        assert_eq!(eq(json!({"databaseName": "GO", "identifier": "0005634"})), vec!["go:0005634"]);
    }

    #[test]
    fn identifier_that_is_already_a_curie_is_used_as_is() {
        assert_eq!(eq(json!({"databaseName": "GO", "identifier": "GO:0005634"})), vec!["go:0005634"]);
    }

    #[test]
    fn url_and_identifier_do_not_duplicate() {
        assert_eq!(
            eq(json!({"databaseName": "UniProt", "identifier": "P12345",
                      "url": "http://www.uniprot.org/entry/P12345"})),
            vec!["uniprot:P12345"]
        );
    }

    #[test]
    fn database_name_that_only_prefix_matches_is_rejected() {
        // "ensembl_" is a known prefix, but this database name is not one
        assert_eq!(
            eq(json!({"databaseName": "ENSEMBL_homo_sapiens_GENE", "identifier": "ENSG00000156508.19"})),
            Vec::<String>::new()
        );
        assert_eq!(eq(json!({"databaseName": "ENSEMBL", "identifier": "ENSG00000156508"})), vec!["ensembl:ENSG00000156508"]);
    }

    #[test]
    fn unknown_database_yields_nothing() {
        assert_eq!(eq(json!({"databaseName": "NoSuchDb", "identifier": "X1"})), Vec::<String>::new());
        assert_eq!(eq(json!({"identifier": "8893"})), Vec::<String>::new());
    }

    #[test]
    fn stable_id_and_taxon() {
        assert_eq!(
            eq(json!({"stId": "R-HSA-445355", "taxId": "9606", "displayName": "Smooth Muscle Contraction"})),
            vec!["reactome:R-HSA-445355", "ncbitaxon:9606"]
        );
    }
}
