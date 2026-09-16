//! PrimeKG's kg.csv (one edge per row) as GrEBI JSONL.
//!
//! Each row yields the source node with the edge as a reified property, and a
//! bare record for the target node so it exists even if it is never a source.
//! Ids are the row's source name and local id as a CURIE; the prefix normaliser
//! maps the source names (MONDO, HPO, GO, UBERON, REACTOME, DrugBank, UMLS) onto
//! the graph's prefixes. PrimeKG drops the leading zeros of ontology ids, so
//! those are padded back to seven digits. Two source names are not prefixes
//! and are rewritten: NCBI (gene ids) to NCBIGene, and CTD, whose exposure ids
//! are MeSH descriptor and supplementary-record UIs (D000075182, C092102), to
//! MESH, so the exposures land on the MeSH nodes the CTD and MeSH datasources
//! already share.

use serde_json::{Map, Value};
use std::io::{self, BufWriter, Write};

const PADDED_SOURCES: &[(&str, usize)] = &[("MONDO", 7), ("HPO", 7), ("GO", 7), ("UBERON", 7)];
const SOURCE_PREFIX: &[(&str, &str)] = &[("NCBI", "NCBIGene"), ("CTD", "MESH")];

/// The CURIE for a PrimeKG source name and local id.
pub fn curie(source: &str, local_id: &str) -> String {
    let mut local = local_id.to_string();
    if let Some((_, width)) = PADDED_SOURCES.iter().find(|(s, _)| *s == source) {
        if !local.is_empty() && local.bytes().all(|b| b.is_ascii_digit()) {
            local = format!("{:0>width$}", local, width = *width);
        }
    }
    let prefix = SOURCE_PREFIX.iter().find(|(s, _)| *s == source).map(|(_, p)| *p).unwrap_or(source);
    format!("{}:{}", prefix, local)
}

/// The source node (with the edge as a reified property) and the bare target
/// node for one row, given as (column, value) pairs in column order.
pub fn records(row: &[(String, String)]) -> (Value, Value) {
    let get = |name: &str| row.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str()).unwrap_or("");
    let x_id = curie(get("x_source"), get("x_id"));
    let y_id = curie(get("y_source"), get("y_id"));

    let mut properties = Map::new();
    for (key, value) in row {
        properties.insert(format!("primekg:{}", key), Value::Array(vec![Value::String(value.clone())]));
    }
    let mut edge = Map::new();
    edge.insert("grebi:value".into(), Value::String(y_id.clone()));
    edge.insert("grebi:properties".into(), Value::Object(properties));

    let mut source = Map::new();
    source.insert("id".into(), Value::String(x_id));
    source.insert("grebi:name".into(), Value::String(get("x_name").to_string()));
    source.insert("grebi:type".into(), Value::String("biolink:Entity".into()));
    source.insert(format!("primekg:{}", get("relation")), Value::Object(edge));

    let mut target = Map::new();
    target.insert("id".into(), Value::String(y_id));
    target.insert("grebi:name".into(), Value::String(get("y_name").to_string()));
    target.insert("grebi:type".into(), Value::String("biolink:Entity".into()));

    (Value::Object(source), Value::Object(target))
}

fn main() {
    let mut reader = csv::Reader::from_reader(io::stdin().lock());
    let headers: Vec<String> = reader.headers().expect("kg.csv header").iter().map(|h| h.to_string()).collect();
    let mut out = BufWriter::new(io::stdout().lock());
    for record in reader.records() {
        let record = record.expect("kg.csv row");
        let row: Vec<(String, String)> = headers.iter().cloned().zip(record.iter().map(|v| v.to_string())).collect();
        let (source, target) = records(&row);
        serde_json::to_writer(&mut out, &source).unwrap();
        out.write_all(b"\n").unwrap();
        serde_json::to_writer(&mut out, &target).unwrap();
        out.write_all(b"\n").unwrap();
    }
    out.flush().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ontology_ids_are_padded_to_seven_digits() {
        // as PrimeKG writes them: leading zeros dropped
        assert_eq!(curie("MONDO", "5083"), "MONDO:0005083");
        assert_eq!(curie("MONDO", "482"), "MONDO:0000482");
        assert_eq!(curie("HPO", "855"), "HPO:0000855");
        assert_eq!(curie("GO", "51581"), "GO:0051581");
        assert_eq!(curie("UBERON", "2"), "UBERON:0000002");
    }

    #[test]
    fn already_seven_digits_unchanged() {
        assert_eq!(curie("GO", "1903891"), "GO:1903891");
        assert_eq!(curie("UBERON", "8000004"), "UBERON:8000004");
    }

    #[test]
    fn other_sources_untouched() {
        assert_eq!(curie("DrugBank", "DB09130"), "DrugBank:DB09130");
        assert_eq!(curie("REACTOME", "R-HSA-109581"), "REACTOME:R-HSA-109581");
        assert_eq!(curie("MONDO_grouped", "1200_1134_15512"), "MONDO_grouped:1200_1134_15512");
    }

    #[test]
    fn ncbi_genes_and_ctd_exposures_get_their_real_prefixes() {
        assert_eq!(curie("NCBI", "9796"), "NCBIGene:9796");
        assert_eq!(curie("CTD", "D000075182"), "MESH:D000075182");
        assert_eq!(curie("CTD", "C092102"), "MESH:C092102");
    }

    #[test]
    fn an_edge_row_yields_the_source_with_the_edge_and_a_bare_target() {
        let row: Vec<(String, String)> = [
            ("relation", "indication"), ("display_relation", "indication"), ("x_index", "1"), ("x_id", "DB00001"),
            ("x_type", "drug"), ("x_name", "Lepirudin"), ("x_source", "DrugBank"), ("y_index", "2"), ("y_id", "5083"),
            ("y_type", "disease"), ("y_name", "psoriasis"), ("y_source", "MONDO"),
        ].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let (source, target) = records(&row);
        assert_eq!(source["id"], "DrugBank:DB00001");
        assert_eq!(source["primekg:indication"]["grebi:value"], "MONDO:0005083");
        assert_eq!(source["primekg:indication"]["grebi:properties"]["primekg:y_id"], serde_json::json!(["5083"]));
        assert_eq!(target, serde_json::json!({"id": "MONDO:0005083", "grebi:name": "psoriasis", "grebi:type": "biolink:Entity"}));
        // keys in the order the pipeline's consumers were written against
        assert_eq!(source.as_object().unwrap().keys().collect::<Vec<_>>(), vec!["id", "grebi:name", "grebi:type", "primekg:indication"]);
    }
}
