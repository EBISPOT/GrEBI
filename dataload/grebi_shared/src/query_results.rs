//! The typed reading of a materialised query's result rows.
//!
//! A query's metadata JSON (written by run_queries) lists its result columns
//! and their types. The Postgres table writer and the download table writer
//! both read a row's JSON values through these functions, so that a column
//! holds the same values in the database and in the files.

use serde_json::Value;

pub enum ColKind {
    Node,      // GraphNodeId: a node object {id, grebi:nodeId, grebi:name, grebi:type}
    TextArray, // DatasourceList
    Float,
    Int,
    Text,      // string, EdgeId, PubmedId and anything else
}

pub struct Col {
    pub id: String,
    /// The column_type as the template has it
    pub column_type: String,
    pub kind: ColKind,
    /// For a node column, the prefixes of the identifiers the column is about,
    /// most wanted first ("mondo:" for a disease). See node_identifier.
    pub id_prefixes: Vec<String>,
}

/// The result columns the metadata lists, in the template's order.
pub fn columns(metadata: &Value) -> Vec<Col> {
    metadata
        .get("columns")
        .and_then(|v| v.as_array())
        .expect("metadata json has no `columns`")
        .iter()
        .map(|c| {
            let id = c.get("column_id").and_then(|v| v.as_str()).expect("column_id").to_string();
            let column_type = c.get("column_type").and_then(|v| v.as_str()).unwrap_or("string").to_string();
            let kind = match column_type.as_str() {
                "GraphNodeId" => ColKind::Node,
                "DatasourceList" => ColKind::TextArray,
                "float" => ColKind::Float,
                "int" | "integer" => ColKind::Int,
                _ => ColKind::Text,
            };
            let id_prefixes = c.get("id_prefixes").map(flatten_to_strings).unwrap_or_default();
            Col { id, column_type, kind, id_prefixes }
        })
        .collect()
}

/// What result rows put before a node id: "<subgraph>:". The nodes and edges
/// tables, and everything outside the graph, know the node by the bare id.
pub fn node_id_prefix(metadata: &Value) -> String {
    metadata
        .get("subgraph")
        .and_then(|v| v.as_str())
        .map(|s| format!("{}:", s))
        .unwrap_or_default()
}

/// A node value's grebi:nodeId, without the subgraph prefix.
pub fn node_id<'a>(v: Option<&'a Value>, nid_prefix: &str) -> Option<&'a str> {
    let nid = v?.get("grebi:nodeId")?.as_str()?;
    Some(nid.strip_prefix(nid_prefix).unwrap_or(nid))
}

/// Identifier prefixes in order of preference, for a node whose column does
/// not say what it is about or that has none of the identifiers it asks for.
/// The API picks the identifiers of its CSV exports from the same list
/// (GrebiCypherRepo.pickFavouriteSourceId).
pub const PREFERRED_ID_PREFIXES: [&str; 16] = [
    "grebi:", "biolink:", "ro:", "hp:", "mp:", "mondo:", "oba:", "efo:", "doid:", "hgnc:",
    "mgi:", "uniprot:", "pmid:", "chebi:", "MTBLS", "MTBLC",
];

/// The identifier a node is given outside the graph. A node stands for every
/// identifier that was merged into it, so there is a choice: the first of its
/// identifiers with the prefix the column wants most, then with the most
/// preferred of the prefixes above or, when it has none of those, the node's
/// own id. The names of a node are among its identifiers, and some identifiers
/// come with a name attached ("chebi:16393 SPHINGOSINE"); anything with a
/// space in it is passed over.
///
/// (Not for the Postgres tables: those know a node by its node id, which is
/// what the closure of a query parameter is matched against.)
pub fn node_identifier(node: Option<&Value>, column_prefixes: &[String], nid_prefix: &str) -> Option<String> {
    let ids = node?.get("id").map(flatten_to_strings).unwrap_or_default();
    let wanted = column_prefixes.iter().map(|p| p.as_str()).chain(PREFERRED_ID_PREFIXES);
    for prefix in wanted {
        if let Some(id) = ids.iter().find(|id| id.starts_with(prefix) && !id.contains(char::is_whitespace)) {
            return Some(id.clone());
        }
    }
    node_id(node, nid_prefix).map(|id| id.to_string())
}

/// A node value's name: the first of its grebi:name.
pub fn node_name(v: Option<&Value>) -> Option<String> {
    v?.get("grebi:name").map(flatten_to_strings)?.into_iter().next()
}

pub fn flatten_to_strings(v: &Value) -> Vec<String> {
    match v {
        Value::String(s) => vec![s.clone()],
        Value::Array(arr) => arr.iter().flat_map(flatten_to_strings).collect(),
        Value::Null => vec![],
        other => vec![serde_json::to_string(other).unwrap()],
    }
}

pub fn as_f64(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        // a list-valued property that slipped through without [0] in the Cypher
        Value::Array(arr) => arr.first().and_then(|el| as_f64(Some(el))),
        _ => None,
    }
}

pub fn as_text(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        Value::Array(arr) => arr.first().and_then(|el| as_text(Some(el))),
        other => Some(serde_json::to_string(other).unwrap().trim_matches('"').to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn columns_take_their_kind_from_the_column_type() {
        let cols = columns(&json!({"columns": [
            {"column_id": "gene", "column_type": "GraphNodeId", "id_prefixes": ["hgnc:", "ensembl:"]},
            {"column_id": "from", "column_type": "DatasourceList"},
            {"column_id": "p", "column_type": "float"},
            {"column_id": "n", "column_type": "int"},
            {"column_id": "edge_id", "column_type": "EdgeId"},
            {"column_id": "untyped"}
        ]}));
        let kinds: Vec<&str> = cols.iter().map(|c| match c.kind {
            ColKind::Node => "node", ColKind::TextArray => "array", ColKind::Float => "float",
            ColKind::Int => "int", ColKind::Text => "text",
        }).collect();
        assert_eq!(kinds, ["node", "array", "float", "int", "text", "text"]);
        assert_eq!(cols[4].column_type, "EdgeId");
        assert_eq!(cols[5].column_type, "string");
        assert_eq!(cols[0].id_prefixes, ["hgnc:", "ensembl:"]);
        assert!(cols[1].id_prefixes.is_empty());
    }

    #[test]
    fn a_node_is_identified_by_what_its_column_is_about_then_by_the_preferred_prefixes() {
        // type 2 diabetes as the graph has it: a disease and a phenotype in one node
        let node = json!({"id": ["type 2 diabetes mellitus", "hp:0005978", "mondo:0005148", "efo:0001360", "doid:9352"],
                          "grebi:nodeId": "g1:doid:9352", "grebi:name": ["type 2 diabetes mellitus"]});
        let prefixes = |list: &[&str]| list.iter().map(|p| p.to_string()).collect::<Vec<String>>();
        assert_eq!(node_identifier(Some(&node), &prefixes(&["mondo:"]), "g1:"), Some("mondo:0005148".to_string()));
        assert_eq!(node_identifier(Some(&node), &prefixes(&["orpha:", "efo:"]), "g1:"), Some("efo:0001360".to_string()),
            "the first prefix the node has an identifier for");
        assert_eq!(node_identifier(Some(&node), &[], "g1:"), Some("hp:0005978".to_string()),
            "hp: is preferred to mondo: when the column does not say");
        assert_eq!(node_identifier(Some(&node), &prefixes(&["hgnc:"]), "g1:"), Some("hp:0005978".to_string()),
            "none of what the column asks for: the preferred prefixes");
    }

    #[test]
    fn identifiers_with_a_name_attached_are_passed_over_and_the_node_id_is_the_last_resort() {
        let chemical = json!({"id": ["sphingosine", "chebi:16393 SPHINGOSINE", "mesh:D013110", "chebi:16393"],
                              "grebi:nodeId": "g1:inchikey:WWUZIQQURGPMPG-KRWOKUGFSA-N"});
        assert_eq!(node_identifier(Some(&chemical), &[], "g1:"), Some("chebi:16393".to_string()));
        let snp = json!({"id": ["rs429358"], "grebi:nodeId": "g1:rs429358"});
        assert_eq!(node_identifier(Some(&snp), &[], "g1:"), Some("rs429358".to_string()));
        let no_ids = json!({"grebi:nodeId": "g1:cl:0000084"});
        assert_eq!(node_identifier(Some(&no_ids), &["cl:".to_string()], "g1:"), Some("cl:0000084".to_string()));
        assert_eq!(node_identifier(Some(&Value::Null), &[], "g1:"), None);
        assert_eq!(node_identifier(None, &[], "g1:"), None);
    }

    #[test]
    fn a_node_is_known_by_its_bare_id_and_first_name() {
        let node = json!({"id": ["APP", "hgnc:620"], "grebi:nodeId": "g1:hgnc:620", "grebi:name": ["APP", "amyloid beta precursor protein"]});
        assert_eq!(node_id(Some(&node), "g1:"), Some("hgnc:620"));
        assert_eq!(node_id(Some(&node), "other:"), Some("g1:hgnc:620"));
        assert_eq!(node_name(Some(&node)), Some("APP".to_string()));
        assert_eq!(node_name(Some(&json!({"grebi:nodeId": "g1:rs1"}))), None);
        assert_eq!(node_id(Some(&Value::Null), "g1:"), None);
        assert_eq!(node_id(None, "g1:"), None);
        assert_eq!(node_id_prefix(&json!({"subgraph": "g1"})), "g1:");
        assert_eq!(node_id_prefix(&json!({})), "");
    }

    #[test]
    fn numbers_are_read_from_numbers_strings_and_the_first_of_a_list() {
        assert_eq!(as_f64(Some(&json!(0.5))), Some(0.5));
        assert_eq!(as_f64(Some(&json!("1E-7"))), Some(1e-7));
        assert_eq!(as_f64(Some(&json!(["2", "3"]))), Some(2.0));
        assert_eq!(as_f64(Some(&json!("NR"))), None);
        assert_eq!(as_f64(Some(&Value::Null)), None);
        assert_eq!(as_f64(None), None);
    }

    #[test]
    fn text_is_the_string_the_first_of_a_list_or_the_json_of_anything_else() {
        assert_eq!(as_text(Some(&json!("a"))), Some("a".to_string()));
        assert_eq!(as_text(Some(&json!(["a", "b"]))), Some("a".to_string()));
        assert_eq!(as_text(Some(&json!(12))), Some("12".to_string()));
        assert_eq!(as_text(Some(&json!(true))), Some("true".to_string()));
        assert_eq!(as_text(Some(&Value::Null)), None);
        assert_eq!(as_text(Some(&json!([]))), None);
        assert_eq!(flatten_to_strings(&json!(["a", ["b", null], 1])), ["a", "b", "1"]);
    }
}
