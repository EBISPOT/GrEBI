//! Biomappings' mappings.tsv (named by GREBI_INGEST_FILENAME) as GrEBI JSONL:
//! each row is a subject node with the relation as a property pointing at the
//! object; identifiers without a prefix get the row's prefix.

use serde_json::{json, Value};
use std::io::{self, BufWriter, Write};

fn curie(prefix: &str, identifier: &str) -> String {
    if identifier.contains(':') { identifier.to_string() } else { format!("{}:{}", prefix, identifier) }
}

fn main() {
    let filename = std::env::var("GREBI_INGEST_FILENAME").expect("GREBI_INGEST_FILENAME names the mappings file");
    let mut reader = csv::ReaderBuilder::new().delimiter(b'\t').flexible(true)
        .from_path(&filename).unwrap_or_else(|e| panic!("{}: {}", filename, e));
    let headers = reader.headers().unwrap().clone();
    let column = |name: &str| headers.iter().position(|h| h == name).unwrap_or_else(|| panic!("no {} column in {}", name, filename));
    let (sp, si, rel, tp, ti) = (column("source prefix"), column("source identifier"), column("relation"), column("target prefix"), column("target identifier"));
    let mut out = BufWriter::new(io::stdout().lock());
    for record in reader.records() {
        let record = record.unwrap();
        let field = |i: usize| record.get(i).unwrap_or("");
        let mut node = serde_json::Map::new();
        node.insert("id".into(), Value::String(curie(field(sp), field(si))));
        node.insert(field(rel).to_string(), Value::String(curie(field(tp), field(ti))));
        serde_json::to_writer(&mut out, &json!(node)).unwrap();
        out.write_all(b"\n").unwrap();
    }
    out.flush().unwrap();
}
