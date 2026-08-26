use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};

use clap::Parser;
use grebi_shared::pgcopy::PgCopyWriter;
use serde_json::Value;
use sha1::{Digest, Sha1};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Read a materialised query's linked_results JSONL from stdin and write its
/// typed per-query table as PGCOPY binary, plus DDL sidecars.
///
/// The query's metadata JSON (written by run_queries) names the storage table
/// and the logical columns; the physical schema is derived per column type:
///
///   GraphNodeId    -> "<col>_id" TEXT[]  (source ids; the closure filter target)
///                     "<col>_name" TEXT  (display name; facet/sort target)
///   DatasourceList -> "<col>" TEXT[]
///   float          -> "<col>" double precision
///   int            -> "<col>" bigint     (the counts_only _count histogram)
///   anything else  -> "<col>" TEXT       (string / EdgeId)
///
/// plus row_number INT and payload BYTEA — the row's exact linked JSON, served
/// verbatim so the API's response shape is independent of the typed projection.
///
/// Outputs (all named after the table, which the loader takes from the
/// filenames): {table}.pgbin, {table}.columns (CREATE TABLE column lines, like
/// the nodes/edges writers), {table}.indexes (full CREATE INDEX statements).
#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    in_metadata_json: String,
}

const PG_MAX_IDENTIFIER: usize = 63;

/// Mirrors grebi_materialise.pg_identifier: keep the readable head, append a
/// short hash of the full name only when it would overflow.
fn pg_identifier(name: &str) -> String {
    if name.len() <= PG_MAX_IDENTIFIER {
        return name.to_string();
    }
    let mut hasher = Sha1::new();
    hasher.update(name.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("{}_{}", &name[..PG_MAX_IDENTIFIER - 9], &digest[..8])
}

enum ColKind {
    Node,       // -> _id TEXT[] + _name TEXT
    TextArray,  // -> TEXT[]
    Float,      // -> double precision
    Int,        // -> bigint
    Text,       // -> TEXT
}

struct Col {
    id: String,
    kind: ColKind,
}

fn main() {
    let args = Args::parse();

    let metadata: Value = serde_json::from_reader(std::io::BufReader::new(
        File::open(&args.in_metadata_json).expect("Failed to open metadata json"),
    ))
    .expect("Failed to parse metadata json");

    let table = metadata
        .get("table")
        .and_then(|v| v.as_str())
        .expect("metadata json has no `table`")
        .to_string();

    let cols: Vec<Col> = metadata
        .get("columns")
        .and_then(|v| v.as_array())
        .expect("metadata json has no `columns`")
        .iter()
        .map(|c| {
            let id = c.get("column_id").and_then(|v| v.as_str()).expect("column_id").to_string();
            let ctype = c.get("column_type").and_then(|v| v.as_str()).unwrap_or("string");
            let kind = match ctype {
                "GraphNodeId" => ColKind::Node,
                "DatasourceList" => ColKind::TextArray,
                "float" => ColKind::Float,
                "int" | "integer" => ColKind::Int,
                _ => ColKind::Text,
            };
            Col { id, kind }
        })
        .collect();

    // The GIN targets: each closure param's filter column (parameterised
    // templates only; standalone tables have no closure filter).
    let filter_columns: Vec<String> = metadata
        .get("params")
        .and_then(|v| v.as_array())
        .map(|params| {
            params
                .iter()
                .filter_map(|p| p.get("filters_column").and_then(|v| v.as_str()))
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    write_columns_sidecar(&table, &cols);
    write_indexes_sidecar(&table, &filter_columns);

    let out_file = File::create(format!("{}.pgbin", table)).unwrap();
    let mut pgw = PgCopyWriter::new(BufWriter::with_capacity(1024 * 1024 * 32, out_file));

    // row_number + typed columns (Node counts double) + payload
    let nfields: i16 = (2 + cols
        .iter()
        .map(|c| if matches!(c.kind, ColKind::Node) { 2 } else { 1 })
        .sum::<usize>()) as i16;

    let stdin = io::stdin().lock();
    let mut row_number: i32 = 0;
    for line in io::BufReader::new(stdin).lines() {
        let line = line.unwrap();
        if line.is_empty() {
            continue;
        }
        let json: serde_json::Map<String, Value> = serde_json::from_str(&line).unwrap();
        row_number += 1;

        pgw.begin_row(nfields);
        pgw.write_int32(row_number);
        for col in &cols {
            let v = json.get(&col.id);
            match col.kind {
                ColKind::Node => {
                    let ids = v
                        .and_then(|v| v.get("id"))
                        .map(flatten_to_strings)
                        .unwrap_or_default();
                    let refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
                    pgw.write_text_array(&refs);
                    match v.and_then(|v| v.get("grebi:name")).map(flatten_to_strings) {
                        Some(names) if !names.is_empty() => pgw.write_text(&names[0]),
                        _ => pgw.write_null(),
                    }
                }
                ColKind::TextArray => {
                    let vals = v.map(flatten_to_strings).unwrap_or_default();
                    let refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
                    pgw.write_text_array(&refs);
                }
                ColKind::Float => match as_f64(v) {
                    Some(f) => pgw.write_float64(f),
                    None => pgw.write_null(),
                },
                ColKind::Int => match as_f64(v) {
                    Some(f) => pgw.write_int64(f as i64),
                    None => pgw.write_null(),
                },
                ColKind::Text => match as_text(v) {
                    Some(s) => pgw.write_text(&s),
                    None => pgw.write_null(),
                },
            }
        }
        pgw.write_bytes(line.as_bytes());
    }

    let n = pgw.finish();
    eprintln!("grebi_make_postgres_mat_queries ({}): wrote {} rows", table, n);
}

fn write_columns_sidecar(table: &str, cols: &[Col]) {
    let f = File::create(format!("{}.columns", table)).unwrap();
    let mut w = BufWriter::new(f);
    writeln!(w, "row_number INT NOT NULL").unwrap();
    for col in cols {
        match col.kind {
            ColKind::Node => {
                writeln!(w, "\"{}_id\" TEXT[] NOT NULL DEFAULT '{{}}'", col.id).unwrap();
                writeln!(w, "\"{}_name\" TEXT", col.id).unwrap();
            }
            ColKind::TextArray => {
                writeln!(w, "\"{}\" TEXT[] NOT NULL DEFAULT '{{}}'", col.id).unwrap()
            }
            ColKind::Float => writeln!(w, "\"{}\" double precision", col.id).unwrap(),
            ColKind::Int => writeln!(w, "\"{}\" bigint", col.id).unwrap(),
            ColKind::Text => writeln!(w, "\"{}\" TEXT", col.id).unwrap(),
        }
    }
    writeln!(w, "payload BYTEA NOT NULL").unwrap();
    w.flush().unwrap();
}

fn write_indexes_sidecar(table: &str, filter_columns: &[String]) {
    let f = File::create(format!("{}.indexes", table)).unwrap();
    let mut w = BufWriter::new(f);
    writeln!(
        w,
        "CREATE INDEX \"{}\" ON \"{}\" USING btree (row_number);",
        pg_identifier(&format!("idx_{}_row", table)),
        table
    )
    .unwrap();
    for fc in filter_columns {
        writeln!(
            w,
            "CREATE INDEX \"{}\" ON \"{}\" USING gin (\"{}_id\");",
            pg_identifier(&format!("idx_{}_{}_id_gin", table, fc)),
            table,
            fc
        )
        .unwrap();
    }
    w.flush().unwrap();
}

fn flatten_to_strings(v: &Value) -> Vec<String> {
    match v {
        Value::String(s) => vec![s.clone()],
        Value::Array(arr) => arr.iter().flat_map(flatten_to_strings).collect(),
        Value::Null => vec![],
        other => vec![serde_json::to_string(other).unwrap()],
    }
}

fn as_f64(v: Option<&Value>) -> Option<f64> {
    match v? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        // a list-valued property that slipped through without [0] in the Cypher
        Value::Array(arr) => arr.first().and_then(|el| as_f64(Some(el))),
        _ => None,
    }
}

fn as_text(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        Value::Array(arr) => arr.first().and_then(|el| as_text(Some(el))),
        other => Some(serde_json::to_string(other).unwrap().trim_matches('"').to_string()),
    }
}
