use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::BufRead;
use std::io::Write;

use clap::Parser;
use serde_json::Value;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    in_edges_jsonl: String,

    #[arg(long)]
    in_graph_metadata_json: String,

    #[arg(long)]
    out_edges_tsv_path: String,

    #[arg(long)]
    out_schema_sql_path: String,

    #[arg(long)]
    table_name: String,
}

/// Fixed columns that are always present and don't need to be discovered.
const FIXED_PROPS: &[&str] = &[
    "_refs",
    "grebi:edgeId",
    "grebi:fromNodeId",
    "grebi:toNodeId",
    "grebi:type",
    "grebi:datasources",
    "grebi:subgraph",
    "grebi:fromSourceIds",
];

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let start_time = std::time::Instant::now();

    // Read extra property names from graph_metadata.json (built by 04_index)
    let extra_props = read_extra_props_from_metadata(&args.in_graph_metadata_json);
    eprintln!("Loaded {} extra edge property names from graph metadata", extra_props.len());

    // Write schema SQL up-front (we know all columns now)
    let schema_file = File::create(&args.out_schema_sql_path).unwrap();
    let mut schema_writer = BufWriter::new(schema_file);
    write_schema_sql(&args.table_name, &extra_props, &mut schema_writer);
    schema_writer.flush().unwrap();

    // Single pass: stream edges JSONL → TSV
    let edges_reader = std::io::BufReader::new(File::open(&args.in_edges_jsonl).unwrap());
    let edges_file = File::create(&args.out_edges_tsv_path).unwrap();
    let mut edges_writer = BufWriter::with_capacity(1024 * 1024 * 32, edges_file);

    let mut n_edges: i64 = 0;

    for line_result in edges_reader.lines() {
        let line = line_result.unwrap();
        if line.is_empty() {
            continue;
        }

        let json: serde_json::Map<String, Value> = serde_json::from_str(&line).unwrap();
        write_edge_tsv_row(&json, &extra_props, &mut edges_writer);
        n_edges += 1;
    }

    edges_writer.flush().unwrap();

    eprintln!(
        "grebi_make_postgres took {} seconds ({} edges)",
        start_time.elapsed().as_secs(),
        n_edges
    );

    Ok(())
}

/// Read the set of extra edge property names from graph_metadata.json.
/// The index step (04_index) writes `"edge_props": { "prop_name": { "count": N }, ... }`.
/// We extract those keys, exclude the fixed columns, and return them sorted.
fn read_extra_props_from_metadata(path: &str) -> Vec<String> {
    let file = File::open(path).expect("Failed to open graph_metadata.json");
    let metadata: Value = serde_json::from_reader(std::io::BufReader::new(file))
        .expect("Failed to parse graph_metadata.json");

    let mut props = BTreeSet::new();

    if let Some(edge_props) = metadata.get("edge_props").and_then(|v| v.as_object()) {
        for key in edge_props.keys() {
            if !FIXED_PROPS.contains(&key.as_str()) {
                props.insert(key.clone());
            }
        }
    }

    // Also include entity_props that appear on edges (edge JSON contains the
    // full merged line which may have entity-level props too). The edge_props
    // from the index are specifically the *reified* property keys. Entity-level
    // props that appear in edge JSONL (like grebi:type etc.) are already
    // handled as fixed columns, so edge_props is sufficient here.

    props.into_iter().collect()
}

fn write_edge_tsv_row(
    json: &serde_json::Map<String, Value>,
    extra_props: &[String],
    writer: &mut BufWriter<File>,
) {
    // Column order:
    // grebi:edgeId, grebi:type, grebi:fromNodeId, grebi:toNodeId,
    // grebi:datasources, grebi:subgraph, grebi:fromSourceIds,
    // ...extra_props..., _json

    let edge_id = json
        .get("grebi:edgeId")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let edge_type = json.get("grebi:type").map(|v| value_to_pg_text(v)).unwrap_or_default();

    let from_node_id = json
        .get("grebi:fromNodeId")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let to_node_id = json
        .get("grebi:toNodeId")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let datasources = json
        .get("grebi:datasources")
        .map(|v| value_to_pg_array(v))
        .unwrap_or_else(|| "{}".to_string());

    let subgraph = json
        .get("grebi:subgraph")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let from_source_ids = json
        .get("grebi:fromSourceIds")
        .map(|v| value_to_pg_array(v))
        .unwrap_or_else(|| "{}".to_string());

    // Build the _json column: the full JSON object minus _refs
    let json_blob = {
        let mut clean = json.clone();
        clean.remove("_refs");
        serde_json::to_string(&clean).unwrap()
    };

    // Write fixed columns
    write!(
        writer,
        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
        escape_tsv(edge_id),
        escape_tsv(&edge_type),
        escape_tsv(from_node_id),
        escape_tsv(to_node_id),
        escape_tsv(&datasources),
        escape_tsv(subgraph),
        escape_tsv(&from_source_ids),
    )
    .unwrap();

    // Write extra property columns
    for prop in extra_props {
        let val = json.get(prop);
        write!(writer, "\t").unwrap();
        match val {
            Some(v) => {
                let flattened = flatten_property_values(v);
                write!(writer, "{}", escape_tsv(&value_to_pg_array(&Value::Array(flattened)))).unwrap();
            }
            None => {
                write!(writer, "\\N").unwrap(); // NULL
            }
        }
    }

    // Write _json column
    write!(writer, "\t{}\n", escape_tsv(&json_blob)).unwrap();
}

/// Flatten reified values: extract grebi:value from objects, flatten nested arrays
fn flatten_property_values(v: &Value) -> Vec<Value> {
    match v {
        Value::Array(arr) => arr.iter().flat_map(|el| flatten_property_values(el)).collect(),
        Value::Object(obj) => {
            if let Some(inner) = obj.get("grebi:value") {
                flatten_property_values(inner)
            } else {
                vec![]
            }
        }
        _ => vec![v.clone()],
    }
}

/// Convert a JSON value to a PostgreSQL text representation.
/// For singular string/type values, return the string.
/// For arrays, return as pg array literal.
fn value_to_pg_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Array(arr) => {
            // For grebi:type which is a singular string for edges, but could be array
            if arr.len() == 1 {
                value_to_pg_text(&arr[0])
            } else {
                // Join with first value (edge type is always singular really)
                arr.iter()
                    .filter_map(|el| el.as_str())
                    .collect::<Vec<&str>>()
                    .join(",")
            }
        }
        _ => serde_json::to_string(v).unwrap(),
    }
}

/// Convert a JSON array value to a PostgreSQL array literal: {val1,val2,...}
fn value_to_pg_array(v: &Value) -> String {
    match v {
        Value::Array(arr) => {
            let elements: Vec<String> = arr
                .iter()
                .map(|el| match el {
                    Value::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
                    _ => {
                        let s = serde_json::to_string(el).unwrap();
                        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
                    }
                })
                .collect();
            format!("{{{}}}", elements.join(","))
        }
        Value::String(s) => format!("{{\"{}\"}}", s.replace('\\', "\\\\").replace('"', "\\\"")),
        _ => {
            let s = serde_json::to_string(v).unwrap();
            format!("{{\"{}\"}}", s.replace('\\', "\\\\").replace('"', "\\\""))
        }
    }
}

/// Escape a value for PostgreSQL COPY TSV format
fn escape_tsv(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn write_schema_sql(
    table_name: &str,
    extra_props: &[String],
    writer: &mut BufWriter<File>,
) {
    writeln!(writer, "-- Auto-generated schema for GrEBI PostgreSQL edges table").unwrap();
    writeln!(writer, "-- Table: {}", table_name).unwrap();
    writeln!(writer).unwrap();
    writeln!(writer, "DROP TABLE IF EXISTS \"{}\" CASCADE;", table_name).unwrap();
    writeln!(writer).unwrap();
    writeln!(writer, "CREATE TABLE \"{}\" (", table_name).unwrap();
    writeln!(writer, "    \"grebi:edgeId\" TEXT PRIMARY KEY,").unwrap();
    writeln!(writer, "    \"grebi:type\" TEXT NOT NULL,").unwrap();
    writeln!(writer, "    \"grebi:fromNodeId\" TEXT NOT NULL,").unwrap();
    writeln!(writer, "    \"grebi:toNodeId\" TEXT NOT NULL,").unwrap();
    writeln!(writer, "    \"grebi:datasources\" TEXT[] NOT NULL DEFAULT '{{}}',").unwrap();
    writeln!(writer, "    \"grebi:subgraph\" TEXT,").unwrap();
    writeln!(writer, "    \"grebi:fromSourceIds\" TEXT[] DEFAULT '{{}}',").unwrap();

    for prop in extra_props {
        writeln!(writer, "    \"{}\" TEXT[],", prop).unwrap();
    }

    writeln!(writer, "    \"_json\" JSONB").unwrap();
    writeln!(writer, ");").unwrap();
    writeln!(writer).unwrap();

    // Create indexes for the most common query patterns
    let safe_name = table_name.replace('"', "");
    writeln!(
        writer,
        "CREATE INDEX \"idx_{}_fromNodeId\" ON \"{}\" (\"grebi:fromNodeId\");",
        safe_name, table_name
    )
    .unwrap();
    writeln!(
        writer,
        "CREATE INDEX \"idx_{}_toNodeId\" ON \"{}\" (\"grebi:toNodeId\");",
        safe_name, table_name
    )
    .unwrap();
    writeln!(
        writer,
        "CREATE INDEX \"idx_{}_type\" ON \"{}\" (\"grebi:type\");",
        safe_name, table_name
    )
    .unwrap();

    writeln!(writer).unwrap();
    writeln!(writer, "-- Import data with:").unwrap();
    writeln!(
        writer,
        "-- COPY \"{}\" FROM 'edges.tsv' WITH (FORMAT text);",
        table_name
    )
    .unwrap();
}
