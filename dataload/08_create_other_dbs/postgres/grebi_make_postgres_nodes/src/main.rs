use std::collections::BTreeMap;
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
    in_nodes_jsonl: String,

    #[arg(long)]
    in_graph_metadata_json: String,

    #[arg(long)]
    out_nodes_tsv_path: String,

    #[arg(long)]
    out_schema_sql_path: String,

    #[arg(long)]
    table_name: String,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let start_time = std::time::Instant::now();

    // Read embedding model dimensions from graph_metadata.json
    let embedding_models = read_embedding_models(&args.in_graph_metadata_json);
    eprintln!(
        "Loaded {} embedding models from graph metadata",
        embedding_models.len()
    );

    // Write schema SQL
    let schema_file = File::create(&args.out_schema_sql_path)?;
    let mut schema_writer = BufWriter::new(schema_file);
    write_schema_sql(&args.table_name, &embedding_models, &mut schema_writer);
    schema_writer.flush()?;

    // Stream nodes JSONL → TSV
    let nodes_reader = std::io::BufReader::new(File::open(&args.in_nodes_jsonl)?);
    let nodes_file = File::create(&args.out_nodes_tsv_path)?;
    let mut nodes_writer = BufWriter::with_capacity(1024 * 1024 * 32, nodes_file);

    let mut n_nodes: i64 = 0;

    for line_result in nodes_reader.lines() {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }

        let json: serde_json::Map<String, Value> = serde_json::from_str(&line).unwrap();
        write_node_tsv_row(&json, &embedding_models, &mut nodes_writer);
        n_nodes += 1;
    }

    nodes_writer.flush()?;

    eprintln!(
        "grebi_make_postgres_nodes took {} seconds ({} nodes)",
        start_time.elapsed().as_secs(),
        n_nodes
    );

    Ok(())
}

/// Read embedding model name → dimension from graph_metadata.json.
/// The link step writes `"embedding_models2dims": { "model_name": dim, ... }`.
fn read_embedding_models(path: &str) -> BTreeMap<String, i64> {
    let file = File::open(path).expect("Failed to open graph_metadata.json");
    let metadata: Value =
        serde_json::from_reader(std::io::BufReader::new(file)).expect("Failed to parse graph_metadata.json");

    let mut models = BTreeMap::new();

    if let Some(m2d) = metadata.get("embedding_models2dims").and_then(|v| v.as_object()) {
        for (model_name, dim) in m2d {
            let d = dim.as_i64()
                .or_else(|| dim.as_str().and_then(|s| s.parse::<i64>().ok()));
            if let Some(d) = d {
                models.insert(model_name.clone(), d);
            }
        }
    }

    models
}

fn write_node_tsv_row(
    json: &serde_json::Map<String, Value>,
    embedding_models: &BTreeMap<String, i64>,
    writer: &mut BufWriter<File>,
) {
    // Column order:
    // grebi:nodeId, grebi:name, grebi:type, grebi:datasources, grebi:sourceIds,
    // ols:curie, ...embedding columns...

    let node_id = json
        .get("grebi:nodeId")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let name = extract_first_string(json.get("grebi:name"));
    let types = json
        .get("grebi:type")
        .map(|v| value_to_pg_array(v))
        .unwrap_or_else(|| "{}".to_string());
    let datasources = json
        .get("grebi:datasources")
        .map(|v| value_to_pg_array(v))
        .unwrap_or_else(|| "{}".to_string());
    let source_ids = json
        .get("grebi:sourceIds")
        .map(|v| value_to_pg_array(v))
        .unwrap_or_else(|| "{}".to_string());
    let ols_curie = extract_first_string(json.get("ols:curie"));

    // Write ref columns
    write!(
        writer,
        "{}\t{}\t{}\t{}\t{}\t{}",
        escape_tsv(node_id),
        escape_tsv(&name),
        escape_tsv(&types),
        escape_tsv(&datasources),
        escape_tsv(&source_ids),
        escape_tsv(&ols_curie),
    )
    .unwrap();

    // Write embedding columns
    for model_name in embedding_models.keys() {
        let key = format!("embedding:{}", model_name);
        write!(writer, "\t").unwrap();
        match json.get(&key) {
            Some(Value::Array(arr)) => {
                // Write as pgvector literal: [0.1,0.2,...]
                write!(writer, "[").unwrap();
                for (i, el) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(writer, ",").unwrap();
                    }
                    match el.as_f64() {
                        Some(f) => write!(writer, "{}", f).unwrap(),
                        None => write!(writer, "0").unwrap(),
                    }
                }
                write!(writer, "]").unwrap();
            }
            _ => {
                write!(writer, "\\N").unwrap(); // NULL
            }
        }
    }

    write!(writer, "\n").unwrap();
}

/// Extract the first string value from a property (which may be a plain string,
/// an array of strings, or an array of reified objects with grebi:value).
fn extract_first_string(v: Option<&Value>) -> String {
    match v {
        None => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) => {
            for el in arr {
                match el {
                    Value::String(s) => return s.clone(),
                    Value::Object(obj) => {
                        if let Some(Value::String(s)) = obj.get("grebi:value") {
                            return s.clone();
                        }
                    }
                    _ => {}
                }
            }
            String::new()
        }
        _ => String::new(),
    }
}

/// Convert a JSON array value to a PostgreSQL array literal: {val1,val2,...}
fn value_to_pg_array(v: &Value) -> String {
    match v {
        Value::Array(arr) => {
            let elements: Vec<String> = arr
                .iter()
                .flat_map(|el| flatten_to_strings(el))
                .map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
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

/// Flatten a value to its string representations (unwrapping reified objects)
fn flatten_to_strings(v: &Value) -> Vec<String> {
    match v {
        Value::String(s) => vec![s.clone()],
        Value::Object(obj) => {
            if let Some(inner) = obj.get("grebi:value") {
                flatten_to_strings(inner)
            } else {
                vec![]
            }
        }
        Value::Array(arr) => arr.iter().flat_map(|el| flatten_to_strings(el)).collect(),
        _ => vec![serde_json::to_string(v).unwrap()],
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
    embedding_models: &BTreeMap<String, i64>,
    writer: &mut BufWriter<File>,
) {
    writeln!(writer, "-- Auto-generated schema for GrEBI PostgreSQL nodes table").unwrap();
    writeln!(writer, "-- Table: {}", table_name).unwrap();
    writeln!(writer).unwrap();
    writeln!(writer, "CREATE EXTENSION IF NOT EXISTS vector;").unwrap();
    writeln!(writer).unwrap();
    writeln!(writer, "DROP TABLE IF EXISTS \"{}\" CASCADE;", table_name).unwrap();
    writeln!(writer).unwrap();
    writeln!(writer, "CREATE TABLE \"{}\" (", table_name).unwrap();
    writeln!(writer, "    \"grebi:nodeId\" TEXT PRIMARY KEY,").unwrap();
    writeln!(writer, "    \"grebi:name\" TEXT,").unwrap();
    writeln!(writer, "    \"grebi:type\" TEXT[] NOT NULL DEFAULT '{{}}',").unwrap();
    writeln!(writer, "    \"grebi:datasources\" TEXT[] NOT NULL DEFAULT '{{}}',").unwrap();
    writeln!(writer, "    \"grebi:sourceIds\" TEXT[] DEFAULT '{{}}',").unwrap();

    if embedding_models.is_empty() {
        writeln!(writer, "    \"ols:curie\" TEXT").unwrap();
    } else {
        writeln!(writer, "    \"ols:curie\" TEXT,").unwrap();
        let last_model = embedding_models.keys().last().unwrap();
        for (model_name, dim) in embedding_models {
            if model_name == last_model {
                writeln!(writer, "    \"embedding:{}\" vector({})", model_name, dim).unwrap();
            } else {
                writeln!(writer, "    \"embedding:{}\" vector({}),", model_name, dim).unwrap();
            }
        }
    }

    writeln!(writer, ");").unwrap();
    writeln!(writer).unwrap();

    // Create HNSW indexes for each embedding column
    let safe_name = table_name.replace('"', "");
    for model_name in embedding_models.keys() {
        let safe_model = model_name.replace('-', "_").replace('.', "_");
        writeln!(
            writer,
            "CREATE INDEX \"idx_{}_embedding_{}\" ON \"{}\" USING hnsw (\"embedding:{}\" vector_cosine_ops);",
            safe_name, safe_model, table_name, model_name
        )
        .unwrap();
    }

    writeln!(writer).unwrap();
    // Text indexes for common lookups
    writeln!(
        writer,
        "CREATE INDEX \"idx_{}_name\" ON \"{}\" (\"grebi:name\");",
        safe_name, table_name
    )
    .unwrap();
}
