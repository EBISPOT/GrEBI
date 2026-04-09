use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::BufRead;
use std::io::Write;

use clap::Parser;
use grebi_shared::pgcopy::PgCopyWriter;
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
    out_nodes_pgbin_path: String,

    #[arg(long)]
    out_columns_path: String,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let start_time = std::time::Instant::now();

    let embedding_models = read_embedding_models(&args.in_graph_metadata_json);
    eprintln!(
        "Loaded {} embedding models from graph metadata",
        embedding_models.len()
    );

    let cols_file = File::create(&args.out_columns_path)?;
    let mut cols_writer = BufWriter::new(cols_file);
    write_columns(&embedding_models, &mut cols_writer);
    cols_writer.flush()?;

    let nodes_reader = std::io::BufReader::new(File::open(&args.in_nodes_jsonl)?);
    let nodes_file = File::create(&args.out_nodes_pgbin_path)?;
    let mut pgw = PgCopyWriter::new(BufWriter::with_capacity(1024 * 1024 * 32, nodes_file));

    // 6 fixed columns + embedding columns
    let nfields = (6 + embedding_models.len()) as i16;

    for line_result in nodes_reader.lines() {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }

        let json: serde_json::Map<String, Value> = serde_json::from_str(&line).unwrap();
        write_node_row(&json, &embedding_models, nfields, &mut pgw);
    }

    let n = pgw.finish();
    eprintln!(
        "grebi_make_postgres_nodes took {} seconds ({} nodes)",
        start_time.elapsed().as_secs(),
        n
    );

    Ok(())
}

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

fn write_node_row(
    json: &serde_json::Map<String, Value>,
    embedding_models: &BTreeMap<String, i64>,
    nfields: i16,
    pgw: &mut PgCopyWriter<BufWriter<File>>,
) {
    pgw.begin_row(nfields);

    // grebi:nodeId TEXT
    pgw.write_text(json.get("grebi:nodeId").and_then(|v| v.as_str()).unwrap_or(""));

    // grebi:name TEXT
    pgw.write_text(&extract_first_string(json.get("grebi:name")));

    // grebi:type TEXT[]
    write_json_as_text_array(json.get("grebi:type"), pgw);

    // grebi:datasources TEXT[]
    write_json_as_text_array(json.get("grebi:datasources"), pgw);

    // grebi:sourceIds TEXT[]
    write_json_as_text_array(json.get("grebi:sourceIds"), pgw);

    // ols:curie TEXT
    pgw.write_text(&extract_first_string(json.get("ols:curie")));

    // Embedding columns (vector(dim))
    for model_name in embedding_models.keys() {
        let key = format!("embedding:{}", model_name);
        match json.get(&key) {
            Some(Value::Array(arr)) => {
                let floats: Vec<f32> = arr.iter().map(|el| el.as_f64().unwrap_or(0.0) as f32).collect();
                pgw.write_vector_f32(&floats);
            }
            _ => pgw.write_null(),
        }
    }
}

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

fn write_json_as_text_array(v: Option<&Value>, pgw: &mut PgCopyWriter<BufWriter<File>>) {
    match v {
        Some(Value::Array(arr)) => {
            let strings: Vec<String> = arr.iter().flat_map(flatten_to_strings).collect();
            let refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
            pgw.write_text_array(&refs);
        }
        Some(Value::String(s)) => pgw.write_text_array(&[s.as_str()]),
        Some(other) => pgw.write_text_array(&[&serde_json::to_string(other).unwrap()]),
        None => pgw.write_text_array(&[]),
    }
}

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
        Value::Array(arr) => arr.iter().flat_map(flatten_to_strings).collect(),
        _ => vec![serde_json::to_string(v).unwrap()],
    }
}

fn write_columns(
    embedding_models: &BTreeMap<String, i64>,
    writer: &mut BufWriter<File>,
) {
    writeln!(writer, "\"grebi:nodeId\" TEXT").unwrap();
    writeln!(writer, "\"grebi:name\" TEXT").unwrap();
    writeln!(writer, "\"grebi:type\" TEXT[] NOT NULL DEFAULT '{{}}'").unwrap();
    writeln!(writer, "\"grebi:datasources\" TEXT[] NOT NULL DEFAULT '{{}}'").unwrap();
    writeln!(writer, "\"grebi:sourceIds\" TEXT[] DEFAULT '{{}}'").unwrap();
    writeln!(writer, "\"ols:curie\" TEXT").unwrap();
    for (model_name, dim) in embedding_models {
        writeln!(writer, "\"embedding:{}\" vector({})", model_name, dim).unwrap();
    }
}
