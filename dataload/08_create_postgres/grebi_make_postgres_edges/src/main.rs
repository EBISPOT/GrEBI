use std::collections::BTreeSet;
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
    in_edges_jsonl: String,

    #[arg(long)]
    in_graph_metadata_json: String,

    #[arg(long)]
    out_edges_pgbin_path: String,

    #[arg(long)]
    out_columns_path: String,
}

/// Fixed columns that are always present and don't need to be discovered.
const FIXED_PROPS: &[&str] = &[
    "_json",
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

    let extra_props = read_extra_props_from_metadata(&args.in_graph_metadata_json);
    eprintln!("Loaded {} extra edge property names from graph metadata", extra_props.len());

    let cols_file = File::create(&args.out_columns_path)?;
    let mut cols_writer = BufWriter::new(cols_file);
    write_columns(&extra_props, &mut cols_writer);
    cols_writer.flush()?;

    let edges_reader = std::io::BufReader::new(File::open(&args.in_edges_jsonl)?);
    let edges_file = File::create(&args.out_edges_pgbin_path)?;
    let mut pgw = PgCopyWriter::new(BufWriter::with_capacity(1024 * 1024 * 32, edges_file));

    // 8 fixed columns + extra_props
    let nfields = (8 + extra_props.len()) as i16;

    for line_result in edges_reader.lines() {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }

        let json: serde_json::Map<String, Value> = serde_json::from_str(&line).unwrap();
        write_edge_row(&json, &extra_props, nfields, &mut pgw);
    }

    let n = pgw.finish();
    eprintln!(
        "grebi_make_postgres_edges took {} seconds ({} edges)",
        start_time.elapsed().as_secs(),
        n
    );

    Ok(())
}

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

    props.into_iter().collect()
}

fn write_edge_row(
    json: &serde_json::Map<String, Value>,
    extra_props: &[String],
    nfields: i16,
    pgw: &mut PgCopyWriter<BufWriter<File>>,
) {
    pgw.begin_row(nfields);

    // grebi:edgeId TEXT
    pgw.write_text(json.get("grebi:edgeId").and_then(|v| v.as_str()).unwrap_or(""));

    // grebi:type TEXT
    pgw.write_text(&extract_type_text(json.get("grebi:type")));

    // grebi:fromNodeId TEXT
    pgw.write_text(json.get("grebi:fromNodeId").and_then(|v| v.as_str()).unwrap_or(""));

    // grebi:toNodeId TEXT
    pgw.write_text(json.get("grebi:toNodeId").and_then(|v| v.as_str()).unwrap_or(""));

    // grebi:datasources TEXT[]
    write_json_as_text_array(json.get("grebi:datasources"), pgw);

    // grebi:subgraph TEXT
    pgw.write_text(json.get("grebi:subgraph").and_then(|v| v.as_str()).unwrap_or(""));

    // _refs TEXT (JSON stored as text)
    let refs_json = json
        .get("_refs")
        .map(|v| serde_json::to_string(v).unwrap())
        .unwrap_or_else(|| "null".to_string());
    pgw.write_text(&refs_json);

    // grebi:fromSourceIds TEXT[]
    write_json_as_text_array(json.get("grebi:fromSourceIds"), pgw);

    // Extra property columns (all TEXT[])
    for prop in extra_props {
        match json.get(prop) {
            Some(v) => {
                let strings = flatten_property_values_to_strings(v);
                let refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
                pgw.write_text_array(&refs);
            }
            None => pgw.write_null(),
        }
    }
}

fn extract_type_text(v: Option<&Value>) -> String {
    match v {
        None => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) if arr.len() == 1 => extract_type_text(Some(&arr[0])),
        Some(Value::Array(arr)) => arr.iter().filter_map(|el| el.as_str()).collect::<Vec<&str>>().join(","),
        Some(other) => serde_json::to_string(other).unwrap(),
    }
}

fn write_json_as_text_array(v: Option<&Value>, pgw: &mut PgCopyWriter<BufWriter<File>>) {
    match v {
        Some(Value::Array(arr)) => {
            let strings: Vec<String> = arr.iter().map(|el| match el {
                Value::String(s) => s.clone(),
                _ => serde_json::to_string(el).unwrap(),
            }).collect();
            let refs: Vec<&str> = strings.iter().map(|s| s.as_str()).collect();
            pgw.write_text_array(&refs);
        }
        Some(Value::String(s)) => pgw.write_text_array(&[s.as_str()]),
        Some(other) => pgw.write_text_array(&[&serde_json::to_string(other).unwrap()]),
        None => pgw.write_text_array(&[]),
    }
}

fn flatten_property_values_to_strings(v: &Value) -> Vec<String> {
    match v {
        Value::Array(arr) => arr.iter().flat_map(flatten_property_values_to_strings).collect(),
        Value::Object(obj) => {
            if let Some(inner) = obj.get("grebi:value") {
                flatten_property_values_to_strings(inner)
            } else {
                vec![]
            }
        }
        Value::String(s) => vec![s.clone()],
        _ => vec![serde_json::to_string(v).unwrap()],
    }
}

fn write_columns(extra_props: &[String], writer: &mut BufWriter<File>) {
    writeln!(writer, "\"grebi:edgeId\" TEXT").unwrap();
    writeln!(writer, "\"grebi:type\" TEXT NOT NULL").unwrap();
    writeln!(writer, "\"grebi:fromNodeId\" TEXT NOT NULL").unwrap();
    writeln!(writer, "\"grebi:toNodeId\" TEXT NOT NULL").unwrap();
    writeln!(writer, "\"grebi:datasources\" TEXT[] NOT NULL DEFAULT '{{}}'").unwrap();
    writeln!(writer, "\"grebi:subgraph\" TEXT").unwrap();
    writeln!(writer, "\"_refs\" TEXT").unwrap();
    writeln!(writer, "\"grebi:fromSourceIds\" TEXT[] DEFAULT '{{}}'").unwrap();
    for prop in extra_props {
        writeln!(writer, "\"{}\" TEXT[]", prop).unwrap();
    }
}
