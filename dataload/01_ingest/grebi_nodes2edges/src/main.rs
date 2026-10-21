//! Re-shapes JSON objects that represent edges into what GrEBI recognises as
//! edges: each line's `--from-field` value becomes the node id and its
//! `--to-field` value the reified value of a property named after the edge
//! type, everything else on the line travelling as the edge's properties. The
//! type is `--edge-type` for every line, or, for a long table with a predicate
//! column, the line's own `--edge-type-field` value (not carried as metadata),
//! behind `--edge-type-prefix`. A line without the from, to or type field is
//! skipped and counted on stderr: a table's unmapped rows, say.

use std::io::{BufWriter, self, BufReader, Write,BufRead};
use std::vec;
use clap::Parser;
use serde_json::Value;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    from_field:String,

    #[arg(long)]
    to_field:String,

    /// The type of every edge
    #[arg(long, required_unless_present = "edge_type_field", conflicts_with = "edge_type_field")]
    edge_type:Option<String>,

    /// The field whose value (its first, if an array) is the line's edge type
    #[arg(long, required_unless_present = "edge_type")]
    edge_type_field:Option<String>,

    /// Put before the type read from --edge-type-field
    #[arg(long, default_value_t = String::new())]
    edge_type_prefix:String
}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

    let mut skipped:usize = 0;

    loop {

        let mut line:Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }

        let json:serde_json::Map<String,Value> = serde_json::from_slice(&line).unwrap();
        let mut out_props_json = serde_json::Map::new();

        let (from, to) = match (json.get(&args.from_field), json.get(&args.to_field)) {
            (Some(from), Some(to)) => (from, to),
            _ => { skipped += 1; continue; }
        };

        let edge_type = match (&args.edge_type, &args.edge_type_field) {
            (Some(edge_type), _) => edge_type.clone(),
            (None, Some(field)) => {
                let value = match json.get(field) {
                    Some(Value::String(s)) => Some(s.clone()),
                    Some(Value::Array(arr)) => arr.first().and_then(|v| v.as_str()).map(|s| s.to_string()),
                    _ => None
                };
                match value {
                    Some(s) if !s.is_empty() => args.edge_type_prefix.clone() + &s,
                    _ => { skipped += 1; continue; }
                }
            },
            (None, None) => unreachable!("clap requires one of --edge-type and --edge-type-field")
        };

        for (k,v) in json.iter() {

            if k.eq(&args.from_field) {
                continue;
            }
            if k.eq(&args.to_field) {
                continue;
            }
            if args.edge_type_field.as_ref().map_or(false, |f| k.eq(f)) {
                continue;
            }

            // everything apart from the from, to (and type) fields is metadata
            // which will be repeated for each (from x to) combination
            out_props_json.insert(k.clone(), v.clone());
        }

        let from_as_arr: Vec<&serde_json::Value> = if from.is_array() {
            from.as_array().unwrap().iter().collect()
        } else {
            vec![from]
        };

        let to_as_arr: Vec<&serde_json::Value> = if to.is_array() {
            to.as_array().unwrap().iter().collect()
        } else {
            vec![to]
        };

        for from_val in &from_as_arr {
            for to_val in &to_as_arr {
                let mut out_value = serde_json::Map::new();
                out_value.insert("grebi:value".to_string(), (*to_val).clone());
                out_value.insert("grebi:properties".to_string(), Value::Object(out_props_json.clone()));

                let mut out_json = serde_json::Map::new();
                out_json.insert("id".to_string(), (*from_val).clone());
                out_json.insert(edge_type.clone(), Value::Object(out_value));

                output_nodes.write_all(Value::Object(out_json).to_string().as_bytes()).unwrap();
                output_nodes.write_all("\n".as_bytes()).unwrap();
            }

        }
    }

    output_nodes.flush().unwrap();

    if skipped > 0 {
        eprintln!("grebi_nodes2edges: skipped {} line(s) without a {}, {} or edge type", skipped, args.from_field, args.to_field);
    }
}
