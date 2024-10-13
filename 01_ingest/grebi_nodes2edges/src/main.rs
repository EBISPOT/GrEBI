
use std::collections::HashMap;
use std::io::{BufWriter, self, BufReader, Write,BufRead};
use clap::Parser;
use serde_json::{self, de, Map};
use serde_json::Value;
use serde_json::json;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    from_field:String,

    #[arg(long)]
    to_field:String,

    #[arg(long)]
    edge_type:String
}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
fn main() {

    let args = Args::parse();

    let stdin = io::stdin().lock();
    let mut reader = BufReader::new(stdin);

    let stdout = io::stdout().lock();
    let mut output_nodes = BufWriter::new(stdout);

    loop {

        let mut line:Vec<u8> = Vec::new();
        reader.read_until(b'\n', &mut line).unwrap();

        if line.len() == 0 {
            break;
        }

        let json:serde_json::Map<String,Value> = serde_json::from_slice(&line).unwrap();
        let mut out_props_json = serde_json::Map::new();

        let from = json.get(&args.from_field);
        let to = json.get(&args.to_field);

        for (k,v) in json.iter() {

            if k.eq(&args.from_field) {
                continue;
            }
            if k.eq(&args.to_field) {
                continue;
            }

            out_props_json.insert(k.clone(), v.clone());
        }

        let mut out_value = serde_json::Map::new();
        out_value.insert("grebi:value".to_string(), to.unwrap().clone());
        out_value.insert("grebi:properties".to_string(), Value::Object(out_props_json));

        let mut out_json = serde_json::Map::new();
        out_json.insert("id".to_string(), from.unwrap().clone());
        out_json.insert(args.edge_type.to_string(), Value::Object(out_value));

        output_nodes.write_all(Value::Object(out_json).to_string().as_bytes()).unwrap();
        output_nodes.write_all("\n".as_bytes()).unwrap();
    }

    output_nodes.flush().unwrap();
}

fn map_value(v:&Value, inject_prefix:Option<&String>, denest_subfield:Option<&String>) -> Value {
    if v.is_array() {
        return Value::Array(v.as_array().unwrap().iter().map(|v2| map_value(v2, inject_prefix, denest_subfield)).collect())
    }

    if denest_subfield.is_some() && v.is_object() {
        let subfield = denest_subfield.unwrap();
        let subfield_value = v.get(subfield);
        if subfield_value.is_some() {
            let mut props_obj:Map<String,Value> = Map::new();

            for (k,v) in v.as_object().unwrap().iter() {
                if k.eq(subfield) {
                    continue;
                }
                props_obj.insert(k.clone(), {
                    if v.is_array() {
                        v.clone()
                    } else {
                        Value::Array([v.clone()].to_vec())
                    }
                });
            }

            return json!({
                "grebi:value": subfield_value.unwrap(),
                "grebi:properties": props_obj
            });
        }
    }

    if inject_prefix.is_some() {
        return cloned_with_prefix(v, inject_prefix.unwrap())
    } else {
        return v.clone()
    }
}

fn cloned_with_prefix(val:&Value, prefix:&str) -> Value {
    if val.is_string() {
        json!(prefix.to_owned() + val.as_str().unwrap())
    } else if val.is_f64() {
        json!(prefix.to_owned() + &val.as_f64().unwrap().to_string())
    } else if val.is_i64() {
        json!(prefix.to_owned() + &val.as_i64().unwrap().to_string())
    } else {
        val.clone()
    }
}
