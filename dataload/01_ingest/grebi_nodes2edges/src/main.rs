
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

        let from = json.get(&args.from_field).unwrap();
        let to = json.get(&args.to_field).unwrap();

        for (k,v) in json.iter() {

            if k.eq(&args.from_field) {
                continue;
            }
            if k.eq(&args.to_field) {
                continue;
            }

            // everything apart from the from: and to: fields is metadata
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
                out_json.insert(args.edge_type.to_string(), Value::Object(out_value));

                output_nodes.write_all(Value::Object(out_json).to_string().as_bytes()).unwrap();
                output_nodes.write_all("\n".as_bytes()).unwrap();
            }

        }
    }

    output_nodes.flush().unwrap();
}

