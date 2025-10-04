
use std::collections::HashMap;
use std::io::{BufWriter, self, BufReader, Write,BufRead};
use clap::Parser;
use serde_json::{self, de, Map};
use serde_json::Value;
use serde_json::json;
use sha1::{Sha1, Digest};

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    #[arg(long)]
    unwind_field:Option<String>

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

        let to_unwind = json.get(args.unwind_field.as_ref().unwrap()).unwrap().as_array().unwrap();

        for v in to_unwind {

            let mut out_props_json = json.clone();
            out_props_json.insert(args.unwind_field.as_ref().unwrap().to_string(), v.clone());

            let out_line = serde_json::to_vec(&out_props_json).unwrap();
            output_nodes.write_all(&out_line).unwrap();
            output_nodes.write_all(b"\n").unwrap();

        }
    }

    output_nodes.flush().unwrap();
}
